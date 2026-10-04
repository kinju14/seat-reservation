#!/usr/bin/env python3
"""On-sale stampede against a live URL, with hard correctness assertions.

    ./burst.sh https://your-app.onrender.com          (ADMIN_TOKEN env var required)
    python burst.py http://localhost:8000 --admin-token dev-admin-token

Phases (each on its own fresh show):
  1 hot-seat storm   many distinct users, same seat(s): exactly one 201 per seat, rest 409
  2 stampede         ~20k mixed requests: hot-biased seats, multi-seat, same-key retries, key conflicts;
                     /shows is polled DURING the burst to check the invariant live
  3 per-user limit   one user, 10 parallel reserves, limit 4
  4 idempotency      same key x20 in parallel; same key different seats
  5 identity         spoofed body user_id, foreign cancel, cancel-after-rebook (no resurrection)
Exit code 0 only if every check passes.
"""
import argparse
import asyncio
import collections
import random
import sys
import time
import uuid
from types import SimpleNamespace as NS

import aiohttp

try:
    import resource
except ImportError:  # Windows
    resource = None


def fit_concurrency(wanted):
    """Raise the open-file limit if we can, and cap --concurrency so we never run out of sockets."""
    if resource is None:
        return wanted
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = 10240 if hard == resource.RLIM_INFINITY else min(hard, 10240)
    if soft < target:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            soft = target
        except (ValueError, OSError):
            pass
    cap = max(50, soft - 300)
    if wanted > cap:
        print(f"note: open-file limit is {soft}; capping --concurrency {wanted} -> {cap}")
    return min(wanted, cap)


def label(i):  # 0->A1 ... 49->A50, 50->B1 ... 1300->AA1
    row, n = divmod(i, 50)
    row, name = row + 1, ""
    while row:
        row, r = divmod(row - 1, 26)
        name = chr(65 + r) + name
    return f"{name}{n + 1}"


class Client:
    def __init__(self, base, admin, concurrency):
        self.base, self.admin = base.rstrip("/"), admin
        self.sem = asyncio.Semaphore(concurrency)
        self.tokens = {}

    async def start(self):
        self.s = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0),
                                       timeout=aiohttp.ClientTimeout(total=180))

    async def close(self):
        await self.s.close()

    async def req(self, method, path, token=None, body=None, use_sem=True, retry=False):
        # retry=True only for operations that are safe to repeat (token mint, GETs).
        for attempt in range(3 if retry else 1):
            r = await self._req(method, path, token, body, use_sem)
            if r.status != 0:
                break
            await asyncio.sleep(0.2)
        return r

    async def _req(self, method, path, token, body, use_sem):
        h = {"Authorization": f"Bearer {token}"} if token else {}
        t0 = time.perf_counter()

        async def go():
            try:
                async with self.s.request(method, self.base + path, json=body, headers=h) as r:
                    try:
                        data = await r.json(content_type=None)
                    except Exception:
                        data = None
                    return r.status, data, r.headers
            except Exception as e:  # connection reset, timeout... counted as failures (status 0)
                return 0, {"error": type(e).__name__, "detail": str(e)[:160]}, {}

        if use_sem:
            async with self.sem:
                st, data, hd = await go()
        else:
            st, data, hd = await go()
        return NS(status=st, body=data or {}, replay=hd.get("Idempotent-Replay") == "true",
                  secs=time.perf_counter() - t0)

    async def token(self, user):
        if user not in self.tokens:
            r = await self.req("POST", "/auth/token", body={"user_id": user}, retry=True)
            if r.status != 200:
                sys.exit(f"cannot mint token: {r.status} {r.body}")
            self.tokens[user] = r.body["token"]
        return self.tokens[user]


ALL = []  # every reserve result across phases, for metrics reconciliation


async def reserve(c, show, user, seats, key=None, extra=None):
    tok = await c.token(user)
    body = {"seats": seats, "idempotency_key": key or uuid.uuid4().hex, **(extra or {})}
    r = await c.req("POST", f"/shows/{show}/reserve", token=tok, body=body)
    r.user, r.seats_req, r.key = user, seats, body["idempotency_key"]
    ALL.append(r)
    return r


async def new_show(c, name, n, limit=4):
    r = await c.req("POST", "/shows", token=c.admin,
                    body={"name": name, "seats": [label(i) for i in range(n)], "price_paise": 25000, "per_user_limit": limit})
    if r.status != 201:
        sys.exit(f"cannot create show: {r.status} {r.body}")
    return r.body["id"]


class Checks:
    def __init__(self):
        self.failed = []

    def __call__(self, ok, msg):
        print(("  PASS  " if ok else "  FAIL  ") + msg)
        if not ok:
            self.failed.append(msg)


def outcome(r):
    if r.status == 201:
        return "201 replay" if r.replay else "201 confirmed"
    err = r.body.get("error", "?")
    return f"{r.status} {err}" + (" (replayed)" if r.replay else "")


def show_dist(title, results, secs):
    d = collections.Counter(outcome(r) for r in results)
    rate = f" in {secs:.1f}s ({len(results) / secs:.0f} req/s)" if secs > 0 else ""
    print(f"\n== {title}: {len(results)} requests{rate}")
    for k, v in sorted(d.items()):
        print(f"     {k:<38}{v:>7}")
    conn_err = [r for r in results if r.status == 0]
    if conn_err:
        print(f"     !! {len(conn_err)} connection-level failures (no HTTP response). Sample: {conn_err[0].body.get('detail')}")
        print("     !! This is the client/network path, not the app: lower --concurrency (try 200) or raise `ulimit -n`.")
    return d


def bad(results):
    return [r for r in results if r.status >= 500 or r.status in (0, 429)]


async def state(c, show, seats=False):
    r = await c.req("GET", f"/shows/{show}" + ("" if seats else "?include_seats=false"), use_sem=False, retry=True)
    return r.body


async def verify_show(c, chk, show, results, name):
    s = await state(c, show)
    if "counts" not in s:
        chk(False, f"[{name}] could not read show state ({s}); skipping verification")
        return s
    cnt = s["counts"]
    chk(cnt["available"] + cnt["held"] + cnt["confirmed"] == s["total_seats"] and s["invariant_ok"],
        f"[{name}] invariant: {cnt['available']}+{cnt['held']}+{cnt['confirmed']} == {s['total_seats']}")
    ledger, dbl = {}, 0
    for r in results:
        if r.status == 201:
            for seat in r.body["seats"]:
                if ledger.setdefault(seat, r.body["reservation_id"]) != r.body["reservation_id"]:
                    dbl += 1
    chk(dbl == 0, f"[{name}] no seat confirmed to two reservations")
    chk(len(ledger) == cnt["confirmed"], f"[{name}] server confirmed={cnt['confirmed']} == client-observed winners={len(ledger)}")
    chk(not bad(results), f"[{name}] zero 5xx / 429 / connection errors (got {len(bad(results))})")
    return s


# ------------------------------------------------------------------ phases
async def phase_hot(c, chk, run, a):
    show = await new_show(c, "hot-seat-storm", 200)
    hot = ["A12", "A13", "A14", "A15", "A16"]
    # distinct contenders per seat, so the per-user limit can't mask a seat-taken result
    users = {s: [f"{run}-{s}-{i}" for i in range(a.hot_users)] for s in hot}
    await asyncio.gather(*(c.token(u) for us in users.values() for u in us))
    t = time.perf_counter()
    res = await asyncio.gather(*(reserve(c, show, u, [s]) for s in hot for u in users[s]))
    show_dist(f"1 hot-seat storm ({len(hot)} seats x {a.hot_users} users each)", res, time.perf_counter() - t)
    for s in hot:
        mine = [r for r in res if r.seats_req == [s]]
        wins = sum(r.status == 201 for r in mine)
        losers = sum(r.status == 409 and r.body.get("error") == "seat_taken" for r in mine)
        chk(wins == 1 and losers == len(mine) - 1, f"[hot] seat {s}: exactly 1x201, {losers}x409 seat_taken")
    await verify_show(c, chk, show, res, "hot")


def build_stampede(run, a):
    rnd = random.Random(7)
    hot = [label(i) for i in range(a.hot)]
    pick = lambda: rnd.choice(hot) if rnd.random() < 0.7 else label(rnd.randrange(a.seats))

    def fresh():
        n = 1 if rnd.random() < 0.8 else 2
        seats = set()
        while len(seats) < n:
            seats.add(pick())
        return (f"{run}-u{rnd.randrange(a.users)}", sorted(seats), uuid.uuid4().hex)

    jobs = []
    while len(jobs) < a.total:
        roll = rnd.random()
        if jobs and roll < 0.10:                       # retry: identical user+key+body
            jobs.append(rnd.choice(jobs))
        elif jobs and roll < 0.13:                     # same key, different seats
            u, _, k = rnd.choice(jobs)
            jobs.append((u, [label(rnd.randrange(a.seats))], k))
        else:
            jobs.append(fresh())
    rnd.shuffle(jobs)
    return jobs


async def phase_stampede(c, chk, run, a):
    show = await new_show(c, "on-sale-stampede", a.seats)
    jobs = build_stampede(run, a)
    await asyncio.gather(*(c.token(u) for u in {j[0] for j in jobs}))
    stop, polls = asyncio.Event(), collections.Counter()

    async def poller():
        while not stop.is_set():
            s = await state(c, show)
            polls["n"] += 1
            if not s or not s.get("invariant_ok"):
                polls["violations"] += 1
            await asyncio.sleep(0.25)

    p = asyncio.create_task(poller())
    t = time.perf_counter()
    res = await asyncio.gather(*(reserve(c, show, u, s, k) for u, s, k in jobs))
    dur = time.perf_counter() - t
    stop.set()
    await p
    show_dist(f"2 stampede ({a.users} users, {a.hot} hot seats, 10% retries, 3% key conflicts)", res, dur)
    lat = sorted(r.secs for r in res)
    print(f"     latency p50={lat[len(lat) // 2]:.2f}s p99={lat[int(len(lat) * .99)]:.2f}s max={lat[-1]:.2f}s")
    await verify_show(c, chk, show, res, "stampede")
    chk(polls["n"] > 0 and polls["violations"] == 0, f"[stampede] invariant held on all {polls['n']} polls DURING the burst")
    return show


async def phase_limit(c, chk, run):
    show = await new_show(c, "per-user-limit", 50, limit=4)
    u = f"{run}-greedy"
    res = await asyncio.gather(*(reserve(c, show, u, [label(i)]) for i in range(10)))
    show_dist("3 per-user limit (1 user x 10 parallel, limit 4)", res, 0)
    s = await verify_show(c, chk, show, res, "limit")
    chk(sum(r.status == 201 for r in res) == 4 and s["counts"]["confirmed"] == 4, "[limit] exactly 4 confirmed, 6 declined per_user_limit")
    chk(sum(r.body.get("error") == "per_user_limit" for r in res) == 6, "[limit] the other 6 are clean per_user_limit 409s")


async def phase_idem(c, chk, run):
    show = await new_show(c, "idempotency", 20)
    u, key = f"{run}-idem", uuid.uuid4().hex
    res = await asyncio.gather(*(reserve(c, show, u, ["A1"], key) for _ in range(20)))
    show_dist("4 idempotency (same key x20 parallel)", res, 0)
    ids = {r.body.get("reservation_id") for r in res if r.status == 201}
    chk(len(ids) == 1 and all(r.status == 201 for r in res), "[idem] 20 parallel same-key requests -> one reservation id, all 201")
    chk(sum(not r.replay for r in res) == 1, "[idem] exactly one request did the work; 19 were replays")
    r = await reserve(c, show, u, ["A2"], key)
    chk(r.status == 409 and r.body.get("error") == "idempotency_key_conflict", "[idem] same key, different seats -> 409 idempotency_key_conflict")
    s = await state(c, show)
    chk(s["counts"]["confirmed"] == 1, "[idem] retries and the conflicting request moved nothing extra (confirmed == 1)")


async def phase_identity(c, chk, run):
    show = await new_show(c, "identity", 10)
    bob, alice, carol = f"{run}-bob", f"{run}-alice", f"{run}-carol"
    r = await reserve(c, show, bob, ["A1"], extra={"user_id": alice, "userId": alice, "user": alice})
    chk(r.status == 201 and r.body["user_id"] == bob, "[identity] spoofed body user_id ignored; reservation belongs to token user")
    rid = r.body["reservation_id"]
    x = await c.req("POST", f"/reservations/{rid}/cancel", token=await c.token(alice))
    chk(x.status == 403, "[identity] non-owner cancel -> 403")
    x = await c.req("POST", f"/reservations/{rid}/cancel")
    chk(x.status == 401, "[identity] cancel without token -> 401")
    x = await c.req("POST", f"/shows/{show}/reserve", token="garbage", body={"seats": ["A2"], "idempotency_key": "k"})
    chk(x.status == 401, "[identity] garbage token -> 401")
    x = await c.req("POST", f"/reservations/{rid}/cancel", token=await c.token(bob))
    chk(x.status == 200 and x.body["status"] == "cancelled", "[identity] owner cancel -> 200 cancelled")
    r2 = await reserve(c, show, carol, ["A1"])
    chk(r2.status == 201, "[identity] released seat is cleanly re-bookable by someone else")
    x = await c.req("POST", f"/reservations/{rid}/cancel", token=await c.token(bob))
    s = await state(c, show, seats=True)
    a1 = next(e for e in s["seats"] if e["seat"] == "A1")["status"]
    chk(x.status == 200 and a1 == "confirmed", "[identity] re-cancelling the OLD reservation does not resurrect/free carol's seat")


# ------------------------------------------------------------------ metrics
def parse_metrics(text):
    out = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            k, _, v = line.rpartition(" ")
            try:
                out[k] = float(v)
            except ValueError:
                pass
    return out


async def scrape(c):
    r = await c.s.get(c.base + "/metrics")
    return parse_metrics(await r.text())


async def reconcile_metrics(c, chk, m0, shows):
    m1 = await scrape(c)
    d = lambda k: m1.get(k, 0) - m0.get(k, 0)
    print("\n== metrics reconciliation (valid only if nothing else hit the service during the run)")
    non_replay = [r for r in ALL if not r.replay]
    chk(d("reservations_confirmed_total") == sum(r.status == 201 for r in non_replay),
        f"reservations_confirmed_total +{d('reservations_confirmed_total'):.0f} == client-observed first-time 201s")
    codes = {"seat_taken": "seat_taken", "per_user_limit": "per_user_limit", "unknown_seat": "unknown_seat",
             "key_conflict": "idempotency_key_conflict"}
    for reason, code in codes.items():
        want = sum(r.status in (404, 409) and r.body.get("error") == code for r in non_replay)
        got = d(f'reservations_declined_total{{reason="{reason}"}}')
        chk(got == want, f"declined{{reason={reason}}} +{got:.0f} == client-observed {want}")
    want = sum(r.replay for r in ALL)
    got = d('reservations_declined_total{reason="idempotent_replay"}')
    chk(got == want, f"declined{{reason=idempotent_replay}} +{got:.0f} == client-observed replays {want}")
    for name, sid in shows.items():
        s = await state(c, sid)
        g = m1.get(f'seats_available{{show_id="{sid}"}}')
        chk(g == s["counts"]["available"], f"seats_available gauge ({g}) == GET /shows available ({s['counts']['available']})  [{name}]")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base_url")
    ap.add_argument("--admin-token", required=True)
    ap.add_argument("--total", type=int, default=20000, help="requests in the stampede phase")
    ap.add_argument("--users", type=int, default=5000)
    ap.add_argument("--seats", type=int, default=2000)
    ap.add_argument("--hot", type=int, default=20, help="number of hot seats in the stampede")
    ap.add_argument("--hot-users", type=int, default=500, help="users per seat in the hot-seat storm")
    ap.add_argument("--concurrency", type=int, default=2000, help="client-side max in-flight requests")
    a = ap.parse_args()

    c, chk, run = Client(a.base_url, a.admin_token, fit_concurrency(a.concurrency)), Checks(), uuid.uuid4().hex[:6]
    await c.start()
    r = await c.req("GET", "/readyz")
    chk(r.status == 200, f"/readyz -> {r.status}")
    m0 = await scrape(c)
    shows = {}
    t0 = time.perf_counter()
    try:
        await phase_hot(c, chk, run, a)
        shows["stampede"] = await phase_stampede(c, chk, run, a)
        await phase_limit(c, chk, run)
        await phase_idem(c, chk, run)
        await phase_identity(c, chk, run)
        await reconcile_metrics(c, chk, m0, shows)
        x = await c.req("GET", "/shows/00000000-0000-0000-0000-000000000000")
        chk(x.status == 404, "unknown show -> 404")
    finally:
        await c.close()
    chk(not bad(ALL), f"GLOBAL: zero 5xx/429/conn-errors across {len(ALL)} reserve calls")
    print(f"\nTotal {time.perf_counter() - t0:.0f}s. " + ("ALL CHECKS PASSED" if not chk.failed else f"{len(chk.failed)} CHECK(S) FAILED"))
    sys.exit(1 if chk.failed else 0)


if __name__ == "__main__":
    asyncio.run(main())