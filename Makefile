URL ?= http://localhost:8000
ADMIN_TOKEN ?= dev-admin-token
up:      ## run API + Postgres locally
	docker compose up --build -d
down:
	docker compose down -v
burst:   ## make burst URL=https://your-app.onrender.com ADMIN_TOKEN=...
	ADMIN_TOKEN=$(ADMIN_TOKEN) ./burst.sh $(URL)
logs:
	docker compose logs -f api
