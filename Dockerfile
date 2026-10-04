FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
RUN useradd -m appuser
USER appuser
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s \
  CMD python -c "import os,urllib.request as u; u.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8000'), timeout=2)"
# One worker on purpose: the bottleneck is the DB, and one process keeps in-process counters exact.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --backlog 8192 --timeout-keep-alive 75 --no-access-log"]
