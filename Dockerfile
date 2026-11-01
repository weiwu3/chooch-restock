FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# Runtime dependencies only; the test and lint tools are pinned in requirements-dev.txt.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# Editable install: the app reads alembic/, templates/ and fixtures/ from the source tree.
RUN pip install --no-cache-dir --no-deps -e . \
    && groupadd --system --gid 10001 restock \
    && useradd --system --uid 10001 --gid restock --no-create-home restock
# I run the app as an unprivileged user because it needs no root: it writes no files (the
# database is PostgreSQL, bytecode caching is off) and listens on an unprivileged port.
USER 10001:10001

EXPOSE 8000
# I exec the server so it replaces sh as PID 1 and gets the SIGTERM of `docker stop`.
CMD ["sh", "-c", "restock migrate && exec restock run --host 0.0.0.0 --port 8000"]
