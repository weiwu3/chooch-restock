-- Runs once when the compose Postgres volume is created: a separate database for
-- `make test-pg`, so the test suite never touches the app's data.
CREATE DATABASE restock_test OWNER restock;
