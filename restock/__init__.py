"""restock: inventory events in, replenishment orders out. One FastAPI process.

main.py      create_app() and the `restock` command
api.py       every HTTP route except the page and the mock ERP; webhook auth
inbound.py   the rules (decide) and ingest(): one event, one transaction
outbound.py  the worker thread that sends orders to the ERP
schemas.py   the four tables, the event contract and the API's JSON shapes
runtime.py   settings, database engine and sessions, JSON logging
erp_mock.py  the mock ERP at /mock-erp, and the demo's ERP simulator
page.py      the operations page at GET / (with templates/index.html)
"""
