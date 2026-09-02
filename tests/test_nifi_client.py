import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import nifi_client


@pytest.fixture(autouse=True)
def _fast_backoff(monkeypatch: pytest.MonkeyPatch):
    # Keep the test fast: real retry timing isn't what's under test here.
    monkeypatch.setattr(nifi_client, "NIFI_RETRY_BACKOFF_SECONDS", 0.01)


async def test_push_batch_retries_on_503_then_succeeds(monkeypatch: pytest.MonkeyPatch):
    received: list[dict] = []

    async def handler(request: web.Request) -> web.Response:
        body = json.loads(await request.text())
        received.append(body)
        if len(received) < 3:
            return web.Response(status=503)
        return web.Response(status=200)

    app = web.Application()
    app.router.add_post("/ingest", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        monkeypatch.setattr(
            nifi_client, "NIFI_WEBHOOK_URL", str(server.make_url("/ingest"))
        )
        await nifi_client.push_batch("test", "live_chat", [{"n": 1}])
    finally:
        await server.close()

    assert len(received) == 3
    # Every retry must reuse the same batch_id, or downstream ON CONFLICT can't
    # dedupe a replay of a request that actually landed despite the client not
    # seeing a 2xx.
    batch_ids = {body["batch_id"] for body in received}
    assert len(batch_ids) == 1


async def test_push_batch_gives_up_after_max_retries(monkeypatch: pytest.MonkeyPatch):
    received: list[dict] = []

    async def handler(request: web.Request) -> web.Response:
        received.append(json.loads(await request.text()))
        return web.Response(status=503)

    app = web.Application()
    app.router.add_post("/ingest", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        monkeypatch.setattr(
            nifi_client, "NIFI_WEBHOOK_URL", str(server.make_url("/ingest"))
        )
        monkeypatch.setattr(nifi_client, "NIFI_MAX_RETRIES", 3)
        await nifi_client.push_batch("test", "live_chat", [{"n": 1}])
    finally:
        await server.close()

    assert len(received) == 3


async def test_push_batch_succeeds_on_first_try_without_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    received: list[dict] = []

    async def handler(request: web.Request) -> web.Response:
        received.append(json.loads(await request.text()))
        return web.Response(status=200)

    app = web.Application()
    app.router.add_post("/ingest", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        monkeypatch.setattr(
            nifi_client, "NIFI_WEBHOOK_URL", str(server.make_url("/ingest"))
        )
        await nifi_client.push_batch("test", "live_chat", [{"n": 1}])
    finally:
        await server.close()

    assert len(received) == 1
