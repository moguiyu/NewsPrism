import logging

import httpx

from newsprism.config import Config, SourceConfig
from newsprism.service.collector import Collector


def _collector() -> tuple[Collector, SourceConfig]:
    source = SourceConfig(
        "Example",
        "Example",
        "https://example.com",
        "https://example.com/rss",
        "rss",
        1.0,
        "en",
        region="us",
    )
    cfg = Config(
        raw={},
        sources=[source],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={},
        active_search={},
    )
    return Collector(cfg), source


def test_empty_rss_window_is_not_logged_as_a_collection_failure(monkeypatch, caplog):
    collector, source = _collector()
    monkeypatch.setattr(collector, "_try_rss", lambda *_args: [])

    with caplog.at_level(logging.INFO):
        assert collector._collect_source(source, max_age_hours=3) == []

    assert "No eligible articles in collection window" in caplog.text
    assert "Collection attempts had errors" not in caplog.text


def test_rss_transport_error_remains_a_warning_with_method_context(monkeypatch, caplog):
    collector, source = _collector()
    monkeypatch.setattr(collector, "_try_rss", lambda *_args: None)

    with caplog.at_level(logging.WARNING):
        assert collector._collect_source(source, max_age_hours=3) == []

    assert "Collection attempts had errors: rss:error" in caplog.text


def test_rss_fetch_retries_with_plain_client_ua_on_403(monkeypatch):
    """Bot filters like tweakers.net 403 unknown custom UAs but allow plain
    clients; the fetch falls back once instead of failing the whole source."""
    collector, source = _collector()

    rss_xml = (
        "<?xml version='1.0'?><rss version='2.0'><channel>"
        "<item><title>Headline</title><link>https://example.com/a</link>"
        "<description>" + "x" * 400 + "</description></item>"
        "</channel></rss>"
    )
    seen_uas: list[str] = []

    class FakeResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text
            self._request = httpx.Request("GET", "https://example.com/rss")

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError(
                    "HTTP %d" % self.status_code,
                    request=self._request,
                    response=httpx.Response(self.status_code, request=self._request),
                )

    class FakeClient:
        def __init__(self, timeout=None, follow_redirects=False):
            self._responses = [(403, ""), (200, rss_xml)]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, headers=None):
            seen_uas.append((headers or {})["User-Agent"])
            status, text = self._responses.pop(0)
            return FakeResponse(status, text)

    monkeypatch.setattr(httpx, "Client", lambda **_kwargs: FakeClient())

    articles = collector._fetch_rss(source, "https://example.com/rss", max_age_hours=24)

    assert len(articles) == 1
    assert articles[0].title == "Headline"
    assert seen_uas == ["NewsPrism/1.0 (RSS reader)", "curl/8.5.0"]
