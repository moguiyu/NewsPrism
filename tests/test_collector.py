import logging

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
