# ./src/extract/scripts/test_paging_fitbit6.py

import pytest
import requests
from extract.clients import fitbit6_client as c


class FakeResp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body or {}, headers or {}
        self.text = str(self._body)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    c._limiters.clear()
    monkeypatch.setattr(c.time, "sleep", lambda s: None)
    monkeypatch.setattr(c.random, "uniform", lambda a, b: 0)
    monkeypatch.setattr(c, "RATE_LIMIT_WAIT_SECONDS", 0)


def script(monkeypatch, responses):
    """Feed responses in order; record the params of each call."""
    calls, it = [], iter(responses)

    def fake(method, url, **kw):
        calls.append(kw.get("params") or kw.get("json"))
        return next(it)

    monkeypatch.setattr(c.requests, "request", fake)
    return calls


def test_pages_are_merged_and_token_is_sent(monkeypatch):
    calls = script(monkeypatch, [
        FakeResp(body={"dataPoints": [1, 2], "nextPageToken": "A"}),
        FakeResp(body={"dataPoints": [3], "nextPageToken": "B"}),
        FakeResp(body={"dataPoints": [4]}),
    ])
    out = c._get_data_points("tok", "steps", "2026-01-01", "2026-01-02")
    assert out == {"dataPoints": [1, 2, 3, 4]}
    assert [p.get("pageToken") for p in calls] == [None, "A", "B"]


def test_failure_midway_raises_not_truncates(monkeypatch):
    script(monkeypatch, [
        FakeResp(body={"dataPoints": [1], "nextPageToken": "A"}),
        FakeResp(status=400),
    ])
    with pytest.raises(requests.HTTPError):
        c._get_data_points("tok", "steps", "2026-01-01", "2026-01-02")


def test_429_is_waited_out_then_succeeds(monkeypatch):
    calls = script(monkeypatch, [
        FakeResp(status=429, headers={"Retry-After": "0"}),
        FakeResp(status=429, headers={"Retry-After": "0"}),
        FakeResp(body={"dataPoints": [1]}),
    ])
    assert c._get_data_points("tok", "steps", "2026-01-01", "2026-01-01") == {"dataPoints": [1]}
    assert len(calls) == 3


def test_429_gives_up_after_max_and_raises(monkeypatch):
    script(monkeypatch, [FakeResp(status=429, headers={"Retry-After": "0"})] * (c.MAX_429_RETRIES + 1))
    with pytest.raises(requests.HTTPError):
        c._get_data_points("tok", "steps", "2026-01-01", "2026-01-01")


def test_rollup_token_goes_in_body_and_chunks(monkeypatch):
    calls = script(monkeypatch, [
        FakeResp(body={"rollupDataPoints": [1], "nextPageToken": "A"}),
        FakeResp(body={"rollupDataPoints": [2]}),
        FakeResp(body={"rollupDataPoints": [3]}),   # second 90-day chunk
    ])
    out = c._get_daily_rollup("tok", "floors", "2026-01-01", "2026-05-01")  # 121 days -> 2 chunks
    assert out == {"rollupDataPoints": [1, 2, 3]}
    assert calls[1]["pageToken"] == "A"