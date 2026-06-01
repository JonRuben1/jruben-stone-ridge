from unittest.mock import MagicMock, patch

import coinbase


def _mock_resp(body):
    m = MagicMock()
    m.json.return_value = body
    m.raise_for_status.return_value = None
    return m


def test_fetch_prices_projects_to_time_and_price(sample_coinbase_response):
    with patch("coinbase.requests.get", return_value=_mock_resp(sample_coinbase_response)):
        out = list(coinbase.fetch_prices("BTC-USD"))

    assert out == [
        {"time": "2026-05-19T20:13:42.123456Z", "price": "67421.42"},
        {"time": "2026-05-19T20:13:41.987654Z", "price": "67420.10"},
    ]


def test_fetch_prices_empty_response_yields_nothing():
    with patch("coinbase.requests.get", return_value=_mock_resp([])):
        out = list(coinbase.fetch_prices("BTC-USD"))
    assert out == []


def test_fetch_prices_paginates_with_after_cursor(sample_coinbase_response):
    page2 = [
        {
            "time": "2026-05-19T20:13:40.000000Z",
            "trade_id": 123456787,
            "price": "67419.00",
            "size": "0.1",
            "side": "buy",
        }
    ]
    responses = [_mock_resp(sample_coinbase_response), _mock_resp(page2)]
    with patch("coinbase.requests.get", side_effect=responses) as mock_get:
        out = list(coinbase.fetch_prices("BTC-USD", pages=2))

    assert len(out) == 3
    assert mock_get.call_count == 2
    second_call_params = mock_get.call_args_list[1].kwargs["params"]
    assert second_call_params == {"after": 123456788}


def test_fetch_prices_stops_when_response_is_empty(sample_coinbase_response):
    responses = [_mock_resp(sample_coinbase_response), _mock_resp([])]
    with patch("coinbase.requests.get", side_effect=responses) as mock_get:
        out = list(coinbase.fetch_prices("BTC-USD", pages=5))

    assert len(out) == 2
    assert mock_get.call_count == 2
