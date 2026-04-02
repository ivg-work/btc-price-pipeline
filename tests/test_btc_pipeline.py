import pytest
import requests
from unittest.mock import patch, MagicMock
from dags.btc_price_pipeline import fetch_btc_price, record_exists, FULL_TABLE

def test_fetch_btc_price_success():
    mock_response = MagicMock()
    mock_response.json.return_value = {"bitcoin": {"usd": 83241.0}}
    mock_response.raise_for_status = MagicMock()
    with patch("dags.btc_price_pipeline.requests.get", return_value=mock_response):
        price = fetch_btc_price()
        assert price == 83241.0

def test_fetch_btc_price_raises_on_http_error():
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("429")
    with patch("dags.btc_price_pipeline.requests.get", return_value=mock_response):
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_btc_price()

def test_record_exists_true():
    mock_client = MagicMock()
    mock_row = MagicMock()
    mock_row.cnt = 1
    mock_client.query.return_value.result.return_value = [mock_row]
    from datetime import datetime, timezone
    dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert record_exists(mock_client, dt) is True

def test_record_exists_false():
    mock_client = MagicMock()
    mock_row = MagicMock()
    mock_row.cnt = 0
    mock_client.query.return_value.result.return_value = [mock_row]
    from datetime import datetime, timezone
    dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert record_exists(mock_client, dt) is False

def test_full_table_name():
    assert FULL_TABLE == "crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.bitcoin_prices"
