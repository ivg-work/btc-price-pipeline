CREATE OR REPLACE VIEW `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.btc_latest`
AS
SELECT
  ingested_at AS timestamp,
  price_usd,
  source
FROM `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.bitcoin_prices`
ORDER BY ingested_at DESC
LIMIT 1
