CREATE OR REPLACE VIEW `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.clean_bitcoin_view`
AS
SELECT
  ingested_at AS timestamp,
  coin_id,
  price_usd,
  source
FROM `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.bitcoin_prices`
ORDER BY ingested_at DESC
