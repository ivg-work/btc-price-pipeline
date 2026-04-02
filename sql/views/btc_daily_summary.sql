CREATE OR REPLACE VIEW `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.btc_daily_summary`
AS
SELECT
  DATE(ingested_at) AS date,
  MIN(price_usd)    AS price_low,
  MAX(price_usd)    AS price_high,
  AVG(price_usd)    AS price_avg,
  (ARRAY_AGG(price_usd ORDER BY ingested_at ASC  LIMIT 1))[OFFSET(0)] AS price_open,
  (ARRAY_AGG(price_usd ORDER BY ingested_at DESC LIMIT 1))[OFFSET(0)] AS price_close,
  STDDEV(price_usd) AS volatility,
  COUNT(*)          AS record_count
FROM `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.bitcoin_prices`
GROUP BY DATE(ingested_at)
ORDER BY date DESC
