CREATE OR REPLACE VIEW `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.btc_hourly_analytics`
AS
SELECT
  ingested_at AS timestamp,
  price_usd,
  AVG(price_usd) OVER (
    ORDER BY ingested_at
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  ) AS moving_avg_24h,
  AVG(price_usd) OVER (
    ORDER BY ingested_at
    ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7d,
  AVG(price_usd) OVER (
    ORDER BY ingested_at
    ROWS BETWEEN 719 PRECEDING AND CURRENT ROW
  ) AS moving_avg_30d,
  price_usd - LAG(price_usd) OVER (ORDER BY ingested_at) AS price_change,
  SAFE_DIVIDE(
    price_usd - LAG(price_usd) OVER (ORDER BY ingested_at),
    LAG(price_usd) OVER (ORDER BY ingested_at)
  ) * 100 AS pct_change
FROM `crypto-pipeline-486808.crypto_pipeline_486808_crypto_warehouse.bitcoin_prices`
ORDER BY ingested_at DESC
