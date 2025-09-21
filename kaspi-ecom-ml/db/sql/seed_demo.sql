-- Seed demo data for MVP
INSERT INTO categories (id, name) VALUES (1, 'Smartphones') ON CONFLICT DO NOTHING;

INSERT INTO products (id, sku, product_id, title, category_id)
VALUES
  (1, 'SKU-IPH-13-128', '102298404', 'Apple iPhone 13 128Gb', 1),
  (2, 'SKU-IPH-15-128', '113137790', 'Apple iPhone 15 128Gb', 1),
  (3, 'SKU-RDM-13C-8-256', '114695323', 'Xiaomi Redmi 13C 8/256', 1),
  (4, 'SKU-SAM-S24-128', '115000001', 'Samsung S24 128Gb', 1),
  (5, 'SKU-RLM-11-128', '115000002', 'Realme 11 128Gb', 1)
ON CONFLICT DO NOTHING;

INSERT INTO merchants (id, name) VALUES (1, 'BestStore'), (2, 'CheapShop') ON CONFLICT DO NOTHING;

-- Minimal features and price history
INSERT INTO features_daily (sku, date, competitor_min_price, competitor_avg_price, own_price, sales_units, stock_on_hand)
VALUES
  ('SKU-IPH-13-128', CURRENT_DATE - 1, 270000, 280000, 275000, 10, 50),
  ('SKU-IPH-15-128', CURRENT_DATE - 1, 350000, 360000, 355000, 8, 40)
ON CONFLICT DO NOTHING;

INSERT INTO price_history (product_id, sku, price)
VALUES
  ('102298404', 'SKU-IPH-13-128', 275000),
  ('113137790', 'SKU-IPH-15-128', 355000);

-- Simple forecast and reco
INSERT INTO demand_forecast (sku, date, q, model_ver)
VALUES
  ('SKU-IPH-13-128', CURRENT_DATE + 1, 9, 'demo-v1'),
  ('SKU-IPH-15-128', CURRENT_DATE + 1, 7, 'demo-v1')
ON CONFLICT DO NOTHING;

INSERT INTO price_reco (sku, price, expected_qty, expected_profit, explain, model_ver)
VALUES
  ('SKU-IPH-13-128', 279000, 9, 32000, 'demo grid reco', 'demo-v1'),
  ('SKU-IPH-15-128', 359000, 7, 31000, 'demo grid reco', 'demo-v1');

-- A sample alert
INSERT INTO alerts (type, sku, payload)
VALUES ('anomaly', 'SKU-IPH-13-128', '{"msg":"sudden price drop detected"}');

