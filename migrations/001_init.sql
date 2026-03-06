CREATE TABLE IF NOT EXISTS basket_ranges (
  basket     SMALLINT PRIMARY KEY,
  start_nm   BIGINT NOT NULL,
  end_nm     BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scan_jobs (
  job_id      BIGSERIAL PRIMARY KEY,
  start_nm    BIGINT NOT NULL,
  end_nm      BIGINT NOT NULL,
  basket      SMALLINT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'queued',  -- queued/running/done/failed
  worker_id   TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts    INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_scan_jobs_status ON scan_jobs(status);
CREATE INDEX IF NOT EXISTS idx_scan_jobs_basket ON scan_jobs(basket);

CREATE TABLE IF NOT EXISTS wb_cards (
  nm_id        BIGINT PRIMARY KEY,
  basket       SMALLINT NOT NULL,
  supplier_id  BIGINT,
  title        TEXT,
  description  TEXT,
  product_key  TEXT,
  score        REAL,
  url          TEXT,
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wb_cards_supplier ON wb_cards(supplier_id);
CREATE INDEX IF NOT EXISTS idx_wb_cards_product  ON wb_cards(product_key);
CREATE INDEX IF NOT EXISTS idx_wb_cards_fetched  ON wb_cards(fetched_at);
