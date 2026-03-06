CREATE TABLE IF NOT EXISTS wb_score_result (
  nm_id       BIGINT NOT NULL,
  score_ver   INTEGER NOT NULL,
  score       REAL NOT NULL,
  decision    TEXT NOT NULL,           -- accept | middle | reject | unknown
  category    TEXT,
  target      TEXT,
  explain     JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (nm_id, score_ver)
);

CREATE INDEX IF NOT EXISTS idx_wb_score_result_ver_decision
  ON wb_score_result (score_ver, decision);

CREATE INDEX IF NOT EXISTS idx_wb_score_result_ver_score_desc
  ON wb_score_result (score_ver, score DESC);

CREATE INDEX IF NOT EXISTS idx_wb_score_result_ver_cat_target
  ON wb_score_result (score_ver, category, target);