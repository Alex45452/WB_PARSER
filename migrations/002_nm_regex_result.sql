CREATE TABLE IF NOT EXISTS nm_regex_result (
  nm_id bigint PRIMARY KEY,
  ver int NOT NULL,
  decision text NOT NULL,             -- accept | reject | unknown
  category text,                      -- iphone/macbook/...
  target text,                        -- iphone_17_pro, s25_ultra, ...
  reject_reason text,                 -- код причины
  title_used boolean NOT NULL DEFAULT true,
  matched_in text,                    -- title | description
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nm_regex_result_ver_decision
  ON nm_regex_result (ver, decision);

CREATE INDEX IF NOT EXISTS idx_nm_regex_result_ver_category
  ON nm_regex_result (ver, category);