\copy (
  WITH base AS (
    SELECT
      s.nm_id,
      s.score_ver,
      s.score,
      s.decision,
      s.category,
      s.target,
      c.title,
      c.description,
      c.url,
      s.explain
    FROM wb_score_result s
    JOIN wb_cards c ON c.nm_id = s.nm_id
    WHERE s.score_ver = 1
      AND s.decision = 'accept'
      AND s.target IS NOT NULL
  ),
  ranked AS (
    SELECT
      *,
      row_number() OVER (PARTITION BY target ORDER BY random()) AS rn
    FROM base
  )
  SELECT jsonb_build_object(
    'nm_id', nm_id,
    'score_ver', score_ver,
    'score', score,
    'decision', decision,
    'category', category,
    'target', target,
    'title', title,
    'description', description,
    'url', url,
    'best_keyword', explain #>> '{best_match,keyword}',
    'best_ratio', explain #>> '{best_match,ratio}',
    'model_anchor_in_title', explain #>> '{best_match,model_anchor_in_title}',
    'anchors_in_title', explain->>'anchors_in_title'
  )::text
  FROM ranked
  WHERE rn <= 100
  ORDER BY target, rn
) TO 'accept_samples_by_target.jsonl' WITH (FORMAT text);