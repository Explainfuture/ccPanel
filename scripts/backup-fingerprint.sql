-- Operator-only reconciliation: fingerprints contain no row payloads or credentials.
-- Include logical parents and partitions; restore must preserve both structure and rows.
SELECT format(
  'SELECT json_build_array(%L,count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM %I.%I t;',
  schemaname||'.'||tablename,schemaname,tablename
) FROM pg_tables WHERE schemaname='public' ORDER BY tablename
\gexec
