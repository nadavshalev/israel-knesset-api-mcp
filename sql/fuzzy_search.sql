-- ==========================================================================
-- Fuzzy Hebrew text search: normalize + FTS + strict_word_similarity
-- ==========================================================================
-- Run via ensure_fuzzy_infra() in core/db.py (called from ensure_indexes).
-- ==========================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ---------------------------------------------------------------------------
-- Text normalization for Hebrew
-- ---------------------------------------------------------------------------
-- Replaces maqaf, dashes, colons with space; strips geresh/gershayim/quotes.
-- IMMUTABLE + STRICT so it can be used in GIN expression indexes.

-- normalize_hebrew: base normalization (punctuation/dashes only).
-- Used for query text and trigram matching.
CREATE OR REPLACE FUNCTION normalize_hebrew(text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT trim(regexp_replace(
        translate(
            replace(replace(replace(replace($1, E'\u05BE', ' '), E'\u2013', ' '), E'\u2014', ' '), '-', ' '),
            E'\u05F3\u05F4\u0022\u0027:', ''
        ),
        '\s+', ' ', 'g'
    ))
$$;

-- normalize_hebrew_fts: normalization for FTS indexing.
-- Inlines base normalization and additionally strips common Hebrew prefixes
-- (ה/ב/ל/מ/ו) so that "הכנסה" and "כנסה" index to the same token.
-- Note: "כ" is intentionally excluded — it is often a root letter (כספים, כנסת).
-- Self-contained (does not call normalize_hebrew) to avoid inlining issues.
-- IMMUTABLE + STRICT so it can be used in GIN expression indexes.
CREATE OR REPLACE FUNCTION normalize_hebrew_fts(text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT trim(regexp_replace(
        regexp_replace(
            translate(
                replace(replace(replace(replace($1, E'\u05BE', ' '), E'\u2013', ' '), E'\u2014', ' '), '-', ' '),
                E'\u05F3\u05F4\u0022\u0027:', ''
            ),
            E'\\m[\u05D4\u05D1\u05DC\u05DE\u05D5](?=[\u05D0-\u05EA]{2,})', '', 'g'
        ),
        '\s+', ' ', 'g'
    ))
$$;

-- ---------------------------------------------------------------------------
-- Build tsquery that requires ALL words (AND logic)
-- ---------------------------------------------------------------------------
-- plainto_tsquery uses OR; we need AND so "חוק הלאום" doesn't match everything
-- containing just "חוק". Splits on spaces, joins with ' & '.

-- make_and_tsquery: build a tsquery requiring ALL words (AND logic).
-- Uses normalize_hebrew_fts (same normalization as the FTS index) so query
-- tokens match indexed tokens (e.g. "הלאום" query → "לאום" token, which
-- matches the "לאום" token stored for "מדינת הלאום").
CREATE OR REPLACE FUNCTION make_and_tsquery(query text)
RETURNS tsquery LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT to_tsquery('simple',
        array_to_string(
            array(SELECT w FROM unnest(string_to_array(normalize_hebrew_fts(query), ' ')) AS w WHERE w != ''),
            ' & '
        )
    )
$$;

