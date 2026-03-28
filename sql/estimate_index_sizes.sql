-- ==========================================================================
-- Estimate GIN index sizes for fuzzy search
-- ==========================================================================
-- Run this against the live database to get actual numbers.
-- No indexes are created — this is read-only analysis.
-- ==========================================================================

SELECT
    'bill_raw' AS tbl,
    'name' AS col,
    COUNT(*) AS row_count,
    ROUND(AVG(LENGTH(name))) AS avg_len,
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1) AS raw_mb
FROM bill_raw

UNION ALL SELECT
    'query_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM query_raw

UNION ALL SELECT
    'agenda_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM agenda_raw

UNION ALL SELECT
    'israel_law_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM israel_law_raw

UNION ALL SELECT
    'secondary_law_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM secondary_law_raw

UNION ALL SELECT
    'plenum_vote_raw', 'votetitle',
    COUNT(*), ROUND(AVG(LENGTH(votetitle))),
    ROUND(AVG(LENGTH(votetitle)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM plenum_vote_raw

UNION ALL SELECT
    'plenum_vote_raw', 'votesubject',
    COUNT(*), ROUND(AVG(LENGTH(votesubject))),
    ROUND(AVG(LENGTH(votesubject)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM plenum_vote_raw

UNION ALL SELECT
    'committee_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM committee_raw

UNION ALL SELECT
    'cmt_session_item_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM cmt_session_item_raw

UNION ALL SELECT
    'plenum_session_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM plenum_session_raw

UNION ALL SELECT
    'plm_session_item_raw', 'name',
    COUNT(*), ROUND(AVG(LENGTH(name))),
    ROUND(AVG(LENGTH(name)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM plm_session_item_raw

UNION ALL SELECT
    'person_raw', 'firstname',
    COUNT(*), ROUND(AVG(LENGTH(firstname))),
    ROUND(AVG(LENGTH(firstname)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM person_raw

UNION ALL SELECT
    'person_raw', 'lastname',
    COUNT(*), ROUND(AVG(LENGTH(lastname))),
    ROUND(AVG(LENGTH(lastname)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM person_raw

UNION ALL SELECT
    'person_to_position_raw', 'role_concat',
    COUNT(*),
    ROUND(AVG(
        LENGTH(COALESCE(dutydesc,''))
        + LENGTH(COALESCE(govministryname,''))
        + LENGTH(COALESCE(committeename,''))
    )),
    ROUND(AVG(
        LENGTH(COALESCE(dutydesc,''))
        + LENGTH(COALESCE(govministryname,''))
        + LENGTH(COALESCE(committeename,''))
    ) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM person_to_position_raw

UNION ALL SELECT
    'position_raw', 'description',
    COUNT(*), ROUND(AVG(LENGTH(description))),
    ROUND(AVG(LENGTH(description)) * COUNT(*) / 1024.0 / 1024.0, 1)
FROM position_raw

ORDER BY row_count DESC;
