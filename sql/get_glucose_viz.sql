-- bg_ts: timestamp of the blood glucose measurement
-- value: measured blood glucose value
WITH first_filename AS (
    SELECT filename
    FROM glucose_data
    WHERE filename IS NOT NULL
    ORDER BY filename
    LIMIT 1
)
SELECT
    g.bg_ts,    -- timestamp of the blood glucose measurement
    g.value     -- measured blood glucose value
FROM
    glucose_data g
JOIN
    first_filename f
    ON g.filename = f.filename
ORDER BY
    g.bg_ts;