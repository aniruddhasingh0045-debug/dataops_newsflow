WITH source AS (
    SELECT * FROM raw.articles
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY url
            ORDER BY ingested_at DESC
        ) AS rn
    FROM source
)
SELECT
    url,
    LOWER(TRIM(title))  AS title,
    source_name,
    author,
    publishedat::date   AS published_date,
    ingested_at
FROM deduped
WHERE rn = 1
