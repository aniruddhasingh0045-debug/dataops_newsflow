SELECT
    source_name,
    published_date,
    COUNT(*)            AS article_count,
    COUNT(DISTINCT url) AS unique_articles
FROM {{ ref('stg_articles') }}
GROUP BY 1, 2
ORDER BY published_date DESC, article_count DESC
