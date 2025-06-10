

SELECT
    id,
    city,
    business_name,
    address,
    review_text,
    stars,
    timestamp,
    scraped_at
FROM "google_reviews_db"."public"."reviews"
-- WHERE scraped_at >= CURRENT_DATE - INTERVAL '2 months'