SELECT
    DENSE_RANK() OVER (ORDER BY location) AS location_id,
    location,
    city,
    postal_code
FROM "google_reviews_db"."public_public"."stg_bank_reviews"
GROUP BY location, city, postal_code