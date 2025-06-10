SELECT
    DENSE_RANK() OVER (ORDER BY bank_name) AS bank_id,
    bank_name
FROM "google_reviews_db"."public_public"."stg_bank_reviews"
GROUP BY bank_name