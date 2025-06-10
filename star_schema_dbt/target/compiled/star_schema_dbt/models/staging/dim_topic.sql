SELECT
    DENSE_RANK() OVER (ORDER BY topic) AS topic_id,
    topic AS topic_name
FROM "google_reviews_db"."public_public"."stg_bank_reviews"
GROUP BY topic