
  create view "google_reviews_db"."public_public"."dim_sentiment__dbt_tmp"
    
    
  as (
    SELECT
    DENSE_RANK() OVER (ORDER BY sentiment) AS sentiment_id,
    sentiment AS sentiment_label
FROM "google_reviews_db"."public_public"."stg_bank_reviews"
GROUP BY sentiment
  );