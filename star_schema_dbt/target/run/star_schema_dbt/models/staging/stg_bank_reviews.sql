
  create view "google_reviews_db"."public_public"."stg_bank_reviews__dbt_tmp"
    
    
  as (
    WITH raw_reviews AS (
    SELECT
        *,
        -- Extract city and postal_code from location
        
        TRIM(SPLIT_PART(location, ' ', -1)) AS postal_code
    FROM "google_reviews_db"."public_public"."enriched_reviews"
)

SELECT
    ROW_NUMBER() OVER () AS review_id,  -- Surrogate key
    bank_name,
    branch_name,
    location,
    city,
    postal_code,
    review_text_cleaned,
    rating::INT,
    review_date::DATE,
    processed_text,
    TRIM(topic) AS topic,              -- Clean topic name
    topic_confidence::FLOAT,
    LOWER(sentiment) AS sentiment      -- Standardize sentiment
FROM raw_reviews
  );