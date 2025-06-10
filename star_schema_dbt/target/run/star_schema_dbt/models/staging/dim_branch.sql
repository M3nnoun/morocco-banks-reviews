
  create view "google_reviews_db"."public_public"."dim_branch__dbt_tmp"
    
    
  as (
    WITH branches AS (
    SELECT
        branch_name,
        bank_name
    FROM "google_reviews_db"."public_public"."stg_bank_reviews"
    GROUP BY branch_name, bank_name
)

SELECT
    DENSE_RANK() OVER (ORDER BY branch_name) AS branch_id,
    branch_name,
    b.bank_id
FROM branches
LEFT JOIN "google_reviews_db"."public_public"."dim_bank" b ON branches.bank_name = b.bank_name
  );