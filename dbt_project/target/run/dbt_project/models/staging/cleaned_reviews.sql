
  
    

  create  table "google_reviews_db"."public_public"."cleaned_reviews__dbt_tmp"
  
  
    as
  
  (
    WITH cleaned_data AS ( 
    SELECT
        NULLIF(TRIM(business_name), '') AS bank_name,
        CASE 
            WHEN NULLIF(TRIM(business_name), '') IS NOT NULL AND NULLIF(TRIM(address), '') IS NOT NULL 
            THEN CONCAT(TRIM(business_name), ' - ', TRIM(address))
            ELSE NULL
        END AS branch_name,
        NULLIF(TRIM(address), '') AS location,
        NULLIF(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER(TRIM(review_text)), 
                    '[^\w\s]', 
                    ' ', 
                    'g'
                ),
                '\s+', 
                ' ', 
                'g'
            ),
            ''
        ) AS review_text,
        CASE
            WHEN timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 
                CASE 
                    WHEN timestamp::DATE <= CURRENT_DATE THEN timestamp::DATE
                    ELSE NULL
                END
            WHEN timestamp ILIKE '%jour%' THEN
                CASE
                    WHEN REGEXP_REPLACE(timestamp, '\D', '', 'g') = '' THEN CURRENT_DATE - 1
                    WHEN CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT) BETWEEN 1 AND 1000 THEN 
                        (CURRENT_DATE - CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT))::DATE
                    ELSE NULL
                END
            WHEN timestamp ILIKE '%semaine%' THEN
                CASE
                    WHEN REGEXP_REPLACE(timestamp, '\D', '', 'g') = '' THEN CURRENT_DATE - 7
                    WHEN CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT) BETWEEN 1 AND 500 THEN 
                        (CURRENT_DATE - CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT) * 7)::DATE
                    ELSE NULL
                END
            WHEN timestamp ILIKE '%mois%' THEN
                CASE
                    WHEN REGEXP_REPLACE(timestamp, '\D', '', 'g') = '' THEN CURRENT_DATE - INTERVAL '1 month'
                    WHEN CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT) BETWEEN 1 AND 120 THEN 
                        (CURRENT_DATE - INTERVAL '1 month' * CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT))::DATE
                    ELSE NULL
                END
            WHEN timestamp ILIKE '%an%' THEN
                CASE
                    WHEN REGEXP_REPLACE(timestamp, '\D', '', 'g') = '' THEN CURRENT_DATE - INTERVAL '1 year'
                    WHEN CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT) BETWEEN 1 AND 50 THEN 
                        (CURRENT_DATE - INTERVAL '1 year' * CAST(REGEXP_REPLACE(timestamp, '\D', '', 'g') AS INT))::DATE
                    ELSE NULL
                END
            ELSE NULL
        END AS review_date,
        CASE
            WHEN stars IS NULL THEN NULL
            WHEN CAST(stars AS DECIMAL) BETWEEN 0 AND 5 THEN CAST(stars AS DECIMAL)
            ELSE NULL
        END AS rating,
        id AS review_id,
        -- Nettoyage et normalisation des noms de villes
        city AS clean_city
    FROM 
        "google_reviews_db"."public_public"."stg_reviews"
    WHERE 
        review_text IS NOT NULL
        AND LENGTH(TRIM(review_text)) > 5
        AND TRIM(review_text) != 'No review text found'
        AND TRIM(review_text) NOT ILIKE 'no%review%'
        AND TRIM(review_text) NOT ILIKE '%review text%'
),

deduplicated_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(bank_name, ''),
                COALESCE(branch_name, ''),
                COALESCE(review_text, ''),
                COALESCE(rating, 0),
                COALESCE(clean_city, '')
            ORDER BY 
                review_date DESC NULLS LAST,
                review_id
        ) AS row_num
    FROM 
        cleaned_data
    WHERE 
        clean_city IS NOT NULL  -- Exclure les lignes sans ville valide
)

SELECT
    bank_name,
    branch_name,
    location,
    clean_city AS city,
    
    (SELECT string_agg(word, ' ') 
     FROM unnest(string_to_array(review_text, ' ')) AS word
     WHERE word NOT IN (
         'je', 'tu', 'il', 'elle', 'nous', 'vous', 'ils', 'elles', 
         'me', 'te', 'se', 'le', 'la', 'les', 'lui', 'leur', 'y', 'en', 
         'mon', 'ton', 'son', 'notre', 'votre', 'leur', 'ma', 'ta', 'sa', 
         'mes', 'tes', 'ses', 'nos', 'vos', 'leurs', 
         'ce', 'cet', 'cette', 'ces', 'ceci', 'cela', 'celui', 'celle', 
         'ceux', 'celles', 'qui', 'que', 'quoi', 'dont', 'où', 
         'et', 'ou', 'mais', 'donc', 'or', 'ni', 'car', 
         'à', 'de', 'en', 'pour', 'par', 'avec', 'sans', 'sous', 'sur', 'dans', 'entre', 
         'au', 'aux', 'du', 'des', 'un', 'une', 'le', 'la', 'les', 'l', 
         'ne', 'pas', 'plus', 'moins', 'aucun', 'rien', 'tout', 'tous', 'toutes', 'chaque', 
         'quelques', 'certains', 'certaines', 'autre', 'autres', 
         'même', 'mêmes', 'ainsi', 'alors', 'donc', 'puis', 'ensuite', 
         'avant', 'après', 'depuis', 'pendant', 'toujours', 'jamais', 'souvent', 
         'ici', 'là', 'ailleurs', 'partout', 'nulle part', 
         'comment', 'pourquoi', 'parce que', 'combien', 'quel', 'quelle', 'quels', 'quelles', 
         'si', 'quand', 'tant', 'trop', 'très', 'peu', 'assez', 'bien', 'mal', 'mieux', 
         'cependant', 'pourtant', 'tandis', 'lorsque', 'quand', 'comme', 'ainsi que'
     ))
 AS review_text_cleaned,
    rating,
    review_date
FROM 
    deduplicated_data
WHERE 
    row_num = 1
    AND bank_name IS NOT NULL
    AND review_text IS NOT NULL
    AND clean_city IS NOT NULL
    AND LENGTH(
    (SELECT string_agg(word, ' ') 
     FROM unnest(string_to_array(review_text, ' ')) AS word
     WHERE word NOT IN (
         'je', 'tu', 'il', 'elle', 'nous', 'vous', 'ils', 'elles', 
         'me', 'te', 'se', 'le', 'la', 'les', 'lui', 'leur', 'y', 'en', 
         'mon', 'ton', 'son', 'notre', 'votre', 'leur', 'ma', 'ta', 'sa', 
         'mes', 'tes', 'ses', 'nos', 'vos', 'leurs', 
         'ce', 'cet', 'cette', 'ces', 'ceci', 'cela', 'celui', 'celle', 
         'ceux', 'celles', 'qui', 'que', 'quoi', 'dont', 'où', 
         'et', 'ou', 'mais', 'donc', 'or', 'ni', 'car', 
         'à', 'de', 'en', 'pour', 'par', 'avec', 'sans', 'sous', 'sur', 'dans', 'entre', 
         'au', 'aux', 'du', 'des', 'un', 'une', 'le', 'la', 'les', 'l', 
         'ne', 'pas', 'plus', 'moins', 'aucun', 'rien', 'tout', 'tous', 'toutes', 'chaque', 
         'quelques', 'certains', 'certaines', 'autre', 'autres', 
         'même', 'mêmes', 'ainsi', 'alors', 'donc', 'puis', 'ensuite', 
         'avant', 'après', 'depuis', 'pendant', 'toujours', 'jamais', 'souvent', 
         'ici', 'là', 'ailleurs', 'partout', 'nulle part', 
         'comment', 'pourquoi', 'parce que', 'combien', 'quel', 'quelle', 'quels', 'quelles', 
         'si', 'quand', 'tant', 'trop', 'très', 'peu', 'assez', 'bien', 'mal', 'mieux', 
         'cependant', 'pourtant', 'tandis', 'lorsque', 'quand', 'comme', 'ainsi que'
     ))
) > 3
ORDER BY
    clean_city,
    bank_name,
    branch_name,
    review_date DESC NULLS LAST
  );
  