
  create view "google_reviews_db"."public_public"."fact_reviews__dbt_tmp"
    
    
  as (
    WITH reviews AS (
    SELECT 
        review_id,
        bank_name,
        branch_name,
        location,
        topic,
        sentiment,
        rating,
        topic_confidence,
        review_date,
        processed_text
    FROM "google_reviews_db"."public_public"."stg_bank_reviews"
),

-- Calculs de métriques dérivées
enhanced_reviews AS (
    SELECT 
        *,
        -- Métriques de satisfaction
        CASE 
            WHEN rating >= 4 THEN 'Satisfait'
            WHEN rating = 3 THEN 'Neutre'
            ELSE 'Insatisfait'
        END AS satisfaction_level,
        
        -- Score de satisfaction (0-1)
        ROUND(CAST((rating - 1.0) / 4.0 AS NUMERIC), 2) AS satisfaction_score,
        
        -- Indicateurs binaires pour agrégations
        CASE WHEN rating >= 4 THEN 1 ELSE 0 END AS is_positive_rating,
        CASE WHEN rating <= 2 THEN 1 ELSE 0 END AS is_negative_rating,
        CASE WHEN sentiment = 'positif' THEN 1 ELSE 0 END AS is_positive_sentiment,
        CASE WHEN sentiment = 'négatif' THEN 1 ELSE 0 END AS is_negative_sentiment,
        
        -- Segmentation temporelle
        EXTRACT(YEAR FROM review_date) AS review_year,
        EXTRACT(MONTH FROM review_date) AS review_month,
        EXTRACT(QUARTER FROM review_date) AS review_quarter,
        TO_CHAR(review_date, 'YYYY-MM') AS year_month,
        TO_CHAR(review_date, 'YYYY-Q') AS year_quarter,
        
        -- Jour de la semaine (1=Lundi, 7=Dimanche)
        EXTRACT(ISODOW FROM review_date) AS day_of_week,
        CASE 
            WHEN EXTRACT(ISODOW FROM review_date) IN (6,7) THEN 'Weekend'
            ELSE 'Semaine'
        END AS week_period,
        
        -- Ancienneté de l'avis (en jours)
        CURRENT_DATE - review_date AS review_age_days,
        
        -- Catégorisation de l'ancienneté
        CASE 
            WHEN CURRENT_DATE - review_date <= 30 THEN 'Récent (≤30j)'
            WHEN CURRENT_DATE - review_date <= 90 THEN 'Moyen (31-90j)'
            WHEN CURRENT_DATE - review_date <= 180 THEN 'Ancien (91-180j)'
            ELSE 'Très ancien (>180j)'
        END AS review_freshness,
        
        -- Score de confiance catégorisé
        CASE 
            WHEN topic_confidence >= 0.7 THEN 'Élevée'
            WHEN topic_confidence >= 0.4 THEN 'Moyenne'
            ELSE 'Faible'
        END AS confidence_level,
        
        -- Longueur du texte traité (indicateur de détail)
        LENGTH(processed_text) AS text_length,
        CASE 
            WHEN LENGTH(processed_text) >= 200 THEN 'Détaillé'
            WHEN LENGTH(processed_text) >= 100 THEN 'Moyen'
            ELSE 'Court'
        END AS review_detail_level,
        
        -- Alignment sentiment/rating (cohérence)
        CASE 
            WHEN (sentiment = 'positif' AND rating >= 4) OR 
                 (sentiment = 'négatif' AND rating <= 2) OR
                 (sentiment = 'neutre' AND rating = 3) THEN 'Cohérent'
            ELSE 'Incohérent'
        END AS sentiment_rating_alignment
    FROM reviews
),

-- Ajout de métriques de contexte (moyennes par banque/agence)
contextual_metrics AS (
    SELECT 
        er.*,
        
        -- Moyennes par banque
        ROUND(CAST(AVG(rating) OVER (PARTITION BY bank_name) AS NUMERIC), 2) AS bank_avg_rating,
        ROUND(CAST(AVG(topic_confidence) OVER (PARTITION BY bank_name) AS NUMERIC), 2) AS bank_avg_confidence,
        
        -- Moyennes par agence
        ROUND(CAST(AVG(rating) OVER (PARTITION BY bank_name, branch_name) AS NUMERIC), 2) AS branch_avg_rating,
        ROUND(CAST(AVG(topic_confidence) OVER (PARTITION BY bank_name, branch_name) AS NUMERIC), 2) AS branch_avg_confidence,
        
        -- Moyennes par localisation
        ROUND(CAST(AVG(rating) OVER (PARTITION BY location) AS NUMERIC), 2) AS location_avg_rating,
        
        -- Moyennes par topic
        ROUND(CAST(AVG(rating) OVER (PARTITION BY topic) AS NUMERIC), 2) AS topic_avg_rating,
        
        -- Compteurs pour contexte
        COUNT(*) OVER (PARTITION BY bank_name) AS total_reviews_bank,
        COUNT(*) OVER (PARTITION BY bank_name, branch_name) AS total_reviews_branch,
        COUNT(*) OVER (PARTITION BY location) AS total_reviews_location,
        COUNT(*) OVER (PARTITION BY topic) AS total_reviews_topic
    FROM enhanced_reviews er
),

-- Performance relative
performance_indicators AS (
    SELECT 
        cm.*,
        
        -- Performance relative à la moyenne de la banque
        ROUND(CAST(rating - bank_avg_rating AS NUMERIC), 2) AS rating_vs_bank_avg,
        ROUND(CAST(topic_confidence - bank_avg_confidence AS NUMERIC), 2) AS confidence_vs_bank_avg,
        
        -- Performance relative à la moyenne par localisation
        ROUND(CAST(rating - location_avg_rating AS NUMERIC), 2) AS rating_vs_location_avg,
        
        -- Performance relative à la moyenne du topic
        ROUND(CAST(rating - topic_avg_rating AS NUMERIC), 2) AS rating_vs_topic_avg,
        
        -- Indicateurs de performance
        CASE 
            WHEN rating > bank_avg_rating THEN 'Au-dessus moyenne banque'
            WHEN rating = bank_avg_rating THEN 'Moyenne banque'
            ELSE 'En-dessous moyenne banque'
        END AS performance_vs_bank,
        
        CASE 
            WHEN rating > location_avg_rating THEN 'Au-dessus moyenne zone'
            WHEN rating = location_avg_rating THEN 'Moyenne zone'
            ELSE 'En-dessous moyenne zone'
        END AS performance_vs_location
    FROM contextual_metrics cm
)

-- Sélection finale avec jointures aux dimensions
SELECT 
    pi.review_id,
    db.bank_id,
    dbr.branch_id,
    dl.location_id,
    dt.topic_id,
    ds.sentiment_id,
    
    -- Métriques originales
    pi.rating,
    pi.topic_confidence,
    pi.review_date,
    pi.processed_text,
    
    -- Nouvelles métriques pour visualisation
    pi.satisfaction_level,
    pi.satisfaction_score,
    pi.is_positive_rating,
    pi.is_negative_rating,
    pi.is_positive_sentiment,
    pi.is_negative_sentiment,
    
    -- Dimensions temporelles
    pi.review_year,
    pi.review_month,
    pi.review_quarter,
    pi.year_month,
    pi.year_quarter,
    pi.day_of_week,
    pi.week_period,
    pi.review_age_days,
    pi.review_freshness,
    
    -- Métriques de qualité
    pi.confidence_level,
    pi.text_length,
    pi.review_detail_level,
    pi.sentiment_rating_alignment,
    
    -- Métriques de contexte
    pi.bank_avg_rating,
    pi.bank_avg_confidence,
    pi.branch_avg_rating,
    pi.branch_avg_confidence,
    pi.location_avg_rating,
    pi.topic_avg_rating,
    pi.total_reviews_bank,
    pi.total_reviews_branch,
    pi.total_reviews_location,
    pi.total_reviews_topic,
    
    -- Indicateurs de performance relative
    pi.rating_vs_bank_avg,
    pi.confidence_vs_bank_avg,
    pi.rating_vs_location_avg,
    pi.rating_vs_topic_avg,
    pi.performance_vs_bank,
    pi.performance_vs_location,
    
    -- Métriques calculées additionnelles pour Looker Studio
    CASE WHEN pi.total_reviews_branch >= 50 THEN 'Volume élevé' 
         WHEN pi.total_reviews_branch >= 20 THEN 'Volume moyen'
         ELSE 'Volume faible' 
    END AS branch_review_volume_category,
    
    -- Score composite (pondération rating 70% + confidence 30%)
    ROUND(CAST((pi.rating * 0.7) + (pi.topic_confidence * 5 * 0.3) AS NUMERIC), 2) AS composite_score

FROM performance_indicators pi
LEFT JOIN "google_reviews_db"."public_public"."dim_bank" db ON pi.bank_name = db.bank_name
LEFT JOIN "google_reviews_db"."public_public"."dim_branch" dbr ON pi.branch_name = dbr.branch_name  
LEFT JOIN "google_reviews_db"."public_public"."dim_location" dl ON pi.location = dl.location
LEFT JOIN "google_reviews_db"."public_public"."dim_topic" dt ON pi.topic = dt.topic_name
LEFT JOIN "google_reviews_db"."public_public"."dim_sentiment" ds ON pi.sentiment = ds.sentiment_label
  );