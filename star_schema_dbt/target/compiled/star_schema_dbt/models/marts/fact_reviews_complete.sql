-- models/marts/fact_reviews_complete.sql
-- Table de faits finale avec toutes les dimensions


WITH base_fact AS (
    SELECT *
    FROM "google_reviews_db"."public_public"."fact_reviews"
),

-- Enrichissement avec toutes les dimensions
enriched_fact AS (
    SELECT 
        bf.*,
        
        -- Dimensions de base
        db.bank_name,
        dbr.branch_name,
        dl.location,
        dl.city,
        dl.postal_code,
        dt.topic_name,
        ds.sentiment_label,
        
        -- Nouvelle dimension région
        dr.region_id,
        dr.region_name,
        
        -- Métriques dérivées supplémentaires
        CASE 
            WHEN bf.composite_score >= 4.0 THEN 'Excellent'
            WHEN bf.composite_score >= 3.5 THEN 'Bon'
            WHEN bf.composite_score >= 2.5 THEN 'Moyen'
            ELSE 'Faible'
        END AS performance_category,
        
        -- Score normalisé sur 100
        ROUND(CAST(bf.composite_score * 20 AS NUMERIC), 1) AS score_sur_100,
        
        -- Catégorie de maturité de l'agence basée sur le volume
        CASE 
            WHEN bf.total_reviews_branch >= 100 THEN 'Très mature'
            WHEN bf.total_reviews_branch >= 50 THEN 'Mature'
            WHEN bf.total_reviews_branch >= 20 THEN 'En développement'
            ELSE 'Nouvelle'
        END AS branch_maturity,
        
        -- Indicateur de fiabilité basé sur volume et confiance
        CASE 
            WHEN bf.total_reviews_branch >= 30 AND bf.topic_confidence >= 0.7 THEN 'Très fiable'
            WHEN bf.total_reviews_branch >= 10 AND bf.topic_confidence >= 0.5 THEN 'Fiable'
            WHEN bf.total_reviews_branch >= 5 THEN 'Moyennement fiable'
            ELSE 'Peu fiable'
        END AS reliability_indicator,
        
        -- Segment de performance régionale
        CASE 
            WHEN bf.rating_vs_location_avg > 0.5 THEN 'Leader régional'
            WHEN bf.rating_vs_location_avg > 0 THEN 'Au-dessus moyenne régionale'
            WHEN bf.rating_vs_location_avg >= -0.3 THEN 'Moyenne régionale'
            ELSE 'En-dessous moyenne régionale'
        END AS regional_performance_segment

    FROM base_fact bf
    LEFT JOIN "google_reviews_db"."public_public"."dim_bank" db ON bf.bank_id = db.bank_id
    LEFT JOIN "google_reviews_db"."public_public"."dim_branch" dbr ON bf.branch_id = dbr.branch_id
    LEFT JOIN "google_reviews_db"."public_public"."dim_location" dl ON bf.location_id = dl.location_id
    LEFT JOIN "google_reviews_db"."public_public"."dim_topic" dt ON bf.topic_id = dt.topic_id
    LEFT JOIN "google_reviews_db"."public_public"."dim_sentiment" ds ON bf.sentiment_id = ds.sentiment_id
    LEFT JOIN "google_reviews_db"."public_public"."dim_region" dr ON dl.city = dr.city
),

-- Métriques agrégées par région (précalculées)
regional_aggregates AS (
    SELECT 
        ef.region_id,
        ef.bank_id,
        ROUND(CAST(AVG(ef.rating) AS NUMERIC), 2) AS region_avg_rating,
        ROUND(CAST(AVG(ef.composite_score) AS NUMERIC), 2) AS region_avg_composite_score,
        ROUND(CAST(AVG(ef.topic_confidence) AS NUMERIC), 2) AS region_avg_confidence,
        COUNT(*) AS total_reviews_region,
        COUNT(DISTINCT ef.bank_id) AS total_banks_region,
        COUNT(DISTINCT ef.branch_id) AS total_branches_region,
        COUNT(*) AS bank_reviews_in_region,
        ROUND(CAST(AVG(ef.rating) AS NUMERIC), 2) AS bank_avg_rating_in_region
    FROM enriched_fact ef
    WHERE ef.region_id IS NOT NULL
    GROUP BY ef.region_id, ef.bank_id
),

-- Calcul des parts de marché et rangs
regional_market_analysis AS (
    SELECT 
        ra.*,
        -- Part de marché par banque dans la région
        ROUND(CAST(
            ra.bank_reviews_in_region * 100.0 / ra.total_reviews_region
        AS NUMERIC), 2) AS bank_market_share_region,
        
        -- Rang de la banque dans la région
        DENSE_RANK() OVER (
            PARTITION BY ra.region_id 
            ORDER BY ra.bank_avg_rating_in_region DESC
        ) AS bank_rank_in_region
        
    FROM regional_aggregates ra
),

-- Métriques par région uniquement (pour éviter la duplication)
regional_totals AS (
    SELECT 
        ef.region_id,
        ROUND(CAST(AVG(ef.rating) AS NUMERIC), 2) AS region_avg_rating,
        ROUND(CAST(AVG(ef.composite_score) AS NUMERIC), 2) AS region_avg_composite_score,
        ROUND(CAST(AVG(ef.topic_confidence) AS NUMERIC), 2) AS region_avg_confidence,
        COUNT(*) AS total_reviews_region,
        COUNT(DISTINCT ef.bank_id) AS total_banks_region,
        COUNT(DISTINCT ef.branch_id) AS total_branches_region
    FROM enriched_fact ef
    WHERE ef.region_id IS NOT NULL
    GROUP BY ef.region_id
),

-- Jointure finale avec les métriques régionales
regional_context AS (
    SELECT 
        ef.*,
        rt.region_avg_rating,
        rt.region_avg_composite_score,
        rt.region_avg_confidence,
        rt.total_reviews_region,
        rt.total_banks_region,
        rt.total_branches_region,
        COALESCE(rma.bank_market_share_region, 0) AS bank_market_share_region,
        COALESCE(rma.bank_rank_in_region, 999) AS bank_rank_in_region
        
    FROM enriched_fact ef
    LEFT JOIN regional_totals rt ON ef.region_id = rt.region_id
    LEFT JOIN regional_market_analysis rma ON ef.region_id = rma.region_id AND ef.bank_id = rma.bank_id
)

-- Sélection finale avec toutes les métriques
SELECT 
    -- IDs et clés
    rc.review_id,
    rc.bank_id,
    rc.branch_id,
    rc.location_id,
    rc.topic_id,
    rc.sentiment_id,
    rc.region_id,
    
    -- Informations descriptives
    rc.bank_name,
    rc.branch_name,
    rc.location,
    rc.city,
    rc.postal_code,
    rc.region_name,
    rc.topic_name,
    rc.sentiment_label,
    
    -- Métriques de base
    rc.rating,
    rc.topic_confidence,
    rc.review_date,
    rc.processed_text,
    
    -- Métriques de satisfaction
    rc.satisfaction_level,
    rc.satisfaction_score,
    rc.composite_score,
    rc.score_sur_100,
    rc.performance_category,
    
    -- Indicateurs binaires
    rc.is_positive_rating,
    rc.is_negative_rating,
    rc.is_positive_sentiment,
    rc.is_negative_sentiment,
    
    -- Dimensions temporelles
    rc.review_year,
    rc.review_month,
    rc.review_quarter,
    rc.year_month,
    rc.year_quarter,
    rc.day_of_week,
    rc.week_period,
    rc.review_age_days,
    rc.review_freshness,
    
    -- Métriques de qualité
    rc.confidence_level,
    rc.text_length,
    rc.review_detail_level,
    rc.sentiment_rating_alignment,
    rc.reliability_indicator,
    
    -- Contexte agence
    rc.branch_avg_rating,
    rc.branch_avg_confidence,
    rc.total_reviews_branch,
    rc.branch_review_volume_category,
    rc.branch_maturity,
    
    -- Contexte banque
    rc.bank_avg_rating,
    rc.bank_avg_confidence,
    rc.total_reviews_bank,
    rc.bank_market_share_region,
    rc.bank_rank_in_region,
    
    -- Contexte localisation
    rc.location_avg_rating,
    rc.total_reviews_location,
    
    -- Contexte régional
    rc.region_avg_rating,
    rc.region_avg_composite_score,
    rc.region_avg_confidence,
    rc.total_reviews_region,
    rc.total_banks_region,
    rc.total_branches_region,
    
    -- Contexte topic
    rc.topic_avg_rating,
    rc.total_reviews_topic,
    
    -- Performance relative
    rc.rating_vs_bank_avg,
    rc.confidence_vs_bank_avg,
    rc.rating_vs_location_avg,
    rc.rating_vs_topic_avg,
    ROUND(CAST(rc.rating - rc.region_avg_rating AS NUMERIC), 2) AS rating_vs_region_avg,
    
    -- Segments de performance
    rc.performance_vs_bank,
    rc.performance_vs_location,
    rc.regional_performance_segment,
    
    -- Métriques calculées pour l'analyse
    -- Écart-type pour mesurer la variabilité
    ROUND(CAST(STDDEV(rc.rating) OVER (PARTITION BY rc.bank_id) AS NUMERIC), 2) AS bank_rating_std,
    ROUND(CAST(STDDEV(rc.rating) OVER (PARTITION BY rc.region_id) AS NUMERIC), 2) AS region_rating_std,
    
    -- Coefficient de variation (mesure de consistance)
    CASE 
        WHEN rc.bank_avg_rating > 0 THEN 
            ROUND(CAST(STDDEV(rc.rating) OVER (PARTITION BY rc.bank_id) / rc.bank_avg_rating AS NUMERIC), 2)
        ELSE NULL
    END AS bank_rating_cv,
    
    -- Index de performance composite (comparaison multi-niveaux)
    ROUND(CAST(
        (rc.rating_vs_bank_avg * 0.3) + 
        (rc.rating_vs_location_avg * 0.3) + 
        (rc.rating - rc.region_avg_rating * 0.4)
    AS NUMERIC), 2) AS performance_index,
    
    -- Timestamp de création
    CURRENT_TIMESTAMP AS created_at

FROM regional_context rc

-- Ajout d'un index pour optimiser les performances
