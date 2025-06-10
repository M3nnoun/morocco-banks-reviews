

WITH nlp_enhanced_reviews AS (
    SELECT
        bank_name,
        branch_name,
        location,
        review_text,
        review_text_cleaned,
        rating,
        review_date,
        
        -- Ajout de la détection de langue
        
    /*
    Cette macro utilise une approche basée sur les règles pour détecter la langue
    en identifiant des caractères et motifs spécifiques à chaque langue.
    Pour une détection plus précise, utiliser une UDF Python avec langdetect ou langid
    */
    CASE
        -- Français: détection par présence d'accents et caractères spécifiques
        WHEN review_text ~* '[àáâäæçèéêëìíîïòóôöùúûüÿ]|(^|\s)(je|nous|vous|tu|il|elle|les|des|du|au|aux|est|sont|ont|cette|ce|ces|cette|cet)(\s|$)' THEN 'fr'
        
        -- Anglais: détection par défaut si pas de caractères spéciaux ou motifs d'autres langues
        WHEN review_text ~* '(^|\s)(the|this|that|these|those|is|are|was|were|have|has|had|will|would|a|an|of|for|to|with|by|at|on|in)(\s|$)' THEN 'en'
        
        -- Allemand: détection par présence d'umlauts et autres caractères allemands
        WHEN review_text ~* '[äöüßÄÖÜ]|(^|\s)(und|oder|das|ist|sind|haben|werden|ich|du|er|sie|wir|ihr|sie|den|dem|die|ein|eine)(\s|$)' THEN 'de'
        
        -- Espagnol: détection par présence de caractères et motifs spécifiques à l'espagnol
        WHEN review_text ~* '[ñáéíóúü¿¡]|(^|\s)(el|la|los|las|un|una|unos|unas|y|o|pero|porque|como|cuando|donde|quien|que|es|son|estar|haber)(\s|$)' THEN 'es'
        
        -- Italien: détection par présence de caractères et motifs spécifiques à l'italien
        WHEN review_text ~* '[àèéìíòóùú]|(^|\s)(il|lo|la|i|gli|le|un|uno|una|e|o|ma|perché|come|quando|dove|chi|che|è|sono|stare|avere)(\s|$)' THEN 'it'
        
        -- Inconnu si aucun motif ne correspond
        ELSE 'unknown'
    END
 AS language,
        
        -- Ajout de l'analyse de sentiment
        
    /*
    Macro d'analyse de sentiment qui combine:
    1. L'approche basée sur les notes (rating) quand disponible
    2. L'analyse lexicale basée sur les mots-clés positifs et négatifs en français
    3. Gestion des négations pour éviter les faux positifs
    */
    CASE
        -- Quand la note est disponible, elle est utilisée comme principale source
        WHEN rating IS NOT NULL THEN
            CASE
                WHEN rating >= 4 THEN 'Positive'
                WHEN rating <= 2 THEN 'Negative'
                ELSE 'Neutral'
            END
        
        -- Analyse basée sur le texte quand la note n'est pas disponible
        ELSE
            CASE
                -- Expressions positives fortes
                WHEN review_text_cleaned ~* '(excellent|parfait|super|génial|extraordinaire|formidable|impeccable|merveilleux|fantastique|remarquable)'
                    AND NOT review_text_cleaned ~* '(pas |non |aucun |jamais |ni )+(excellent|parfait|super|génial|extraordinaire|formidable|impeccable|merveilleux|fantastique|remarquable)'
                    THEN 'Positive'
                
                -- Expressions positives modérées
                WHEN review_text_cleaned ~* '(bien|bon|bonne|satisfait|satisfaisant|content|agréable|plaisant|pratique|efficace|utile|recommande|positif|merci|bravo|sympa|recommand)'
                    AND NOT review_text_cleaned ~* '(pas |non |aucun |jamais |ni )+(bien|bon|bonne|satisfait|satisfaisant|content|agréable|plaisant|pratique|efficace|utile|recommande|positif|merci|bravo|sympa)'
                    THEN 'Positive'
                
                -- Expressions négatives fortes
                WHEN review_text_cleaned ~* '(horrible|terrible|nul|catastrophe|catastrophique|désastre|scandaleux|inacceptable|inadmissible|déplorable|honteux)'
                    AND NOT review_text_cleaned ~* '(pas |non |aucun |jamais |ni )+(horrible|terrible|nul|catastrophe|catastrophique|désastre|scandaleux|inacceptable|inadmissible|déplorable|honteux)'
                    THEN 'Negative'
                
                -- Expressions négatives modérées
                WHEN review_text_cleaned ~* '(mauvais|déçu|décevant|problème|pénible|difficile|médiocre|ennuyeux|ennui|insatisfait|regrettable|dommage|arnaque|incompétent|impoli|désagréable|attente)'
                    AND NOT review_text_cleaned ~* '(pas |non |aucun |jamais |ni )+(mauvais|déçu|décevant|problème|pénible|difficile|médiocre|ennuyeux|ennui|insatisfait|regrettable|dommage|arnaque|incompétent|impoli|désagréable)'
                    THEN 'Negative'
                
                -- Expressions de négation de négatif (double négation = positif)
                WHEN review_text_cleaned ~* '(pas |non |aucun |jamais |ni )+(mauvais|problème|difficulté|pénible|difficile|ennuyeux)'
                    THEN 'Positive'
                
                -- Expressions de négation de positif (négation de positif = négatif)  
                WHEN review_text_cleaned ~* '(pas |non |aucun |jamais |ni )+(bien|bon|bonne|satisfait|content|agréable|utile)'
                    THEN 'Negative'
                
                -- Par défaut, considéré comme neutre
                ELSE 'Neutral'
            END
    END
 AS sentiment,
        
        -- Ajout de l'extraction des sujets
        
    /*
    Macro d'extraction de sujets basée sur des règles lexicales
    pour identifier les thèmes fréquents dans les avis bancaires.
    
    Renvoie un tableau des sujets identifiés dans le texte.
    Note: Pour une véritable analyse LDA, utiliser une UDF Python
    */
    ARRAY_REMOVE(ARRAY[
        -- Thème: Service client
        CASE WHEN review_text_cleaned ~* '(service client|service clientèle|conseiller|conseillers|conseillère|conseillères|accueil|réceptionniste|personnel|employé|employés|équipe|amabilité|gentil|gentille|courtois|courtoisie|poli|politesse|impoli|impolitesse|attente|file|attendre|attendu|longue attente|rendez-vous|rdv)' 
             THEN 'Service Client' 
             ELSE NULL 
        END,
        
        -- Thème: Opérations bancaires et frais
        CASE WHEN review_text_cleaned ~* '(compte|comptes|compte courant|compte épargne|livret|carte|cartes|carte bancaire|carte bleue|cb|crédit|crédits|prêt|prêts|emprunt|emprunts|épargne|virement|virements|frais|tarif|tarifs|coût|coûts|commission|commissions|taux|intérêt|intérêts|découvert|agios|gratuit|gratuité|facturation|facturé|facturée)' 
             THEN 'Opérations Bancaires' 
             ELSE NULL 
        END,
        
        -- Thème: Services numériques
        CASE WHEN review_text_cleaned ~* '(application|appli|applis|mobile|en ligne|web|site|site web|site internet|internet|connexion|connecter|connecté|login|mot de passe|identifiant|authentification|sécurité|sécurisé|digital|numérique|virtuel|dématérialisé|smartphone|téléphone|ordinateur|pc|portable|notification|notifier|message|email|mail|alerte)' 
             THEN 'Services Numériques' 
             ELSE NULL 
        END,
        
        -- Thème: Agences et accessibilité
        CASE WHEN review_text_cleaned ~* '(agence|agences|filiale|filiales|succursale|succursales|bureau|bureaux|guichet|guichets|distributeur|distributeurs|atm|dab|gab|horaire|horaires|ouverture|fermeture|fermé|fermée|ouvrir|accessibilité|accessible|proximité|proche|loin|distance|parking|stationnement|transport|métro|bus|tram|adresse)' 
             THEN 'Agences et Accessibilité' 
             ELSE NULL 
        END,
        
        -- Thème: Conseil et expertise financière
        CASE WHEN review_text_cleaned ~* '(conseil|conseils|conseiller financier|expertise|expert|placement|placements|investissement|investissements|finance|finances|financier|financière|patrimoine|patrimonial|épargne|assurance|assurances|retraite|immobilier|projet|projets|étude|études|simulation|évaluation|professionnel|professionnalisme|compétent|compétence|incompétent|incompétence)' 
             THEN 'Conseil Financier' 
             ELSE NULL 
        END,
        
        -- Thème: Résolution de problèmes
        CASE WHEN review_text_cleaned ~* '(problème|problèmes|incident|incidents|erreur|erreurs|résoudre|résolution|solution|solutions|régler|régularisation|litige|litiges|réclamation|réclamations|plainte|plaintes|dysfonctionnement|panne|pannes|bug|bugs|assistance|aider|aide|support|technique|réparer|correction|rectifier|rectification)' 
             THEN 'Résolution de Problèmes' 
             ELSE NULL 
        END,
        
        -- Thème: Confidentialité et sécurité
        CASE WHEN review_text_cleaned ~* '(sécurité|sécurisé|sécurisation|confidentiel|confidentialité|secret|privé|vie privée|données|donnée|information|informations|protection|protéger|protégé|fraude|frauduleux|piratage|pirater|piraté|hacker|vol|volé|arnaque|arnaquer|escroquerie|escroc)' 
             THEN 'Confidentialité et Sécurité' 
             ELSE NULL 
        END
    ], NULL)
 AS topics
    FROM public_public.cleaned_reviews
 
        
)

SELECT
    bank_name,
    branch_name,
    location,
    review_text,
    review_text_cleaned,
    rating,
    review_date,
    language,
    sentiment,
    topics,
    
    -- Conversion des sujets en colonnes booléennes pour faciliter les analyses
    CASE WHEN 'Service Client' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_service_client,
    CASE WHEN 'Opérations Bancaires' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_operations,
    CASE WHEN 'Services Numériques' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_digital,
    CASE WHEN 'Agences et Accessibilité' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_agences,
    CASE WHEN 'Conseil Financier' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_conseil,
    CASE WHEN 'Résolution de Problèmes' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_problemes,
    CASE WHEN 'Confidentialité et Sécurité' = ANY(topics) THEN TRUE ELSE FALSE END AS topic_securite,
    
    -- Statistiques sur les sujets
    ARRAY_LENGTH(topics, 1) AS topic_count,
    
    -- Date d'exécution pour le suivi
    CURRENT_TIMESTAMP AS processed_at
FROM 
    nlp_enhanced_reviews
WHERE
    -- Filtrage des avis avec une langue détectée (exclusion des 'unknown')
    language != 'unknown'
ORDER BY
    bank_name,
    branch_name,
    review_date DESC NULLS LAST