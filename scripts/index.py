
import pandas as pd
import numpy as np
import os
import logging
import warnings
from sqlalchemy import create_engine, text, Table, Column, String, Date, MetaData, Float, Numeric
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.cluster import KMeans
import nltk
from textblob import TextBlob
from textblob_fr import PatternAnalyzer
from collections import Counter
import re

# Désactiver le GPU
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

class ReviewAnalysisConfig:
    DATABASE_URI = "postgresql://elhassan:elhassan@localhost:5432/google_reviews_db"
    LOG_FILE = "review_analysis.log"
    BATCH_SIZE = 100
    NUM_TOPICS = 15  # Augmenté pour plus de granularité
    NLTK_PATH = "/home/elhassan/nltk_data"

class ImprovedReviewAnalyzer:
    def __init__(self):
        self._setup_logging()
        self._configure_environment()
        self.engine = None
        self.nlp = None
        self.french_stopwords = None
        self._init_topic_mappings()

    def _init_topic_mappings(self):
        """Initialise les mappings pour une meilleure interprétation des topics"""
        self.topic_keywords_mapping = {
            'service_quality_excellent': ['excellent', 'parfait', 'exceptionnel', 'qualité', 'top'],
            'service_quality_poor': ['mauvais', 'nul', 'catastrophique', 'pire', 'horrible'],
            'personnel_competent': ['personnel', 'professionnel', 'compétent', 'serviable', 'aimable'],
            'personnel_problematic': ['impoli', 'incompétent', 'désagréable', 'agent', 'staff'],
            'waiting_time_long': ['attendre', 'attente', 'lent', 'temps', 'file', 'queue'],
            'waiting_time_quick': ['rapide', 'vite', 'efficace', 'immédiat', 'prompt'],
            'phone_communication': ['téléphone', 'appeler', 'communication', 'numéro', 'ligne'],
            'physical_infrastructure': ['agence', 'banque', 'bâtiment', 'locaux', 'emplacement'],
            'atm_services': ['guichet', 'distributeur', 'automatique', 'retrait', 'dépôt'],
            'account_operations': ['compte', 'opération', 'transaction', 'virement', 'solde'],
            'fees_charges': ['frais', 'commission', 'coût', 'tarif', 'prix'],
            'opening_hours': ['horaire', 'ouverture', 'fermeture', 'heure', 'temps'],
            'security_issues': ['sécurité', 'vol', 'fraude', 'problème', 'risque'],
            'digital_services': ['application', 'site', 'internet', 'numérique', 'en_ligne'],
            'customer_experience': ['expérience', 'satisfaction', 'recommander', 'éviter', 'client']
        }
        
        self.topic_names_fr = {
            'service_quality_excellent': 'Service Excellent',
            'service_quality_poor': 'Service Défaillant',
            'personnel_competent': 'Personnel Compétent',
            'personnel_problematic': 'Personnel Problématique',
            'waiting_time_long': 'Temps d\'Attente Excessif',
            'waiting_time_quick': 'Service Rapide',
            'phone_communication': 'Communication Téléphonique',
            'physical_infrastructure': 'Infrastructure Physique',
            'atm_services': 'Services Guichet Automatique',
            'account_operations': 'Opérations de Compte',
            'fees_charges': 'Frais et Tarification',
            'opening_hours': 'Horaires d\'Ouverture',
            'security_issues': 'Questions de Sécurité',
            'digital_services': 'Services Numériques',
            'customer_experience': 'Expérience Client Globale'
        }

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(ReviewAnalysisConfig.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _configure_environment(self):
        warnings.filterwarnings('ignore')
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
        nltk.data.path.append(ReviewAnalysisConfig.NLTK_PATH)

    def setup_nlp_resources(self):
        try:
            self._download_nltk_resources()
            self._load_spacy_model()
            self.logger.info("Ressources NLP françaises chargées")
        except Exception as e:
            self.logger.error(f"Erreur NLP : {e}")
            raise

    def _download_nltk_resources(self):
        try:
            nltk.data.find('corpora/stopwords', paths=[ReviewAnalysisConfig.NLTK_PATH])
        except LookupError:
            nltk.download('stopwords', quiet=True, download_dir=ReviewAnalysisConfig.NLTK_PATH)
            
        from nltk.corpus import stopwords
        self.french_stopwords = set(stopwords.words('french'))
        
        # Ajouter des stopwords spécifiques au domaine bancaire
        banking_stopwords = {'banque', 'agence', 'aller', 'faire', 'être', 'avoir', 'dire', 'voir', 'plus', 'bien', 'tout', 'fois', 'très', 'toujours', 'jamais'}
        self.french_stopwords.update(banking_stopwords)

    def _load_spacy_model(self):
        try:
            self.nlp = spacy.load("fr_core_news_sm")
        except OSError:
            os.system("python -m spacy download fr_core_news_sm")
            self.nlp = spacy.load("fr_core_news_sm")

    def analyze_sentiment(self, text):
        """Analyse le sentiment en français avec une approche NLP hybride"""
        try:
            if not isinstance(text, str) or len(text.strip()) < 3:
                return 'neutre'
            
            # Nettoyer le texte
            text_clean = text.lower().strip()
            
            # Analyse avec TextBlob-FR (plus précise pour le français)
            try:
                blob = TextBlob(text_clean, analyzer=PatternAnalyzer())
                polarity = blob.sentiment[0]
                
                # Analyse lexicale complémentaire avec des mots-clés français
                positive_words = [
                    'excellent', 'parfait', 'génial', 'super', 'formidable', 'magnifique',
                    'rapide', 'efficace', 'professionnel', 'serviable', 'aimable', 'courtois',
                    'satisfait', 'content', 'heureux', 'recommande', 'top', 'bien', 'bon',
                    'agréable', 'sympa', 'souriant', 'accueillant', 'compétent'
                ]
                
                negative_words = [
                    'horrible', 'nul', 'catastrophique', 'désastreux', 'inacceptable',
                    'lent', 'incompétent', 'impoli', 'désagréable', 'mauvais', 'pire',
                    'déçu', 'mécontent', 'frustré', 'énervé', 'éviter', 'fuyez',
                    'scandaleux', 'inadmissible', 'honte', 'zéro', 'aucun', 'jamais'
                ]
                
                # Compter les mots positifs et négatifs
                words = text_clean.split()
                pos_count = sum(1 for word in words if any(pw in word for pw in positive_words))
                neg_count = sum(1 for word in words if any(nw in word for nw in negative_words))
                
                # Combiner TextBlob et analyse lexicale
                lexical_score = (pos_count - neg_count) / max(len(words), 1)
                
                # Score final combiné (70% TextBlob + 30% lexical)
                final_score = 0.7 * polarity + 0.3 * lexical_score
                
                # Classification avec seuils ajustés
                if final_score > 0.1:
                    return 'positif'
                elif final_score < -0.1:
                    return 'négatif'
                else:
                    return 'neutre'
                    
            except Exception:
                # Fallback sur analyse lexicale seule
                words = text_clean.split()
                pos_count = sum(1 for word in words if any(pw in word for pw in positive_words))
                neg_count = sum(1 for word in words if any(nw in word for nw in negative_words))
                
                if pos_count > neg_count:
                    return 'positif'
                elif neg_count > pos_count:
                    return 'négatif'
                else:
                    return 'neutre'
                    
        except Exception as e:
            self.logger.warning(f"Erreur analyse sentiment: {e}")
            return 'neutre'

    def connect_database(self):
        try:
            self.engine = create_engine(ReviewAnalysisConfig.DATABASE_URI)
            self.logger.info("Connexion DB réussie")
            return self.engine
        except Exception as e:
            self.logger.error(f"Erreur connexion DB : {e}")
            raise

    def fetch_reviews(self):
        try:
            with self.engine.connect() as connection:
                query = """
                    SELECT 
                        bank_name,
                        branch_name,
                        city,
                        location,
                        review_text_cleaned,
                        rating,
                        review_date::date
                    FROM public_public.cleaned_reviews
                    WHERE review_text_cleaned IS NOT NULL
                      AND review_text_cleaned != ''
                      AND rating IS NOT NULL
                """
                result = connection.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                # Validation et nettoyage des données
                self.logger.info(f"Reviews brutes récupérées : {len(df)}")
                
                # Convertir les types de données
                df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
                df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
                
                # Supprimer les lignes avec des données critiques manquantes
                initial_count = len(df)
                df = df.dropna(subset=['rating', 'review_text_cleaned'])
                
                # Log des statistiques de nettoyage
                cleaned_count = len(df)
                if initial_count != cleaned_count:
                    self.logger.info(f"Lignes supprimées lors du nettoyage : {initial_count - cleaned_count}")
                
                self.logger.info(f"Reviews finales après nettoyage : {cleaned_count}")
                return df
                
        except Exception as e:
            self.logger.error(f"Erreur récupération : {e}")
            raise

    def preprocess_text(self, text):
        if not isinstance(text, str) or len(text.strip()) < 10:
            return []
        
        try:
            # Nettoyer le texte
            text = re.sub(r'[^\w\s]', ' ', text.lower())
            text = re.sub(r'\s+', ' ', text).strip()
            
            doc = self.nlp(text[:1000])
            tokens = []
            
            for token in doc:
                if (not token.is_stop and 
                    not token.is_punct and 
                    not token.is_space and
                    len(token.lemma_) > 2 and
                    token.lemma_.lower() not in self.french_stopwords and
                    token.pos_ in ['NOUN', 'ADJ', 'VERB']):
                    tokens.append(token.lemma_.lower())
            
            return tokens
        except Exception as e:
            self.logger.warning(f"Erreur prétraitement : {e}")
            return []

    def _categorize_topic(self, keywords):
        """Catégorise un topic basé sur ses mots-clés"""
        keyword_scores = {}
        
        for category, cat_keywords in self.topic_keywords_mapping.items():
            score = sum(1 for kw in keywords if kw.lower() in [ck.lower() for ck in cat_keywords])
            if score > 0:
                keyword_scores[category] = score
        
        if keyword_scores:
            best_category = max(keyword_scores.items(), key=lambda x: x[1])[0]
            return self.topic_names_fr[best_category]
        
        return self._generate_contextual_name(keywords)

    def _generate_contextual_name(self, keywords):
        """Génère un nom contextuel basé sur l'analyse des mots-clés"""
        # Analyser les sentiments implicites dans les mots-clés
        positive_words = ['excellent', 'bon', 'meilleur', 'professionnel', 'rapide', 'serviable']
        negative_words = ['mauvais', 'pire', 'lent', 'nul', 'impoli', 'éviter']
        
        pos_count = sum(1 for kw in keywords if kw.lower() in positive_words)
        neg_count = sum(1 for kw in keywords if kw.lower() in negative_words)
        
        # Identifier les aspects principaux
        if any(kw in keywords for kw in ['service', 'qualité']):
            if neg_count > pos_count:
                return "Service Insatisfaisant"
            elif pos_count > neg_count:
                return "Service de Qualité"
            else:
                return "Évaluation du Service"
        
        elif any(kw in keywords for kw in ['personnel', 'agent', 'accueil']):
            if neg_count > pos_count:
                return "Personnel Problématique"
            elif pos_count > neg_count:
                return "Personnel Compétent"
            else:
                return "Évaluation du Personnel"
        
        elif any(kw in keywords for kw in ['attendre', 'temps', 'lent']):
            return "Gestion du Temps d'Attente"
        
        elif any(kw in keywords for kw in ['téléphone', 'communication']):
            return "Communication Téléphonique"
        
        else:
            # Utiliser les 2 mots-clés les plus significatifs
            main_keywords = [kw for kw in keywords[:3] if len(kw) > 3]
            if len(main_keywords) >= 2:
                return f"{main_keywords[0].capitalize()} & {main_keywords[1].capitalize()}"
            elif len(main_keywords) == 1:
                return main_keywords[0].capitalize()
            else:
                return "Aspects Divers"

    def extract_topics(self, processed_texts):
        try:
            # Filtrer les textes vides
            valid_texts = [text for text in processed_texts if text.strip()]
            
            if len(valid_texts) < 20:  # Augmenté le minimum pour plus de topics
                self.logger.warning("Pas assez de textes valides pour l'analyse thématique")
                return ["Données Insuffisantes"] * len(processed_texts), {}, [0.0] * len(processed_texts), ["Topic_0"] * len(processed_texts)

            vectorizer = TfidfVectorizer(
                stop_words=list(self.french_stopwords),
                max_features=3000,  # Augmenté pour plus de vocabulaire
                min_df=2,           # Réduit pour capturer plus de nuances
                max_df=0.9,         # Augmenté légèrement
                ngram_range=(1, 3)  # Inclure les trigrammes pour plus de contexte
            )
            
            X = vectorizer.fit_transform(valid_texts)
            
            # Utiliser LDA avec des paramètres optimisés pour plus de topics
            lda = LatentDirichletAllocation(
                n_components=ReviewAnalysisConfig.NUM_TOPICS,
                random_state=42,
                max_iter=100,       # Plus d'itérations pour la convergence
                learning_method='batch',
                learning_decay=0.7,
                doc_topic_prior=0.05,   # Réduit pour plus de spécificité
                topic_word_prior=0.005  # Réduit pour des topics plus distincts
            )
            
            lda_output = lda.fit_transform(X)
            
            return self._process_improved_lda_results(lda, vectorizer, lda_output, processed_texts)
            
        except Exception as e:
            self.logger.error(f"Erreur topics : {e}")
            return ["Erreur d'Analyse"] * len(processed_texts), {}, [0.0] * len(processed_texts), ["Topic_Error"] * len(processed_texts)

    def _process_improved_lda_results(self, lda_model, vectorizer, lda_output, original_texts):
        feature_names = vectorizer.get_feature_names_out()
        themes = {}
        
        # Générer des thèmes améliorés
        for i, topic in enumerate(lda_model.components_):
            # Obtenir les mots-clés les plus importants
            top_indices = topic.argsort()[:-20:-1]  # Top 20 mots
            top_keywords = [feature_names[idx] for idx in top_indices]
            
            # Filtrer les mots-clés peu significatifs
            filtered_keywords = [kw for kw in top_keywords if len(kw) > 2 and not kw.isdigit()]
            
            # Générer un nom de thème contextuel
            theme_name = self._categorize_topic(filtered_keywords[:10])
            
            themes[f"Topic_{i}"] = {
                'name': theme_name,
                'keywords': ', '.join(filtered_keywords[:10]),
                'weight': topic.sum()
            }

        # Attribution des topics aux reviews
        topic_names = []
        topic_labels = []
        topic_scores = []
        
        text_index = 0
        for original_text in original_texts:
            if original_text.strip():  # Texte valide
                if text_index < len(lda_output):
                    topic_distribution = lda_output[text_index]
                    dominant_topic = np.argmax(topic_distribution)
                    confidence = topic_distribution[dominant_topic]
                    
                    topic_labels.append(f"Topic_{dominant_topic}")
                    topic_names.append(themes[f"Topic_{dominant_topic}"]['name'])
                    topic_scores.append(confidence)
                    text_index += 1
                else:
                    topic_labels.append("Topic_0")
                    topic_names.append(themes["Topic_0"]['name'])
                    topic_scores.append(0.0)
            else:  # Texte vide
                topic_labels.append("Topic_0")
                topic_names.append("Données Insuffisantes")
                topic_scores.append(0.0)
        
        return topic_names, themes, topic_scores, topic_labels

    def create_enriched_table(self):
        metadata = MetaData()
        Table(
            'enriched_reviews', metadata,
            Column('bank_name', String),
            Column('branch_name', String),
            Column('city', String),  # Ajout de la colonne city
            Column('location', String),
            Column('review_text_cleaned', String),
            Column('rating', Numeric),
            Column('review_date', Date),
            Column('processed_text', String),
            Column('topic', String),
            Column('topic_confidence', Float),
            Column('sentiment', String),
            schema='public_public'
        )
        with self.engine.begin() as conn:
            if not conn.dialect.has_table(conn, 'enriched_reviews', schema='public_public'):
                metadata.create_all(conn)
                self.logger.info("Table enriched_reviews créée avec la colonne city")

    def save_results(self, df):
        try:
            self.create_enriched_table()
            
            # Sauvegarder avec la colonne city incluse
            df_to_save = df[['bank_name', 'branch_name', 'city', 'location', 
                           'review_text_cleaned', 'rating', 'review_date',
                           'processed_text', 'topic', 'topic_confidence',
                           'sentiment']].copy()
            df_to_save['city']=df_to_save['city'].str.strip().str.replace('+', ' ').str.title()
            df_to_save.to_sql(
                name='enriched_reviews',
                con=self.engine,
                schema='public_public',
                if_exists='replace',
                index=False,
                chunksize=ReviewAnalysisConfig.BATCH_SIZE
            )
            self.logger.info(f"{len(df_to_save)} avis enrichis sauvegardés avec la colonne city")
        except Exception as e:
            self.logger.error(f"Erreur sauvegarde : {e}")
            raise

    def run_analysis(self):
        try:
            self.setup_nlp_resources()
            self.connect_database()
            
            df = self.fetch_reviews()
            
            # Prétraitement amélioré
            self.logger.info("Début du prétraitement...")
            df['processed_text'] = df['review_text_cleaned'].apply(
                lambda x: ' '.join(self.preprocess_text(x)) if isinstance(x, str) else ""
            )
            
            # Analyse de sentiment
            self.logger.info("Analyse de sentiment en cours...")
            df['sentiment'] = df['review_text_cleaned'].apply(self.analyze_sentiment)
            
            # Analyse thématique améliorée
            self.logger.info("Analyse thématique améliorée en cours...")
            df['topic'], topics, df['topic_confidence'], topic_labels = self.extract_topics(
                df['processed_text'].tolist()
            )

            # Sauvegarde avec la colonne city incluse
            self.save_results(df[['bank_name', 'branch_name', 'city', 'location', 
                                'review_text_cleaned', 'rating', 'review_date',
                                'processed_text', 'topic', 'topic_confidence',
                                'sentiment']])
            
            return df, topics
        except Exception as e:
            self.logger.error(f"Échec de l'analyse : {e}")
            raise

    def generate_summary_report(self, df, topics):
        """Génère un rapport de synthèse des analyses"""
        print("\n" + "="*80)
        print("RAPPORT D'ANALYSE DES REVIEWS BANCAIRES")
        print("="*80)
        
        print(f"\n📊 STATISTIQUES GÉNÉRALES:")
        print(f"   • Nombre total de reviews analysées: {len(df)}")
        print(f"   • Note moyenne: {df['rating'].mean():.2f}/5")
        
        # Gestion sécurisée des dates
        try:
            valid_dates = df['review_date'].dropna()
            if len(valid_dates) > 0:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                print(f"   • Période: {min_date} à {max_date}")
                print(f"   • Reviews avec dates valides: {len(valid_dates)}/{len(df)}")
            else:
                print(f"   • Période: Dates non disponibles")
        except Exception as e:
            print(f"   • Période: Erreur dans les dates ({str(e)})")
        
        print(f"\n💭 DISTRIBUTION DES SENTIMENTS:")
        sentiment_counts = df['sentiment'].value_counts()
        for sentiment, count in sentiment_counts.items():
            percentage = (count / len(df)) * 100
            print(f"   • {sentiment.capitalize()}: {count} ({percentage:.1f}%)")
        
        print(f"\n🎯 THÈMES IDENTIFIÉS:")
        topic_counts = df['topic'].value_counts()
        for i, (topic, count) in enumerate(topic_counts.items(), 1):
            percentage = (count / len(df)) * 100
            print(f"   {i}. {topic}: {count} reviews ({percentage:.1f}%)")
        
        print(f"\n🔍 DÉTAILS DES THÈMES:")
        for topic_id, details in topics.items():
            print(f"\n   📌 {details['name']}:")
            print(f"      Mots-clés: {details['keywords']}")
        
        print(f"\n🏦 PERFORMANCE PAR BANQUE:")
        try:
            bank_performance = df.groupby('bank_name').agg({
                'rating': 'mean',
                'sentiment': lambda x: (x == 'positif').sum() / len(x) * 100
            }).round(2)
            
            for bank, stats in bank_performance.iterrows():
                print(f"   • {bank}: Note {stats['rating']}/5, {stats['sentiment']:.1f}% positif")
        except Exception as e:
            print(f"   • Erreur dans l'analyse par banque: {str(e)}")
        
        # Analyse par ville
        print(f"\n🏙️ PERFORMANCE PAR VILLE:")
        try:
            city_performance = df.groupby('city').agg({
                'rating': 'mean',
                'sentiment': lambda x: (x == 'positif').sum() / len(x) * 100
            }).round(2).head(10)  # Top 10 des villes
            
            for city, stats in city_performance.iterrows():
                print(f"   • {city}: Note {stats['rating']}/5, {stats['sentiment']:.1f}% positif")
        except Exception as e:
            print(f"   • Erreur dans l'analyse par ville: {str(e)}")
        
        # Ajouter des statistiques supplémentaires
        print(f"\n📈 STATISTIQUES DÉTAILLÉES:")
        print(f"   • Reviews positives (rating ≥ 4): {len(df[df['rating'] >= 4])}")
        print(f"   • Reviews négatives (rating ≤ 2): {len(df[df['rating'] <= 2])}")
        print(f"   • Confiance moyenne des topics: {df['topic_confidence'].mean():.3f}")
        print(f"   • Nombre de villes différentes: {df['city'].nunique()}")
        
        # Top 3 des topics les plus fréquents
        top_topics = topic_counts.head(3)
        print(f"\n🔝 TOP 3 DES PRÉOCCUPATIONS:")
        for i, (topic, count) in enumerate(top_topics.items(), 1):
            percentage = (count / len(df)) * 100
            print(f"   {i}. {topic} ({percentage:.1f}%)")

def main():
    analyzer = ImprovedReviewAnalyzer()
    try:
        results, topics = analyzer.run_analysis()
        
        # Générer le rapport amélioré
        analyzer.generate_summary_report(results, topics)
        
        print("\n✅ Analyse terminée avec succès!")
        print(f"💾 Résultats sauvegardés dans la table 'enriched_reviews' avec la colonne city")
        
    except Exception as e:
        print(f"\n❌ Échec de l'analyse: {str(e)}")
        logging.error(f"Erreur principale: {e}", exc_info=True)

if __name__ == "__main__":
    main()