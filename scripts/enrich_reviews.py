import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from langdetect import detect
from transformers import pipeline
import spacy
import logging
import os
import warnings
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import nltk

class ReviewAnalysisConfig:
    DATABASE_URI = "postgresql://elhassan:elhassan@localhost:5432/google_reviews_db"
    LOG_FILE = "review_analysis.log"
    BATCH_SIZE = 100
    NUM_TOPICS = 7

class ReviewAnalyzer:
    def __init__(self):
        # Configuration du logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(ReviewAnalysisConfig.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        warnings.filterwarnings('ignore')
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

    def setup_nlp_resources(self):
        try:
            nltk.download('stopwords', quiet=True)
            from nltk.corpus import stopwords
            self.french_stopwords = set(stopwords.words('french'))

            self.nlp = spacy.load("fr_core_news_sm")
            self.logger.info("Ressources NLP chargées avec succès")
        except Exception as e:
            self.logger.error(f"Erreur de configuration NLP : {e}")
            raise

    def connect_database(self):
        try:
            self.engine = create_engine(ReviewAnalysisConfig.DATABASE_URI)
            self.logger.info("Connexion à la base de données réussie")
            return self.engine
        except Exception as e:
            self.logger.error(f"Échec de connexion à la base de données : {e}")
            raise

    def fetch_reviews(self):
        try:
            with self.engine.connect() as connection:
                query = "SELECT * FROM public_public.cleaned_reviews"
                result = connection.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                # Convertir review_date en date sans heure
                df['review_date'] = pd.to_datetime(df['review_date']).dt.date
                
                self.logger.info(f"Reviews récupérées. Total: {len(df)} lignes")
                return df
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des reviews : {e}")
            raise

    def preprocess_text(self, text):
        if not isinstance(text, str) or len(text.strip()) == 0:
            return []
        
        try:
            text = text[:1000]
            doc = self.nlp(text)
            
            tokens = [
                token.lemma_.lower() 
                for token in doc 
                if (not token.is_stop and 
                    not token.is_punct and 
                    len(token.lemma_) > 2 and 
                    token.lemma_.lower() not in self.french_stopwords)
            ]
            return tokens
        except Exception as e:
            self.logger.warning(f"Erreur de prétraitement : {e}")
            return []

    def map_topics(self, topic_indices):
        """Mapper les indices de topics en libellés plus descriptifs"""
        topic_labels = {
            0: "Service Client",
            1: "Infrastructure Bancaire", 
            2: "Transactions Financières",
            3: "Relation Personnelle",
            4: "Expérience Digitale",
            5: "Conseil Financier", 
            6: "Performance Opérationnelle"
        }
        return [topic_labels.get(idx, f"Sujet {idx+1}") for idx in topic_indices]

    def extract_topics(self, processed_texts):
        try:
            vectorizer = TfidfVectorizer(
                stop_words=list(self.french_stopwords),
                max_features=5000,
                min_df=3,
                max_df=0.9
            )
            X = vectorizer.fit_transform(processed_texts)
            
            lda_model = LatentDirichletAllocation(
                n_components=ReviewAnalysisConfig.NUM_TOPICS, 
                random_state=42,
                max_iter=15
            )
            lda_output = lda_model.fit_transform(X)
            
            feature_names = vectorizer.get_feature_names_out()
            topics = {}
            for topic_idx, topic in enumerate(lda_model.components_):
                top_words = [feature_names[i] for i in topic.argsort()[:-10:-1]]
                topics[f"Sujet {topic_idx+1}"] = top_words
            
            # Mapper les indices de topics avec des libellés descriptifs
            topic_labels = self.map_topics(lda_output.argmax(axis=1))
            
            return topic_labels, topics
        except Exception as e:
            self.logger.error(f"Erreur d'extraction des sujets : {e}")
            return ["Sujet Inconnu"] * len(processed_texts), {}

    def run_analysis(self):
        try:
            self.setup_nlp_resources()
            self.connect_database()
            
            df = self.fetch_reviews()
            
            # Filtrage des reviews françaises
            # french_reviews = df[df['language'] == 'fr'].copy()
            french_reviews = df.copy()
            self.logger.info(f"Reviews françaises : {len(french_reviews)}")
            
            # Prétraitement du texte
            french_reviews['processed_tokens'] = french_reviews['review_text_cleaned'].apply(self.preprocess_text)
            french_reviews['processed_text'] = french_reviews['processed_tokens'].apply(' '.join)
            
            # Point clé : suppression de processed_text_str
            french_reviews.drop(columns=['processed_text_str'], inplace=True, errors='ignore')
            
            # Extraction des sujets
            french_reviews['topic'], topics = self.extract_topics(french_reviews['processed_text'])
            
            # Création du rapport de synthèse
            summary = {
                "total_reviews": len(df),
                "french_reviews": len(french_reviews),
                "topics": topics
            }
            
            # Affichage des résultats
            self.logger.info("\n--- Résumé de l'analyse ---")
            self.logger.info(f"Total des reviews : {summary['total_reviews']}")
            self.logger.info(f"Reviews françaises : {summary['french_reviews']}")
            
            self.logger.info("\nSujets principaux:")
            for topic, words in summary['topics'].items():
                self.logger.info(f"{topic}: {', '.join(words)}")
            
            return french_reviews, summary
        
        except Exception as e:
            self.logger.error(f"Erreur durant l'analyse : {e}")
            raise

def main():
    analyzer = ReviewAnalyzer()
    french_reviews, summary = analyzer.run_analysis()
    
    # Afficher quelques exemples pour vérification
    print("\nExemples de reviews :")
    print(french_reviews[['review_text_cleaned', 'review_date', 'topic']].head())

if __name__ == "__main__":
    main()