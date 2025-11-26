"""
Contrôles qualité automatisés pour données de ventes
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Effectue des contrôles qualité sur les données"""

    def __init__(self, df):
        self.df = df
        self.issues = []
        self.severity_counts = {'CRITICAL': 0, 'WARNING': 0, 'INFO': 0}

    def check_all(self):
        """Exécute tous les contrôles"""
        logger.info("🔍 Début des contrôles qualité...")

        self.check_missing_values()
        self.check_duplicates()
        self.check_data_types()
        self.check_negative_values()
        self.check_date_validity()
        self.check_outliers()
        self.check_business_rules()

        self._print_summary()
        return self

    def check_missing_values(self, threshold=0.05):
        """Vérifie les valeurs manquantes"""
        logger.info("  → Vérification des valeurs manquantes...")

        for col in self.df.columns:
            null_ratio = self.df[col].isnull().sum() / len(self.df)
            if null_ratio > 0:
                severity = 'CRITICAL' if null_ratio > threshold else 'WARNING'
                issue = {
                    'check': 'Missing Values',
                    'column': col,
                    'severity': severity,
                    'description': f"{null_ratio*100:.2f}% de valeurs manquantes",
                    'count': self.df[col].isnull().sum()
                }
                self.issues.append(issue)
                self.severity_counts[severity] += 1

                logger.warning(f"{col}: {null_ratio*100:.2f}% manquants")

        return self

    def check_duplicates(self):
        """Vérifie les doublons"""
        logger.info("  → Vérification des doublons...")

        # Doublons complets
        duplicates = self.df.duplicated().sum()
        if duplicates > 0:
            issue = {
                'check': 'Duplicates',
                'column': 'ALL',
                'severity': 'WARNING',
                'description': f"{duplicates} lignes dupliquées complètes",
                'count': duplicates
            }
            self.issues.append(issue)
            self.severity_counts['WARNING'] += 1
            logger.warning(f"    {duplicates} doublons complets détectés")

        # Doublons sur order_id
        if 'order_id' in self.df.columns:
            order_duplicates = self.df['order_id'].duplicated().sum()
            if order_duplicates > 0:
                issue = {
                    'check': 'Duplicates',
                    'column': 'order_id',
                    'severity': 'CRITICAL',
                    'description': f"{order_duplicates} order_id dupliqués",
                    'count': order_duplicates
                }
                self.issues.append(issue)
                self.severity_counts['CRITICAL'] += 1
                logger.error(f"     {order_duplicates} order_id dupliqués !")

        return self


    def check_negative_values(self):
        """Vérifie les valeurs négatives sur colonnes numériques critiques"""
        logger.info("  → Vérification des valeurs négatives...")

        critical_cols = ['quantity', 'unit_price', 'total_amount', 'final_amount']

        for col in critical_cols:
            if col in self.df.columns:
                negative_count = (self.df[col] < 0).sum()
                zero_count = (self.df[col] == 0).sum()

                if negative_count > 0:
                    issue = {
                        'check': 'Negative Values',
                        'column': col,
                        'severity': 'CRITICAL',
                        'description': f"{negative_count} valeurs négatives",
                        'count': negative_count
                    }
                    self.issues.append(issue)
                    self.severity_counts['CRITICAL'] += 1
                    logger.error(f"     {col}: {negative_count} valeurs négatives !")

                if zero_count > 0 and col in ['unit_price', 'total_amount']:
                    issue = {
                        'check': 'Zero Values',
                        'column': col,
                        'severity': 'WARNING',
                        'description': f"{zero_count} valeurs nulles (zéro)",
                        'count': zero_count
                    }
                    self.issues.append(issue)
                    self.severity_counts['WARNING'] += 1
                    logger.warning(f"     {col}: {zero_count} valeurs à zéro")

        return self

    def check_date_validity(self):
        """Vérifie la validité des dates"""
        logger.info("  → Vérification des dates...")

        if 'date' in self.df.columns:
            today = datetime.now()
            future_dates = (self.df['date'] > today).sum()

            if future_dates > 0:
                issue = {
                    'check': 'Future Dates',
                    'column': 'date',
                    'severity': 'CRITICAL',
                    'description': f"{future_dates} dates dans le futur",
                    'count': future_dates
                }
                self.issues.append(issue)
                self.severity_counts['CRITICAL'] += 1
                logger.error(f"     {future_dates} dates dans le futur détectées !")

        return self

    def check_outliers(self):
        """Détecte les valeurs aberrantes (IQR method)"""
        logger.info("  → Détection des valeurs aberrantes...")

        numeric_cols = ['quantity', 'unit_price', 'final_amount']

        for col in numeric_cols:
            if col in self.df.columns:
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1

                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR

                outliers = ((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).sum()

                if outliers > 0:
                    issue = {
                        'check': 'Outliers',
                        'column': col,
                        'severity': 'INFO',
                        'description': f"{outliers} valeurs aberrantes (IQR method)",
                        'count': outliers
                    }
                    self.issues.append(issue)
                    self.severity_counts['INFO'] += 1
                    logger.info(f"     {col}: {outliers} outliers détectés")

        return self

    def check_business_rules(self):
        """Vérifie les règles métier"""
        logger.info("  → Vérification des règles métier...")

        # Règle 1: total_amount doit être quantity * unit_price
        if all(col in self.df.columns for col in ['quantity', 'unit_price', 'total_amount']):
            expected = self.df['quantity'] * self.df['unit_price']
            discrepancy = np.abs(self.df['total_amount'] - expected) > 0.01
            discrepancy_count = discrepancy.sum()

            if discrepancy_count > 0:
                issue = {
                    'check': 'Business Rule',
                    'column': 'total_amount',
                    'severity': 'CRITICAL',
                    'description': f"{discrepancy_count} incohérences dans le calcul total_amount",
                    'count': discrepancy_count
                }
                self.issues.append(issue)
                self.severity_counts['CRITICAL'] += 1
                logger.error(f"     {discrepancy_count} incohérences de calcul !")

        # Règle 2: profit ne doit pas être négatif (ou très rarement)
        if 'profit' in self.df.columns:
            negative_profit = (self.df['profit'] < 0).sum()
            if negative_profit > len(self.df) * 0.05:  # > 5%
                issue = {
                    'check': 'Business Rule',
                    'column': 'profit',
                    'severity': 'WARNING',
                    'description': f"{negative_profit} transactions à perte ({negative_profit/len(self.df)*100:.1f}%)",
                    'count': negative_profit
                }
                self.issues.append(issue)
                self.severity_counts['WARNING'] += 1
                logger.warning(f"     {negative_profit} transactions à perte")

        return self

    def check_data_types(self):
        """Vérifie la cohérence des types de données"""
        logger.info("  → Vérification des types de données...")

        expected_types = {
            'quantity': 'int',
            'unit_price': 'float',
            'total_amount': 'float',
            'date': 'datetime'
        }

        for col, expected_type in expected_types.items():
            if col in self.df.columns:
                actual_type = str(self.df[col].dtype)

                if expected_type == 'int' and 'int' not in actual_type:
                    logger.warning(f"      {col}: attendu int, reçu {actual_type}")
                elif expected_type == 'float' and 'float' not in actual_type:
                    logger.warning(f"      {col}: attendu float, reçu {actual_type}")
                elif expected_type == 'datetime' and 'datetime' not in actual_type:
                    logger.warning(f"      {col}: attendu datetime, reçu {actual_type}")

        return self

    def _print_summary(self):
        """Affiche le résumé des contrôles"""
        logger.info("\n" + "="*60)
        logger.info(" RÉSUMÉ DES CONTRÔLES QUALITÉ")
        logger.info("="*60)
        logger.info(f" CRITICAL: {self.severity_counts['CRITICAL']}")
        logger.info(f"  WARNING:  {self.severity_counts['WARNING']}")
        logger.info(f"  INFO:     {self.severity_counts['INFO']}")
        logger.info(f" Total issues: {len(self.issues)}")
        logger.info("="*60 + "\n")

    def get_report(self):
        """Retourne un rapport sous forme de DataFrame"""
        if not self.issues:
            return pd.DataFrame({'message': [' Aucun problème détecté !']})

        return pd.DataFrame(self.issues)

    def save_report(self, filepath='outputs/quality_report.csv'):
        """Sauvegarde le rapport"""
        report = self.get_report()
        report.to_csv(filepath, index=False)
        logger.info(f" Rapport sauvegardé : {filepath}")

if __name__ == "__main__":
    # Test avec des données générées
    from data_generator import SalesDataGenerator

    generator = SalesDataGenerator()
    df = generator.generate_sales_data(1000)

    checker = DataQualityChecker(df)
    checker.check_all()

    report = checker.get_report()
    print("\n Rapport détaillé :")
    print(report)

    checker.save_report()


