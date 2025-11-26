"""
Générateue de données de ventes pour analyse
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class SalesDataGenerator:
    """Génère des données de ventes réalistes pour démo"""

    def __init__(self, seed=42):
        np.random.seed(seed)
        self.seed=seed

    def generate_sales_data(self, num_records=5000, start_date='2023-01-01'):
        """
        Génère un dataset de ventes

        Args:
            num_records (int): Nombre d'enregistrements
            start_date (str): Date de début

        Returns:
            pd.DataFrame: Dataset de ventes
        """
        print("Génération de {num_records} enregistrements ...")

        # Dates
        start = datetime.strptime(start_date, '%Y-%m-%d')
        dates = [start + timedelta(days=np.random.randint(0, 365)) for _ in range (num_records)]

        # Produits avec prix moyens réalistes
        products = {
            'Laptop Pro': 1200,
            'Laptop Standard': 800,
            'Smartphone Premium': 900,
            'Smartphone Basic': 400,
            'Tablet': 500,
            'Headphones': 150,
            'Mouse': 30,
            'Keyboard': 80,
            'Monitor': 300,
            'Webcam': 100
        }

        # Génération
        data = []
        for i in range(num_records):
            product = np.random.choice(list(products.keys()))
            base_price = products[product]
             # Prix avec variation +/- 10%
            unit_price = base_price * np.random.uniform(0.9, 1.1)

            # Quantité (produits chers = moins de quantité)
            if base_price > 500:
                quantity = np.random.randint(1, 5)
            else:
                quantity = np.random.randint(1, 20)

            record = {
                'order_id': f'ORD-{i+1:05d}',
                'date': dates[i],
                'product': product,
                'category': self._get_category(product),
                'quantity': quantity,
                'unit_price': round(unit_price, 2),
                'region': np.random.choice(['North', 'South', 'East', 'West'], p=[0.3, 0.25, 0.25, 0.2]),
                'customer_type': np.random.choice(['B2B', 'B2C'], p=[0.4, 0.6]),
                'salesperson': f'Agent_{np.random.randint(1, 21):02d}'
            }

            data.append(record)

        df = pd.DataFrame(data)

         # Calculs dérivés
        df['total_amount'] = df['quantity'] * df['unit_price']
        df['discount_rate'] = np.random.choice([0, 0.05, 0.10, 0.15], num_records, p=[0.5, 0.3, 0.15, 0.05])
        df['discount_amount'] = df['total_amount'] * df['discount_rate']
        df['final_amount'] = df['total_amount'] - df['discount_amount']

        # Coûts (marge entre 30% et 50%)
        df['unit_cost'] = df['unit_price'] * np.random.uniform(0.5, 0.7, num_records)
        df['total_cost'] = df['unit_cost'] * df['quantity']
        df['profit'] = df['final_amount'] - df['total_cost']
        df['profit_margin'] = (df['profit'] / df['final_amount']) * 100

        # Dates dérivées
        df['year'] = df['date'].dt.year
        df['quarter'] = df['date'].dt.quarter
        df['month'] = df['date'].dt.month
        df['month_name'] = df['date'].dt.month_name()
        df['day_of_week'] = df['date'].dt.day_name()
        df['week_number'] = df['date'].dt.isocalendar().week

        # Injection d'anomalies (pour tests qualité)
        num_anomalies = int(num_records * 0.02)  # 2% d'anomalies
        anomaly_indices = np.random.choice(df.index, num_anomalies, replace=False)

        # Anomalie 1: Prix négatifs ou nuls
        df.loc[anomaly_indices[:len(anomaly_indices)//3], 'unit_price'] = 0

        # Anomalie 2: Quantités aberrantes
        df.loc[anomaly_indices[len(anomaly_indices)//3:2*len(anomaly_indices)//3], 'quantity'] = 1000

        # Anomalie 3: Dates futures
        df.loc[anomaly_indices[2*len(anomaly_indices)//3:], 'date'] = datetime.now() + timedelta(days=365)

        print(f"Génération terminée : {len(df)} lignes, {len(df.columns)} colonnes")
        print(f"{num_anomalies} anomalies injectées pour tests")

        return df

    def _get_category(self, product):
        """Détermine la catégorie du produit"""
        if 'Laptop' in product:
            return 'Computers'
        elif 'Smartphone' in product or 'Tablet' in product:
            return 'Mobile Devices'
        elif 'Monitor' in product or 'Webcam' in product:
            return 'Peripherals'
        else:
            return 'Accessories'

    def save_data(self, df, output_path='data/input/sales_data.csv'):
        """Sauvegarde les données"""
        df.to_csv(output_path, index=False)
        print(f"Données sauvegardées : {output_path}")

if __name__ == "__main__":
    # Test du générateur
    generator = SalesDataGenerator()
    df = generator.generate_sales_data(5000)
    generator.save_data(df)

    # Affichage d'un échantillon
    print("\n Aperçu des données :")
    print(df.head())
    print("\n Statistiques :")
    print(df[['quantity', 'unit_price', 'final_amount', 'profit']].describe())


