import os
import math
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# Configurare Cassandra
ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_BUNDLE = "data/secure-connect-dw-cassandra.zip"

cloud_config = {'secure_connect_bundle': ASTRA_DB_BUNDLE}
auth_provider = PlainTextAuthProvider("token", ASTRA_DB_TOKEN)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace(ASTRA_DB_KEYSPACE)

print("Connected to Cassandra for model training")

def get_historical_data(asset_id='IBM', data_source_id='ALPHAVANTAGE'):
    """Obține datele istorice pentru un asset"""
    # Obținem ultimii 3 ani pentru a acoperi partitionarea
    current_year = datetime.now().year
    years = [current_year, current_year - 1, current_year - 2]
    
    historical_data = []
    
    for year in years:
        query = """
        SELECT business_date, data_values
        FROM time_series_data
        WHERE asset_id = %s 
          AND data_source_id = %s 
          AND business_date_year = %s
        """
        rows = session.execute(query, (asset_id, data_source_id, year))
        historical_data.extend(list(rows))
    
    # Sortăm descrescător după dată și luăm ultimele 100 de înregistrări
    historical_data.sort(key=lambda x: x.business_date, reverse=True)
    return historical_data[:100]

def calculate_moving_average(data, window_size=5):
    """Calculează media mobilă simplă"""
    ma = []
    for i in range(len(data)):
        start = max(0, i - window_size + 1)
        window = data[start:i+1]
        avg = sum(window) / len(window)
        ma.append(avg)
    return ma

def predict_future_prices(prices, num_predictions=7):
    """Prezice prețurile viitoare folosind o medie mobilă ponderată"""
    # Calculăm ponderile - mai recentele au pondere mai mare
    weights = [0.5**i for i in range(5, 0, -1)]
    total_weight = sum(weights)
    
    predictions = []
    for _ in range(num_predictions):
        # Folosim ultimele 5 valori pentru predicție
        last_values = prices[-5:] if len(prices) >= 5 else prices
        last_values = last_values[-len(weights):]  # Ajustăm la numărul de ponderi
        
        # Calculăm media ponderată
        weighted_sum = sum(value * weight for value, weight in zip(last_values, weights))
        prediction = weighted_sum / total_weight
        
        predictions.append(prediction)
        prices.append(prediction)  # Adăugăm predicția pentru următoarea iterație
    
    return predictions

def save_predictions(asset_id, predictions):
    """Salvează predicțiile în Cassandra"""
    query = """
    INSERT INTO predictions (
        asset_id, 
        prediction_date, 
        prediction_time,
        predicted_close,
        model_name
    ) VALUES (%s, %s, %s, %s, %s)
    """
    
    today = datetime.now().date()
    for days_ahead, prediction in enumerate(predictions, start=1):
        prediction_date = today + timedelta(days=days_ahead)
        session.execute(query, (
            asset_id,
            prediction_date,
            datetime.now(),
            prediction,
            "WeightedMovingAverage"
        ))

if __name__ == "__main__":
    try:
        # 1. Obține date istorice
        historical_data = get_historical_data()
        
        if not historical_data:
            raise Exception("Nu s-au găsit date istorice")
        
        print(f"S-au găsit {len(historical_data)} înregistrări istorice")
        
        # Extrage prețurile de închidere
        closing_prices = [float(row.data_values['close']) for row in historical_data]
        
        # 2. Calculează media mobilă
        moving_avg = calculate_moving_average(closing_prices)
        
        # 3. Generează predicții pentru următoarele 7 zile
        predictions = predict_future_prices(closing_prices.copy())
        
        # 4. Salvează predicțiile
        save_predictions('IBM', predictions)
        print("Predicțiile au fost salvate în Cassandra")
        
    except Exception as e:
        print(f"Eroare: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        cluster.shutdown()