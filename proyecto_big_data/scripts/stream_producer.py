import duckdb
import json
import time
import os

def stream_data():
    parquet_path = 'datos/amz_UK.parquet'
    json_path = 'datos/current_event.json'
    
    print(f"🔍 Conectando al dataset en {parquet_path}...")
    
    # Usamos DuckDB para una conexión rápida al archivo
    con = duckdb.connect()

    print("🚀 Simulación de Stream iniciada. (Presiona Ctrl+C para detener)")
    
    try:
        while True:
            # La magia: DuckDB elige 1 fila aleatoria directamente del Parquet en milisegundos
            query = f"SELECT * FROM '{parquet_path}' USING SAMPLE 1 ROWS"
            result = con.execute(query).df().to_dict(orient='records')[0]
            
            # Guardamos el evento
            with open(json_path, 'w') as f:
                json.dump(result, f)
            
            # Imprimimos en consola para saber que está vivo
            print(f"📡 Evento enviado: {result.get('title', 'Sin título')[:50]}... | ⭐ {result.get('stars')}")
            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n🛑 Simulación detenida por el usuario.")
    finally:
        con.close()

if __name__ == "__main__":
    stream_data()