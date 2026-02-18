import duckdb
import time
import os

# 1. Configuración de rutas
input_folder = 'datos'
output_file = 'datos/amz_UK.parquet'

# Conectar a DuckDB en memoria (temporal para la conversión)
con = duckdb.connect()

print("🚀 Iniciando conversión de CSV a Parquet...")
start_time = time.time()

# 2. El "truco" de DuckDB: Lee todos los CSVs de la carpeta y los guarda como Parquet
# 'read_csv_auto' detecta automáticamente tipos de datos, fechas y encabezados
try:
    con.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{input_folder}/*.csv')) 
        TO '{output_file}' (FORMAT 'PARQUET');
    """)
    
    end_time = time.time()
    
    # 3. Verificar resultados
    file_size = os.path.getsize(output_file) / (1024 * 1024) # Tamaño en MB
    filas = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]

    print(f"✅ ¡Conversión exitosa!")
    print(f"📊 Filas procesadas: {filas:,}")
    print(f"📁 Tamaño del archivo Parquet: {file_size:.2f} MB")
    print(f"⏱️ Tiempo total: {end_time - start_time:.2f} segundos")

except Exception as e:
    print(f"❌ Error al convertir: {e}")

finally:
    con.close()