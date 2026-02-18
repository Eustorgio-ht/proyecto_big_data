import duckdb
import time

# Conectar (o crear) la base de datos local
con = duckdb.connect('datos/analisis_master.db')

print("Iniciando carga de datos...")
start_time = time.time()

# DuckDB puede leer múltiples archivos parquet a la vez usando comodines (*)
con.execute("""
    CREATE OR REPLACE TABLE amz_UK_raw AS 
    SELECT * FROM read_parquet('datos/*.parquet');
""")

end_time = time.time()
count = con.execute("SELECT COUNT(*) FROM amz_UK_raw").fetchone()[0]

print(f"¡Éxito! Se cargaron {count:,} filas en {end_time - start_time:.2f} segundos.")
con.close()