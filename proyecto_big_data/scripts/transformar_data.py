import duckdb
import time

con = duckdb.connect('datos/analisis_master.db')

print("🚀 Iniciando transformación para Amazon UK (2.2M)...")
start_time = time.time()

sql_query = """
CREATE OR REPLACE TABLE fct_amazon_analytics AS
SELECT
    categoryName,
    COUNT(asin) as total_productos,
    ROUND(AVG(stars), 2) as rating_promedio,
    SUM(reviews) as total_resenas,
    ROUND(AVG(price), 2) as precio_promedio,
    -- Contamos cuántos son Best Sellers
    SUM(CASE WHEN isBestSeller = true THEN 1 ELSE 0 END) as cantidad_best_sellers,
    -- Sumamos el volumen estimado de ventas del último mes
    SUM(boughtInLastMonth) as volumen_ventas_mes,
    -- Calculamos el ingreso estimado (Precio * Ventas)
    ROUND(SUM(price * boughtInLastMonth), 2) as ingresos_estimados_mes
FROM amz_UK_raw
WHERE price > 0 
GROUP BY 1
ORDER BY volumen_ventas_mes DESC;
"""

try:
    # 1. Ejecutar transformación
    con.execute(sql_query)
    
    # 2. Exportar a Parquet para Power BI (Esto hace que Power BI vuele)
    con.execute("COPY fct_amazon_analytics TO 'datos/reporte_final.parquet' (FORMAT 'PARQUET')")
    
    end_time = time.time()
    print(f"✅ ¡Éxito! Procesadas las categorías en {end_time - start_time:.2f}s")
    print("📊 Tabla 'fct_amazon_analytics' creada en la DB.")
    print("📁 Archivo listo para Power BI: datos/reporte_final.parquet")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    con.close()