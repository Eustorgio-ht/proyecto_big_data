import duckdb
# 1. Recuperamos el diccionario de categorías (El traductor)
# 'df' es el DataFrame original que cargaste de DuckDB al principio del script
categorias_originales = df['categoryName'].astype('category').cat.categories
mapeo = dict(enumerate(categorias_originales))

# 2. Aplicamos el mapeo al DataFrame de resultados
df_final = X_test.copy()
df_final['real'] = y_test.values
df_final['prediccion'] = y_pred

# Esta es la línea mágica: crea una columna nueva con el texto original
df_final['categoryNameText'] = df_final['categoryName'].map(mapeo)

print("✅ Nombres de categoría recuperados.")

# 3. Guardar en DuckDB y luego a Parquet
con = duckdb.connect('datos/analisis_master.db')
con.register('df_temporal', df_final)
con.execute("CREATE OR REPLACE TABLE predicciones_modelo AS SELECT * FROM df_temporal")
con.execute("COPY predicciones_modelo TO 'datos/predicciones.parquet' (FORMAT 'PARQUET')")
con.close()
print("📁 Archivo 'datos/predicciones.parquet' creado para Power BI.")