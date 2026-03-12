import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import confusion_matrix, classification_report

# 1. Datos frescos de DuckDB
con = duckdb.connect('datos/analisis_master.db')
df = con.execute("""
    SELECT 
        stars, 
        reviews, 
        price, 
        categoryName, 
        isBestSeller 
    FROM amz_UK_raw 
    WHERE price > 0
""").df()
con.close()

nombres_categorias = df['categoryName'].astype('category').cat.categories.tolist()
mapeo_nombres = dict(enumerate(nombres_categorias))

# 2. Transformamos categorías de texto a números
df['categoryName'] = df['categoryName'].astype('category').cat.codes

# 3. Definimos variable X y Y
X = df[['stars', 'reviews', 'price', 'categoryName']]
y = df['isBestSeller'].astype(int)

# 4. Dividimos en Entrenamiento y Prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"✅ Datos listos. Entrenando con {len(X_train)} filas...")

# 5. Calculamos el peso para balancear (Negativos / Positivos)
peso_balanceo = 443421 / 1124

# 6. Corremos el modelo con 'scale_pos_weight'
model = XGBClassifier(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    scale_pos_weight=peso_balanceo,
    tree_method='hist',
    device='cpu'
)

model.fit(X_train, y_train)

# 7.Evaluamos la precisión
accuracy = model.score(X_test, y_test)
print(f"🎯 Precisión del modelo: {accuracy * 100:.2f}%")

y_pred = model.predict(X_test)

print("📊 Reporte de Clasificación:")
print(classification_report(y_test, y_pred))

print("🧮 Matriz de Confusión:")
print(confusion_matrix(y_test, y_pred))

import matplotlib.pyplot as plt

# 8.Graficar la importancia de las variables
importances = pd.Series(model.feature_importances_, index=X.columns)
importances.nlargest(10).plot(kind='barh')
plt.title('¿Qué define a un Best Seller en Amazon UK?')
plt.show()

print("💾 Guardando resultados de forma optimizada...")

# 9. Aplicamos el mapeo al DataFrame de resultados
df_final = X_test.copy()
df_final['real'] = y_test.values
df_final['prediccion'] = y_pred

df_final['categoryNameText'] = df_final['categoryName'].map(mapeo_nombres)

print("✅ Nombres de categoría recuperados.")

# 10. Guardar en DuckDB y luego a Parquet
con = duckdb.connect('datos/analisis_master.db')
con.register('df_temporal', df_final)
con.execute("CREATE OR REPLACE TABLE predicciones_modelo AS SELECT * FROM df_temporal")
con.execute("COPY predicciones_modelo TO 'datos/predicciones.parquet' (FORMAT 'PARQUET')")
con.close()
print("💾 Predicciones guardadas en DuckDB para Power BI.")