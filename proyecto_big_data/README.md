# 📊 Amazon UK Marketplace Analytics (2.2M Products)

Este proyecto implementa un pipeline de datos de alto rendimiento para procesar y analizar el dataset de Amazon UK 2023. La solución se enfoca en la eficiencia local, procesando millones de registros en segundos sin necesidad de infraestructura en la nube costosa.

## 🚀 Highlights del Proyecto
* **Volumen de Datos:** +2,200,000 registros analizados.
* **Velocidad:** Ingesta y transformación completa en < 5 segundos.
* **Stack Moderno:** DuckDB + Python 3.14 + Apache Parquet.

## 🛠️ Arquitectura Técnica
1. **Ingesta de Datos:** Scripts de Python para la carga masiva de archivos fuente a un motor analítico vectorial (DuckDB).
2. **Optimización de Almacenamiento:** Uso de formato columnar **Parquet** para reducir el tamaño en disco y acelerar las consultas en Power BI.
3. **Capa de Transformación (ELT):** Implementación de lógica de negocio mediante SQL avanzado para calcular KPIs como ingresos estimados por categoría y volumen de ventas.
4. **Visualización:** Dashboard interactivo en Power BI centrado en el análisis de competitividad y nichos de mercado.

## 🧠 Decisiones de Ingeniería y Resolución de Problemas
Durante el desarrollo, se enfrentó un desafío de compatibilidad entre `dbt-core` y la versión bleeding-edge de **Python 3.14** (errores de serialización en la librería `mashumaro`). 

**Solución:** Se optó por una arquitectura de transformación nativa en DuckDB-Python. Esta decisión permitió:
* Mantener la integridad de la lógica SQL de transformación.
* Eliminar dependencias externas innecesarias.
* Reducir la latencia del pipeline significativamente.

## 📈 KPIs Analizados
* **Ingresos Estimados:** (Precio * Volumen de ventas mensual).
* **Saturación de Categorías:** Relación entre número de productos y Best Sellers.
* **Rating vs. Popularidad:** Análisis de calidad percibida basada en millones de reseñas.

## 📂 Estructura del Repositorio
* `/scripts`: Lógica de ingesta y transformación en Python.
* `/dbt_proyecto`: Modelos SQL y documentación de la estructura de datos.
* `README.md`: Documentación del proyecto.

---
**Nota:** Los archivos de datos (.db, .csv, .parquet) están excluidos con `.gitignore` para evitar peso excesivo.