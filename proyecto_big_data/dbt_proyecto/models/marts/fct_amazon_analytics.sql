{{ config(materialized='table') }}

WITH base_data AS (
    SELECT 
        asin,
        title,
        categoryName,
        stars,
        reviews,
        price,
        listPrice,
        isBestSeller,
        boughtInLastMonth,
        -- Calculamos el descuento real
        (listPrice - price) as discount_value,
        -- Calculamos el porcentaje de descuento
        CASE 
            WHEN listPrice > 0 THEN ROUND(((listPrice - price) / listPrice) * 100, 2)
            ELSE 0 
        END as discount_percentage
    FROM {{ source('main', 'amz_data') }}
    WHERE price > 0 -- Limpieza: solo productos con precio válido
)

SELECT
    categoryName,
    COUNT(asin) as total_productos,
    ROUND(AVG(stars), 2) as rating_promedio,
    SUM(reviews) as total_resenas,
    ROUND(AVG(price), 2) as precio_promedio,
    -- Cantidad de Best Sellers por categoría
    SUM(CASE WHEN isBestSeller = true THEN 1 ELSE 0 END) as cantidad_best_sellers,
    -- Estimación de ventas (usando la columna boughtInLastMonth)
    SUM(boughtInLastMonth) as volumen_ventas_mes,
    -- Promedio de descuento por categoría
    ROUND(AVG(discount_percentage), 2) as descuento_promedio_pct
FROM base_data
GROUP BY 1
ORDER BY volumen_ventas_mes DESC