# Resumen del proyecto

## Objetivo
Este proyecto implementa una solución completa para el procesamiento y análisis de transacciones de supermercado. Su propósito es ingerir archivos CSV de ventas y catálogos, procesarlos con Apache Spark, organizar los datos bajo una arquitectura medallion y exponer los resultados en un dashboard interactivo construido con Streamlit.

## Descripción general
La solución está pensada para trabajar con un conjunto de datos de transacciones por tienda, donde cada fila representa una canasta de compra. El sistema transforma esos archivos en tablas analíticas listas para consulta, además de entrenar modelos de segmentación y recomendación para análisis avanzado.

El flujo principal del proyecto es:

1. Ingesta de archivos en `data/landing/`.
2. Procesamiento Bronze, Silver y Gold.
3. Entrenamiento de modelos analíticos.
4. Consulta y visualización desde Streamlit.

## Estructura del proyecto
El repositorio está organizado en capas claramente separadas:

- `data/landing/`: archivos fuente de transacciones y productos.
- `data/bronze/`: copia cruda en Parquet de los datos ingeridos.
- `data/silver/`: datos limpios y transformados.
- `data/gold/`: tablas analíticas listas para consumo.
- `data/models/`: modelos persistidos.
- `src/pipeline/`: lógica del pipeline de datos y modelos.
- `app/streamlit_app.py`: dashboard principal.
- `docs/`: documentación técnica y de arquitectura.

## Tecnologías usadas
El proyecto utiliza principalmente:

- PySpark 3.5 para el procesamiento distribuido y los modelos.
- Parquet como formato de almacenamiento analítico.
- DuckDB para leer y consultar los resultados desde la app.
- Streamlit para la visualización.
- Pandas y Plotly para manipulación y gráficos.

## Pipeline de datos
El pipeline está orquestado desde `src/pipeline/run.py` y ejecuta cuatro etapas:

- Bronze: carga los archivos originales y los guarda en formato Parquet.
- Silver: limpia y transforma los datos, incluyendo la expansión de productos por transacción.
- Gold: genera tablas analíticas agregadas para métricas, features y análisis.
- Models: entrena K-Means, FP-Growth y ALS para segmentación y recomendaciones.

Las rutas principales del almacenamiento se definen en `src/pipeline/paths.py`, donde se crean automáticamente las carpetas necesarias para `landing`, `bronze`, `silver` y `gold`.

## Modelos analíticos
El proyecto incluye tres componentes de análisis avanzado:

- K-Means para segmentación de clientes según sus características de compra.
- FP-Growth para encontrar reglas de asociación entre productos.
- ALS para generar recomendaciones personalizadas de productos por cliente.

Estos modelos se almacenan en disco para reutilización y para alimentar las vistas analíticas del dashboard.

## Dashboard
La aplicación de Streamlit en `app/streamlit_app.py` consume las tablas Gold y Silver para mostrar:

- Resumen ejecutivo con KPIs principales.
- Visualizaciones analíticas de ventas y comportamiento.
- Segmentación de clientes.
- Recomendaciones de productos.
- Funcionalidades para incorporar nuevos archivos y relanzar procesos.

La interfaz está diseñada como una app multipágina con estilo oscuro personalizado y visualizaciones interactivas.

## Alcance funcional
El sistema permite:

- Calcular ventas totales, transacciones y clientes únicos.
- Identificar productos, clientes y categorías más relevantes.
- Visualizar tendencias de compra por fecha.
- Analizar correlaciones entre variables de cliente.
- Segmentar clientes con clustering.
- Recomendar productos relacionados y sugeridos.
- Reprocesar datos cuando llegan nuevos archivos.

## Cómo se ejecuta
Según el README, el flujo normal es:

1. Instalar dependencias.
2. Ejecutar el pipeline completo.
3. Levantar la app de Streamlit.

## Conclusión
Este proyecto no es solo un notebook de análisis, sino una solución modular y reproducible de extremo a extremo. Combina procesamiento distribuido, modelado analítico y visualización interactiva para convertir datos brutos de supermercado en información útil para análisis de negocio y toma de decisiones.