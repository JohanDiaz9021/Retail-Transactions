## Estructura

Retail-Transactions/
│
├── data/                              # Datos del pipeline
│   ├── landing/                       # Datos fuente crudos
│   │   ├── Transactions/              #   102_Tran.csv, 103_Tran.csv ...
│   │   └── Products/                  #   Categories.csv, ProductCategory.csv
│   ├── bronze/                        # Datos limpios (generado en runtime)
│   ├── silver/transactions_items/     # Transformaciones (generado en runtime)
│   ├── gold/                          # Métricas de negocio (generado en runtime)
│   │   ├── fact_kpis/
│   │   ├── fact_sales_daily/
│   │   ├── dim_customer_features/
│   │   ├── dim_product_features/
│   │   ├── fact_category_metrics/
│   │   ├── cluster_assignments/
│   │   ├── cluster_profiles/
│   │   ├── kmeans_search/
│   │   ├── product_rules/
│   │   └── customer_recommendations/
│   └── models/                        # Modelos entrenados (generado en runtime)
│
├── src/pipeline/                      # Código del pipeline ETL
│   ├── __init__.py
│   ├── bronze.py                      # Lectura y limpieza inicial
│   ├── silver.py                      # Transformaciones y joins
│   ├── gold.py                        # Agregaciones y KPIs
│   ├── models.py                      # K-Means, FP-Growth, ALS
│   ├── ingest.py                      # Ingesta incremental
│   ├── run.py                         # Orquestación del pipeline
│   ├── spark_session.py               # Configuración de Spark
│   └── paths.py                       # Rutas del proyecto
│
├── app/                               # Dashboard Streamlit
│   └── streamlit_app.py
│
├── .git/
├── Makefile
└── README.md
