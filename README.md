
## Estructura

proyecto/
├── data/
│   ├── landing/{Transactions,Products}/  
│   ├── bronze/                           
│   ├── silver/transactions_items/        
│   ├── gold/                            
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
│   └── models/                           
├── src/pipeline/
│   ├── bronze.py · silver.py · gold.py
│   ├── models.py                         
│   ├── ingest.py                         
│   ├── run.py                            
│   └── spark_session.py · paths.py
├── app/streamlit_app.py                  
├── docs/
│   ├── arquitectura.md
│   ├── resumen_ejecutivo.md
│   └── informe_tecnico.md               
├── requirements.txt
└── Makefile