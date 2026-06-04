import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]

DATA = PROJECT_ROOT / "data"
LANDING = DATA / "landing"
BRONZE = DATA / "bronze"
SILVER = DATA / "silver" / "transactions_items"
GOLD = DATA / "gold"
MODELS = DATA / "models"

LANDING_TRANSACTIONS = LANDING / "Transactions"
LANDING_PRODUCTS = LANDING / "Products"

GOLD_FACT_KPIS = GOLD / "fact_kpis"
GOLD_FACT_SALES_DAILY = GOLD / "fact_sales_daily"
GOLD_DIM_CUSTOMER_FEATURES = GOLD / "dim_customer_features"
GOLD_DIM_PRODUCT_FEATURES = GOLD / "dim_product_features"
GOLD_FACT_CATEGORY_METRICS = GOLD / "fact_category_metrics"
GOLD_CLUSTER_ASSIGNMENTS = GOLD / "cluster_assignments"
GOLD_CLUSTER_PROFILES = GOLD / "cluster_profiles"
GOLD_KMEANS_SEARCH = GOLD / "kmeans_search"
GOLD_PRODUCT_RULES = GOLD / "product_rules"
GOLD_CUSTOMER_RECOMMENDATIONS = GOLD / "customer_recommendations"
