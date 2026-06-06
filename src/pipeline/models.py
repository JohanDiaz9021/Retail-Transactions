"""Models: segmentación K-Means + recomendador (FP-Growth y ALS).

Produce tres data marts adicionales en Gold:

    gold/cluster_assignments       (customer_id, cluster_id)
    gold/cluster_profiles          (cluster_id, n_customers, métricas medias)
    gold/product_rules             (antecedent, consequent, support, confidence, lift)
    gold/customer_recommendations  (customer_id, product_id, score, rank)

Además persiste los modelos entrenados en `data/models/` (pyspark.ml savers)
para que el módulo de ingesta los pueda recargar al regenerar resultados.
"""
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, functions as F

from .paths import DATA, GOLD, SILVER
from .spark_session import get_spark


MODELS_DIR = DATA / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

CLUSTER_FEATURES = [
    "frequency",
    "units_total",
    "distinct_products",
    "distinct_categories",
    "avg_basket_size",
    "recency_days",
]

# Valores por defecto: configurables vía args si llegase a hacer falta.
KMEANS_K_RANGE = (3, 4, 5, 6)
# FP-Growth opera sobre ~1.1M canastas. Un min_support muy bajo dispara una explosión
# combinatoria del árbol de candidatos. Con 5% (≈55k canastas) seguimos obteniendo
# reglas accionables y mantenemos el costo bajo control en un nodo único.
FP_MIN_SUPPORT = 0.05
FP_MIN_CONFIDENCE = 0.30
FP_MAX_BASKET_SIZE = 30        # evita canastas patológicas que inflan FP-tree
FP_TOP_N_PRODUCTS = 200        # nos quedamos con los 200 productos más vendidos
ALS_RANK = 16
ALS_MAX_ITER = 10
ALS_REG_PARAM = 0.05
ALS_TOP_N = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _overwrite_dir(path: Path) -> None:
    while path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _silhouette(model: KMeansModel, df: DataFrame, features_col: str = "features",
                prediction_col: str = "cluster_id") -> float:
    predictions = model.transform(df)
    evaluator = ClusteringEvaluator(
        predictionCol=prediction_col,
        featuresCol=features_col,
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )
    return float(evaluator.evaluate(predictions))


# ---------------------------------------------------------------------------
# Segmentación de clientes con K-Means
# ---------------------------------------------------------------------------
def run_kmeans(spark) -> None:
    print("[models] segmentación K-Means → leyendo dim_customer_features ...")
    customers_df = spark.read.parquet(str(GOLD / "dim_customer_features"))

    feature_assembler = VectorAssembler(inputCols=CLUSTER_FEATURES, outputCol="features_raw")
    feature_scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                                    withMean=True, withStd=True)
    prep_pipeline = Pipeline(stages=[feature_assembler, feature_scaler]).fit(customers_df)
    scaled_features = prep_pipeline.transform(customers_df).select("customer_id", "features", *CLUSTER_FEATURES).cache()

    # Selección de k por silhouette sobre una muestra (acelera la evaluación).
    sample_df = scaled_features.sample(False, 0.10, seed=42).cache()
    best_result = None
    silhouette_scores = []
    k_values = list(KMEANS_K_RANGE)
    idx = 0
    while idx < len(k_values):
        k = k_values[idx]
        km = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster_id",
                    maxIter=30, tol=1e-4)
        model = km.fit(sample_df)
        sil = _silhouette(model, sample_df)
        silhouette_scores.append((k, sil))
        print(f"[models]   k={k}  silhouette={sil:.4f}")
        while best_result is None or sil > best_result[1]:
            best_result = (k, sil, model)
            break
        idx += 1
    sample_df.unpersist()

    k_best, sil_best, _ = best_result
    print(f"[models] k seleccionado = {k_best} (silhouette={sil_best:.4f})")

    # Reentrenamos sobre el dataset completo con el k ganador para obtener asignaciones definitivas.
    final_model = KMeans(k=k_best, seed=42, featuresCol="features", predictionCol="cluster_id",
                         maxIter=50, tol=1e-4).fit(scaled_features)
    assignments = final_model.transform(scaled_features).select("customer_id", "cluster_id",
                                                                *CLUSTER_FEATURES)

    # Re-ordenamos cluster_id por tamaño descendente para que el "cluster 0" sea siempre el mayoritario.
    cluster_sizes = (assignments.groupBy("cluster_id").count()
                     .orderBy(F.col("count").desc()).collect())
    id_mapping = {row["cluster_id"]: i for i, row in enumerate(cluster_sizes)}
    remap_expr = F.create_map([F.lit(x) for kv in id_mapping.items() for x in kv])
    assignments = assignments.withColumn("cluster_id", remap_expr[F.col("cluster_id")])

    _overwrite_dir(GOLD / "cluster_assignments")
    (assignments.select("customer_id", "cluster_id")
        .coalesce(4).write.mode("overwrite").parquet(str(GOLD / "cluster_assignments")))

    # Perfil de cada cluster (medias).
    cluster_profiles_df = (
        assignments.groupBy("cluster_id")
        .agg(
            F.count("*").alias("n_customers"),
            *[F.avg(F.col(c)).alias(f"avg_{c}") for c in CLUSTER_FEATURES],
            *[F.expr(f"percentile_approx({c}, 0.5)").alias(f"median_{c}")
              for c in CLUSTER_FEATURES],
        )
        .orderBy("cluster_id")
    )

    _overwrite_dir(GOLD / "cluster_profiles")
    (cluster_profiles_df.coalesce(1).write.mode("overwrite").parquet(str(GOLD / "cluster_profiles")))

    # Histórico de la búsqueda de k (silhouette por k) para mostrarlo en el dashboard.
    # Usamos pandas directamente para evitar el crash del worker Python de PySpark en Windows.
    import pandas as pd
    sil_pd = pd.DataFrame(
        [(int(k), float(s)) for k, s in silhouette_scores] + [(-1, float(sil_best))],
        columns=["k", "silhouette"],
    )
    _overwrite_dir(GOLD / "kmeans_search")
    sil_pd.to_parquet(str(GOLD / "kmeans_search" / "silhouette.parquet"))

    # Persistimos el modelo escalado para reuso al ingerir datos nuevos.
    model_path = MODELS_DIR / "kmeans_pipeline"
    if model_path.exists():
        shutil.rmtree(model_path)
    prep_pipeline.write().overwrite().save(str(MODELS_DIR / "kmeans_preprocessor"))
    final_model.write().overwrite().save(str(model_path))

    scaled_features.unpersist()
    print(f"[models] cluster_assignments y cluster_profiles escritos (k={k_best})")


# ---------------------------------------------------------------------------
# Recomendador por canasta (FP-Growth)
# ---------------------------------------------------------------------------
def run_fpgrowth(spark) -> None:
    print("[models] FP-Growth → construyendo canastas ...")
    items_df = spark.read.parquet(str(SILVER / "transactions_items"))

    # Limitamos al top-N de productos por volumen: esto poda fuertemente el espacio
    # de búsqueda sin perder señal (los productos en la "long tail" no formarían
    # reglas con soporte ≥ FP_MIN_SUPPORT de todas formas).
    top_products = (
        items_df.groupBy("product_id")
        .agg(F.sum("qty").alias("units"))
        .orderBy(F.col("units").desc())
        .limit(FP_TOP_N_PRODUCTS)
        .select("product_id")
    )

    filtered_items = items_df.join(F.broadcast(top_products), "product_id", "inner")

    basket_df = (
        filtered_items.groupBy("transaction_id")
        .agg(F.collect_set("product_id").alias("items"))
        .filter((F.size("items") >= 2) & (F.size("items") <= FP_MAX_BASKET_SIZE))
    )

    print(f"[models] FP-Growth: top-{FP_TOP_N_PRODUCTS} productos, "
          f"min_support={FP_MIN_SUPPORT}, min_confidence={FP_MIN_CONFIDENCE}")

    fp = FPGrowth(itemsCol="items",
                  minSupport=FP_MIN_SUPPORT,
                  minConfidence=FP_MIN_CONFIDENCE)
    model = fp.fit(basket_df)

    assoc_rules = model.associationRules
    # FP-Growth devuelve antecedent/consequent como array<int>; los pasamos a
    # filas (product_id antecedente, product_id consecuente) para que sea trivial
    # consultarlas desde el dashboard.
    flat_rules = (
        assoc_rules
        .withColumn("antecedent_product_id", F.explode("antecedent"))
        .withColumn("consequent_product_id", F.explode("consequent"))
        .select("antecedent_product_id", "consequent_product_id",
                F.col("confidence").cast("double"),
                F.col("lift").cast("double"))
    )

    # Enriquecemos con métricas de producto para mostrarlas bonito.
    product_features_df = spark.read.parquet(str(GOLD / "dim_product_features"))
    enriched = (
        flat_rules
        .join(product_features_df.select(F.col("product_id").alias("consequent_product_id"),
                                          F.col("category_name").alias("consequent_category")),
              "consequent_product_id", "left")
        .join(product_features_df.select(F.col("product_id").alias("antecedent_product_id"),
                                          F.col("category_name").alias("antecedent_category")),
              "antecedent_product_id", "left")
        .select(
            "antecedent_product_id", "antecedent_category",
            "consequent_product_id", "consequent_category",
            "confidence", "lift",
        )
    )

    _overwrite_dir(GOLD / "product_rules")
    (enriched.coalesce(2).write.mode("overwrite").parquet(str(GOLD / "product_rules")))

    n_rules = enriched.count()
    print(f"[models] FP-Growth: {n_rules:,} reglas (min_sup={FP_MIN_SUPPORT}, "
          f"min_conf={FP_MIN_CONFIDENCE})")

    model_path = MODELS_DIR / "fpgrowth"
    if model_path.exists():
        shutil.rmtree(model_path)
    model.write().overwrite().save(str(model_path))


# ---------------------------------------------------------------------------
# Recomendador cliente→producto (ALS implicit)
# ---------------------------------------------------------------------------
def run_als(spark) -> None:
    print("[models] ALS implicit → matriz cliente×producto ...")
    items_df = spark.read.parquet(str(SILVER / "transactions_items"))

    interaction_matrix = (
        items_df.groupBy("customer_id", "product_id")
        .agg(F.sum("qty").cast("double").alias("rating"))
    )

    als = ALS(
        userCol="customer_id",
        itemCol="product_id",
        ratingCol="rating",
        rank=ALS_RANK,
        maxIter=ALS_MAX_ITER,
        regParam=ALS_REG_PARAM,
        implicitPrefs=True,
        coldStartStrategy="drop",
        seed=42,
    )
    model = als.fit(interaction_matrix)

    # top-N recomendaciones por cliente.
    top_per_user = model.recommendForAllUsers(ALS_TOP_N)
    exploded = (
        top_per_user
        .withColumn("rec", F.explode("recommendations"))
        .select(
            F.col("customer_id"),
            F.col("rec.product_id").alias("product_id"),
            F.col("rec.rating").alias("score"),
        )
    )
    # Rango por cliente (1..N).
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("score").desc())
    ranked = exploded.withColumn("rank", F.row_number().over(window_spec))

    _overwrite_dir(GOLD / "customer_recommendations")
    (ranked.coalesce(4).write.mode("overwrite").parquet(str(GOLD / "customer_recommendations")))

    print(f"[models] ALS: top-{ALS_TOP_N} recomendaciones por cliente persistidas")

    model_path = MODELS_DIR / "als"
    if model_path.exists():
        shutil.rmtree(model_path)
    model.write().overwrite().save(str(model_path))


def run() -> None:
    spark = get_spark("models")
    try:
        run_kmeans(spark)
        run_fpgrowth(spark)
        run_als(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
