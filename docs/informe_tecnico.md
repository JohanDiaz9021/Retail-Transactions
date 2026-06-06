# Informe Técnico — Análisis y Modelado Analítico de Transacciones de Supermercado

**Curso:** Procesamiento Distribuido de Datos · G1

**Entrega 3:** 05 de junio de 2026
**Stack:** PySpark 3.5 (ETL + MLlib) → Parquet medallion → DuckDB → Streamlit

---

## 1. Descripción de la arquitectura

El sistema implementa una arquitectura **medallion (Bronze → Silver → Gold)** sobre PySpark 3.5 en modo local (`local[*]` con 8 GB de driver memory, 8 shuffle partitions, huso horario UTC). El orquestador CLI (`src/pipeline/run.py`) acepta `--step {bronze,silver,gold,models,all}` y ejecuta las etapas secuencialmente.

```
data/landing/Transactions/*.csv      data/landing/Products/*.csv
               │                                  │
               ▼                                  ▼
     ┌─────────────────────────────────────────────────────┐
     │  Bronze (PySpark, particionado por store_id)        │
     │   bronze/transactions    bronze/categories          │
     │   bronze/product_category                           │
     └─────────────────────────────────────────────────────┘
               │
               ▼  explode(product_list) + join catálogo
     ┌─────────────────────────────────────────────────────┐
     │  Silver: transactions_items                          │
     │   (transaction_id, date, store_id, customer_id,      │
     │    product_id, category_id, category_name, qty)      │
     │   10.591.792 filas, particionado por store_id        │
     └─────────────────────────────────────────────────────┘
               │
               ▼  agregaciones por dimensión
     ┌─────────────────────────────────────────────────────┐
     │  Gold (data marts analíticos)                        │
     │   fact_kpis · fact_sales_daily                       │
     │   dim_customer_features · dim_product_features       │
     │   fact_category_metrics                              │
     │   cluster_assignments · cluster_profiles             │
     │   kmeans_search · product_rules                      │
     │   customer_recommendations                           │
     └─────────────────────────────────────────────────────┘
               │
               ▼
     Streamlit + DuckDB (lectura sin levantar Spark)
```

### 1.1 SparkSession

`src/pipeline/spark_session.py` crea una `SparkSession` con:
- `master("local[*]")` — utiliza todos los núcleos disponibles
- `spark.driver.memory = "8g"` — suficiente para el dataset de 6 meses
- `spark.sql.shuffle.partitions = 8` — reduce la sobrecarga de shuffles pequeños
- `spark.sql.session.timeZone = "UTC"` — consistencia en conversiones de fecha

### 1.2 Gestión de rutas

`src/pipeline/paths.py` centraliza todas las rutas del proyecto usando `pathlib`:

| Variable | Ruta |
|---|---|
| `ROOT` | Raíz del repositorio |
| `LANDING_TX` | `data/landing/Transactions/` |
| `LANDING_PRODUCTS` | `data/landing/Products/` |
| `BRONZE` | `data/bronze/` |
| `SILVER` | `data/silver/` |
| `GOLD` | `data/gold/` |

---

## 2. Capa Bronze — Ingesta cruda

`src/pipeline/bronze.py` lee los archivos CSV con `spark.read.csv` y los persiste como Parquet sin transformaciones de negocio.

**Transacciones:** los 4 archivos `{102,103,107,110}_Tran.csv` se leen con separador `|`, sin cabecera, con esquema declarado (`date_raw, store_id, customer_id, product_list_raw`). Se extrae el `store_id` del nombre del archivo mediante `regexp_extract(r"(\d+)_Tran\.csv")` y se aplica `coalesce(store_id, store_from_file)` para manejar filas donde el campo `store_id` venga nulo. Se persiste particionado por `store_id` en `data/bronze/transactions/`.

**Catálogos:** `Categories.csv` (50 registros) y `ProductCategory.csv` (112.010 registros) se leen con el mismo formato de separador `|`. `ProductCategory.csv` sí tiene cabecera, por lo que se usa `option("header", "true")` y se renombran las columnas de `v.Code_pr` y `v.code` a `product_id` y `category_id`.

Las tres tablas se escriben con `mode("overwrite")` para garantizar **idempotencia**.

**Volúmenes Bronze:**
- `transactions`: **1.108.987** filas (4 store partitions)
- `categories`: **50** filas
- `product_category`: **112.010** filas (mapeo producto → categoría, con posible 1:N)

### 2.1 Esquema de transacciones

| Columna | Tipo | Origen |
|---|---|---|
| `date_raw` | string → `date` (via `to_date`) | CSV |
| `store_id` | int | CSV o extraído del nombre del archivo |
| `customer_id` | int | CSV |
| `product_list_raw` | string (IDs separados por espacio) | CSV |
| `source_file` | string | `input_file_name()` |
| `ingest_ts` | timestamp | `current_timestamp()` |

---

## 3. Capa Silver — Explosión y enriquecimiento

`src/pipeline/silver.py` transforma las canastas (1 transacción con N productos) a formato largo (1 fila por producto por transacción).

### 3.1 Identificador único de transacción

Se genera `transaction_id` como `sha2(concat_ws("|", date, store_id, customer_id, product_list_raw), 256)`. Esto garantiza que canastas idénticas tengan el mismo hash, haciendo los joins idempotentes entre ejecuciones.

### 3.2 Explosión de la lista de productos

La columna `product_list_raw` se divide por `\s+`, se aplica `explode()` y se descartan tokens vacíos. El ID de producto se castea a `int`. Luego se agrupa por `(transaction_id, date, store_id, customer_id, product_id)` y se cuenta con `count(lit(1))` para obtener `qty`.

### 3.3 Resolución del catálogo

`ProductCategory.csv` contiene mapeos 1:N (un producto puede pertenecer a varias categorías). Para garantizar joins 1:1 en Silver se agrega por `product_id` quedándose con `min(category_id)`. Luego se hace left join contra `Categories` para traer `category_name`.

### 3.4 Resultado Silver

La tabla `transactions_items` contiene **10.591.792** filas (~10,6 M), particionada por `store_id`. Esquema final:

| Columna | Tipo |
|---|---|
| `transaction_id` | string (SHA-256) |
| `date` | date |
| `store_id` | int |
| `customer_id` | int |
| `product_id` | int |
| `category_id` | int (nullable si no tiene mapeo) |
| `category_name` | string (nullable) |
| `qty` | long |

---

## 4. Capa Gold — Data marts analíticos

`src/pipeline/gold.py` calcula cinco tablas agregadas a partir de Silver.

### 4.1 `fact_kpis`

Una única fila con los totales globales del dataset:

| Indicador | Valor |
|---|---|
| Unidades totales | **10.591.793** |
| Transacciones totales | **1.108.986** |
| Clientes únicos | **131.186** |
| Productos distintos | **449** |
| Categorías distintas | **20** (de 50 en catálogo) |
| Fecha mínima | 2013-01-01 |
| Fecha máxima | 2013-06-30 |

### 4.2 `fact_sales_daily` (724 filas)

Agregado `(date, store_id) → (units, txn_count, customers)`. Alimenta la serie de tiempo, el heatmap calendario y los filtros del dashboard.

### 4.3 `dim_customer_features` (131.186 clientes)

Seis features de comportamiento por cliente que alimentan el modelo K-Means:

| Feature | Descripción | Rango observado |
|---|---|---|
| `frequency` | Número de transacciones distintas | 1 – 535 |
| `units_total` | Suma de unidades compradas | 1 – 2.860 |
| `distinct_products` | Productos distintos comprados | 1 – 356 |
| `distinct_categories` | Categorías distintas compradas | 1 – 18 |
| `avg_basket_size` | `units_total / frequency` (tamaño promedio de canasta) | 1,0 – 58,0 |
| `recency_days` | Días desde la última compra hasta `max(date)` | 0 – 181 |

### 4.4 `dim_product_features` (449 productos)

Métricas por producto: `units_total`, `txn_count`, `distinct_customers`, más su `category_id` y `category_name`.

**Top 5 productos por unidades:**
1. Prod 5 → 300.526 uds (AROMÁTICAS CONDIMENTOS)
2. Prod 10 → 290.313 uds (sin categoría)
3. Prod 3 → 269.855 uds (VERDURAS RAÍZ)
4. Prod 4 → 260.418 uds (VERDURAS RAÍZ)
5. Prod 6 → 254.644 uds (AROMÁTICAS MEDICINALES)

### 4.5 `fact_category_metrics` (21 categorías)

Métricas agregadas por categoría: `units`, `txn_count`, `customers`, `distinct_products`.

**Hallazgo relevante:** **206 productos (46 % de los transaccionados) no tienen categoría asignada** en el catálogo, acumulando 5.330.800 unidades (~50 % del volumen total) bajo `NULL`. Esto constituye un problema de calidad de datos.

**Top categorías con categoría conocida:**
1. VERDURAS RAÍZ, TUBÉRCULO Y BULBOS → 1.811.523 uds
2. VERDURAS DE FRUTOS → 1.410.750 uds
3. JUGOS → 729.513 uds
4. AROMÁTICAS CONDIMENTOS → 493.388 uds
5. AROMÁTICAS MEDICINALES → 294.753 uds

---

## 5. Modelos analíticos

`src/pipeline/models.py` entrena y persiste tres modelos. Las constantes están definidas como módulo-level para facilitar su reconfiguración.

### 5.1 Segmentación de clientes con K-Means

**Hiperparámetros:** `maxIter=50, tol=1e-4, seed=42`, `StandardScaler` con `withMean=True, withStd=True`.

**Preprocesamiento:** las 6 features del `dim_customer_features` se ensamblan con `VectorAssembler` y se escalan con `StandardScaler` (z-score). Esto evita que `units_total` (rango 1–2.860) domine sobre `avg_basket_size` o `recency_days`.

**Evaluación de k:** se prueba k ∈ {3, 4, 5, 6} sobre una **muestra aleatoria del 10 %** de clientes (≈13.119), midiendo silhouette con distancia euclídea al cuadrado:

| k | Silhouette |
|---|---|
| 3 | 0,4990 |
| 4 | 0,4784 |
| **5** | **0,5059** |
| 6 | 0,4796 |

k=5 se selecciona por tener el silhouette más alto (0,506) y se **reentrena sobre el dataset completo** (131.186 clientes).

**Re-etiquetado:** los cluster_id nativos de Spark ML son arbitrarios. Se renumeran **por tamaño descendente** para que el cluster 0 sea siempre el mayoritario. Esto estabiliza la visualización en el dashboard entre re-entrenamientos.

**Perfiles de los 5 segmentos (promedios sobre features sin escalar):**

| Cluster | n (clientes) | % | Frecuencia | Unidades totales | Productos distintos | Categorías distintas | Canasta media | Recencia (días) | Etiqueta de negocio |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **0** | 50.966 | **39 %** | 3,7 | 17,8 | 13,1 | 3,4 | 4,9 | 28,5 | 🟡 Ocasionales recientes |
| **1** | 32.086 | **24 %** | 14,3 | 119,6 | 51,6 | 7,7 | 9,0 | 12,8 | 🟢 Regulares activos |
| **2** | 27.964 | **21 %** | 1,5 | 7,7 | 7,0 | 2,2 | 4,8 | 124,6 | ⚫ Inactivos / dormidos |
| **3** | 10.523 | **8 %** | 35,0 | 430,9 | 106,3 | 10,9 | 13,8 | 4,8 | 🔵 VIPs / power users |
| **4** | 9.647 | **7 %** | 4,8 | 113,7 | 53,7 | 7,5 | **24,5** | 45,2 | 🟠 Canasta grande |

**Validación cualitativa:**
- El cliente 336296 (535 transacciones, 2.860 unidades) cae en el cluster 3 (VIP).
- El cluster 4 (canasta grande, baja frecuencia) confirma la baja correlación entre `frequency` y `avg_basket_size`.
- Los clusters 1 y 3 tienen recencia baja (<13 días), señal de clientes activos recientemente.

**Persistencia:** el `Pipeline` de preprocesamiento (assembler + scaler) se guarda en `data/models/kmeans_preprocessor/` y el modelo K-Means final en `data/models/kmeans_pipeline/`.

### 5.2 Recomendador producto–producto con FP-Growth

**Hiperparámetros:** `minSupport=0.05`, `minConfidence=0.30`, `maxBasketSize=30`, `topNProducts=200`.

**Poda del espacio de productos:** el dataset tiene 449 productos transaccionados; con 1,1 M de canastas y `minSupport=0.01` se produce un OutOfMemoryError local. Se filtra a los **top-200 productos por volumen** y se requieren canastas de 2 a 30 ítems. Con `minSupport=0.05` (≈55.000 canastas como umbral) el modelo entrena en ~30 segundos.

**Resultados:** **327 reglas de asociación** producidas. El dashboard expone las reglas en dos vistas: (a) producto → productos asociados (filtrado por antecedente), (b) tabla global top-30 por lift.

**Top reglas por lift:**

| Antecedente | Categoría antecedente | Consecuente | Categoría consecuente | Confianza | Lift |
|---|---|---|---|---|---|
| Prod 2 | VERDURAS DE FRUTOS | Prod 1 | (sin categoría) | 0,423 | **4,44** |
| Prod 1 | (sin categoría) | Prod 2 | VERDURAS DE FRUTOS | 0,528 | **4,44** |
| Prod 3 | VERDURAS RAÍZ | Prod 21 | (sin categoría) | 0,575 | 3,13 |
| Prod 16 | (sin categoría) | Prod 21 | (sin categoría) | 0,575 | 3,13 |
| Prod 3 | VERDURAS RAÍZ | Prod 16 | (sin categoría) | 0,605 | 2,94 |
| Prod 31 | (sin categoría) | Prod 3 | VERDURAS RAÍZ | 0,731 | 2,90 |

**Lectura:** las reglas con mayor lift cruzan productos de verduras (raíz, frutos) y aromáticas, coherente con una operación dominada por frescos. Un lift de 4,44 significa que comprar Prod 1 multiplica por ~4,4× la probabilidad de comprar Prod 2 respecto al azar.

**Persistencia:** modelo guardado en `data/models/fpgrowth/`.

### 5.3 Recomendador cliente–producto con ALS implícito

**Hiperparámetros:** `rank=16, maxIter=10, regParam=0.05, implicitPrefs=True, coldStartStrategy="drop"`, semilla 42.

**Matriz de interacción:** se agrega `(customer_id, product_id) → sum(qty)` desde Silver, usando la cantidad como *confianza* del feedback positivo implícito (no como rating explícito).

**Resultados:** **1.311.860 recomendaciones** (131.186 clientes × 10). Los scores decrecen suavemente con el rank (avg score rank 1 = 0,751; rank 10 = 0,586), lo que indica discriminación.

**Validación cualitativa:** para el cliente 336296 (VIP), ALS recomienda productos que incluyen Prod 3 (VERDURAS RAÍZ — la categoría #1 del dataset), consistente con su historial dominado por frescos.

**Persistencia:** modelo guardado en `data/models/als/`.

### 5.4 Limitaciones reconocidas

- **Cold-start ALS:** clientes sin historial no reciben recomendaciones (`coldStartStrategy="drop"`). Se sugiere fallback a top-N de su cluster K-Means asignado.
- **FP-Growth limitado al top-200 productos:** por restricción de memoria local. En cluster real se podría reducir `minSupport` para capturar reglas de cola larga.
- **Sin split train/test:** el alcance académico entrega métricas internas (silhouette, support/confidence/lift, score ALS). Una iteración productiva agregaría hold-out temporal.
- **Pesos iguales en K-Means:** las 6 features se ponderan igual tras escalado. Se podría re-escalar con pesos de negocio.

---

## 6. Ingesta incremental (RF-8)

`src/pipeline/ingest.py` es un módulo CLI que soporta `--check` (solo reportar), `--run` (ejecutar si hay cambios) y `--force` (ejecutar siempre), más `--skip-models` para iterar rápido.

### 6.1 Mecanismo de detección

1. Escanea todos los CSV bajo `data/landing/Transactions/` y `data/landing/Products/`.
2. Calcula `sha256` de cada archivo en bloques de 1 MB.
3. Compara contra `data/landing/_manifest.json` (mapping `ruta_relativa → sha256`).
4. Clasifica los cambios en: **nuevos**, **modificados**, **eliminados**.
5. Si hay cambios → ejecuta el pipeline completo (bronze → silver → gold → models).
6. Si no hay cambios → termina sin reprocesar.

### 6.2 Decisión de diseño

Se optó por **reprocesar todo** en lugar de implementar diffs incrementales. Justificación:
- Bronze ya está en `mode("overwrite")`.
- El pipeline completo toma <3 minutos.
- Se garantiza consistencia entre todos los data marts sin lógica de delta.
- El manifest solo evita ejecuciones innecesarias cuando no hay cambios.

### 6.3 Histórico

Cada ejecución se registra en `data/landing/_runs.jsonl` con: timestamp de inicio/fin, archivos nuevos/modificados/eliminados, tiempos por etapa, indicador de `--force` y `--skip-models`.

---

## 7. Dashboard Streamlit

`app/streamlit_app.py` (1.098 líneas) implementa un dashboard web de una sola página que carga secuencialmente todo el contenido analítico. Se conecta a los Parquet de Gold mediante DuckDB en modo `:memory:`, con vistas perezosas (`CREATE VIEW ... AS SELECT * FROM read_parquet(...)`).

### 7.1 Secciones del dashboard

| Sección | Contenido |
|---|---|
| **Panel General** | KPIs (unidades, transacciones, clientes, tiendas), top-10 productos, top-10 clientes, serie de tiempo con heatmap calendario, categorías por volumen (barras + pie) |
| **Gráficos Interactivos** | Serie de tiempo configurable (diaria/semanal, transacciones/unidades/ambas), boxplot por cliente/categoría, heatmap de correlación entre las 6 features |
| **Perfiles de Cliente** | Búsqueda de k óptimo (silhouette por k), distribución de clusters, heatmap normalizado de perfiles, tabla de interpretación de negocio, buscador de cluster por customer_id |
| **Sugerencias de Productos** | Reglas FP-Growth (producto → productos asociados), recomendaciones ALS (cliente → productos sugeridos comparado con historial), top-30 reglas globales por lift |
| **Procesar Datos** | Subida de CSV, botones check/run/force del pipeline incremental, visualización del manifest y del histórico de corridas |

### 7.2 Filtros globales

Sidebar con filtro multiselect de tiendas (4 tiendas disponibles) y rango de fechas fijo (2013-01-01 a 2013-06-30). Todos los queries de DuckDB incorporan estos filtros vía interpolación de strings SQL.

### 7.3 Diseño visual

Tema oscuro personalizado con CSS (`background: linear-gradient(135deg, #0f0a06, #1a110a)`), tarjetas KPI con borde resaltado y animación hover, paleta de colores Plotly personalizada (`#f39c12` como color principal).

### 7.4 Observación sobre la navegación

La variable de página está hardcodeada a `"Panel General"` y no existe un selector de página en el sidebar. Como resultado, **todo el contenido de las 5 secciones se renderiza secuencialmente** en una sola página larga, en lugar de funcionar como pestañas navegables. Esto no impide el acceso a la información pero afecta la experiencia de usuario.

---

## 8. Stack tecnológico

| Capa | Herramienta | Versión | Justificación |
|---|---|---|---|
| Procesamiento ETL | PySpark (modo local) | 3.5.1 | Eje del curso; SQL declarativo; particionado; escala a cluster |
| Almacenamiento | Parquet + medallion | — | Columnar, comprimido, particionable |
| ML | Spark MLlib | 3.5.1 | KMeans, FPGrowth, ALS nativos; pipeline unificado |
| Visualización | Streamlit | 1.36.0 | Multipágina, despliegue trivial |
| Consulta analítica | DuckDB | 1.0.0 | Lectura directa de Parquet sin levantar Spark |
| Dependencias | pandas, plotly, pyarrow | varias | Procesamiento ligero y gráficos |
| Orquestación | Makefile + CLI Python | — | Suficiente para alcance académico |
| Despliegue | Docker + Render | — | Streamlit en contenedor |

---

## 9. Reproducción

```bash
make install         # crea .venv e instala dependencias
make pipeline        # bronze → silver → gold → models (~3 min)
make app             # abre http://localhost:8501
```

Comandos adicionales:

```bash
make models          # solo re-entrenar modelos
make ingest-check    # reportar cambios en data/landing/
make ingest          # ejecutar pipeline si hay cambios
make clean           # borrar datos procesados y modelos
```

También se puede ejecutar por etapas individuales:

```bash
python -m src.pipeline.run --step bronze
python -m src.pipeline.run --step silver
python -m src.pipeline.run --step gold
python -m src.pipeline.run --step models
```

---

## 10. Conclusiones

1. **Operación dominada por frescos:** las categorías de verduras (raíz, frutos) y aromáticas concentran el mayor volumen. Las reglas FP-Growth con lift > 3 entre productos de estas categorías validan el comportamiento de "canasta de mercado tradicional".

2. **Calidad de datos del catálogo:** el 46 % de los productos transaccionados (206 de 449) no tienen asignación de categoría, acumulando ~5,3 M de unidades sin clasificar. Es el hallazgo más relevante para el negocio.

3. **Segmentación interpretable:** K-Means con k=5 produce clusters con perfiles de negocio claros (ocasionales 39 %, regulares 24 %, inactivos 21 %, VIPs 8 %, canasta grande 7 %). El silhouette de 0,506 indica una separación moderada pero útil.

4. **Cliente outlier:** el cliente 336296 (535 transacciones, cluster VIP) merece verificación — posible cuenta institucional o revendedor.

5. **Poco catálogo vendido:** solo 449 productos de ~95.000 catalogados se transaccionaron en 6 meses (0,5 %). Oportunidad de depuración de inventario.

6. **Pesos iguales en features:** todas las variables de cliente se ponderan igual en K-Means tras escalado. Esto es razonable como línea base, pero el negocio podría priorizar recencia o frecuencia según el objetivo comercial.

---

**Anexos:**
- [Documento de arquitectura](arquitectura.md)
- [Resumen ejecutivo](resumen_ejecutivo.md)
- Código fuente: `src/pipeline/{bronze,silver,gold,models,ingest,run,spark_session,paths}.py`
- Dashboard: `app/streamlit_app.py`
- Makefile con targets `install`, `pipeline`, `bronze`, `silver`, `gold`, `models`, `ingest-check`, `ingest`, `app`, `clean`
