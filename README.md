# Proyecto Bigdata-03 - Sistema Chinook Data Lake

Proyecto completo de análisis de datos con sistema transaccional, data lake y ETLs para extracción de insights de ventas de música.

## 📋 Descripción General

Este proyecto implementa una solución completa de Big Data que incluye:

1. **Sistema Transaccional**: Aplicación web FastAPI + React para ventas de música
2. **Data Lake en S3**: Almacenamiento analítico con arquitectura RAW → PROCESSED → ANALYTICS
3. **ETLs**: Procesos de extracción, transformación y carga de datos
4. **AWS Glue**: Catalogación automática de datos
5. **Amazon Athena**: Consultas SQL sobre el data lake

## 🏗️ Arquitectura

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│   WebApp        │      │   RDS/MySQL  │      │   S3 Bucket     │
│  (FastAPI +     │─────▶│  Chinook DB  │◀─────│  Data Lake      │
│   React)        │      │              │ ETLs │                 │
└─────────────────┘      └──────────────┘      └─────────────────┘
                                │                        │
                                │                        │
                                ▼                        ▼
                         ┌──────────────┐      ┌─────────────────┐
                         │  AWS Glue    │─────▶│  Amazon Athena  │
                         │  Crawler     │      │  (Consultas)    │
                         └──────────────┘      └─────────────────┘
```

## 📁 Estructura del Proyecto

```
Bigdata-03/
├── .github/                   # GitHub Actions workflows
│   └── workflows/
│       ├── ci-cd-pipeline.yml       # Pipeline principal CI/CD
│       ├── blank.yml                # Template básico
│       └── blankexample.yml         # Ejemplo de referencia
│
├── WebApp/                    # Aplicación web transaccional
│   ├── frontend/             # React + TypeScript
│   ├── backend/              # FastAPI + SQLAlchemy
│   └── docker-compose.yml    # Orquestación de contenedores
│
├── etls/                      # ETLs para el data lake
│   ├── etl_0_full_copy.py    # Copia completa RDS → S3
│   ├── etl_1_ventas_por_dia.py        # Análisis diario
│   ├── etl_2_artista_mas_vendido.py   # Top artistas/mes
│   ├── etl_3_dia_semana.py            # Análisis por día semana
│   ├── etl_4_mes_mayor_ventas.py      # Análisis mensual
│   └── README.md             # Documentación detallada ETLs
│
├── tests/                     # Tests unitarios
│   ├── test_data_lake.py     # Tests para data_lake.py
│   ├── test_crawler.py       # Tests para crawler.py
│   ├── test_etls.py          # Tests para ETLs
│   ├── test_emr_clustering.py       # Tests para clustering
│   └── test_setup_emr_s3.py         # Tests para S3 setup
│
├── data_lake.py              # Creación de estructura S3
├── crawler.py                # Configuración AWS Glue
├── emr_clustering.py         # Pipeline de ML con KMeans
├── setup_emr_s3.py           # Setup de S3 para EMR
├── requirements.txt          # Dependencias Python
├── verify_setup.sh           # Script de verificación
├── GITHUB_ACTIONS_SETUP.md   # Guía de CI/CD
├── EMR_CLUSTERING.md         # Documentación clustering
├── instrucciones.md          # Especificaciones del proyecto
└── README.md                 # Este archivo
```

## ✅ Punto 1: Aplicación Web (WebApp/)

### Frontend
- **Framework**: React + TypeScript + Vite
- **Características**:
  - Catálogo de canciones con búsqueda y filtros
  - Carrito de compras
  - Navegación por artistas y álbumes
  - Diseño responsive

### Backend
- **Framework**: FastAPI + SQLAlchemy
- **Base de Datos**: MySQL (Chinook)
- **Endpoints**:
  - `/tracks`: Búsqueda de canciones
  - `/artists`: Listado de artistas
  - `/albums`: Álbumes por artista
  - `/cart`: Gestión de carrito

### Despliegue
```bash
cd WebApp
docker-compose up
```

## ✅ Punto 2: Data Lake en S3

### Estructura
```
chinook-datalake/
├── raw/                      # Datos crudos desde RDS
│   ├── Artist/
│   ├── Album/
│   ├── Track/
│   ├── Invoice/
│   ├── InvoiceLine/
│   └── customer_employee_history/
│
├── processed/                # Datos procesados por ETLs
│   ├── ventas_por_dia/      # Métricas diarias
│   ├── artista_mes/          # Top artistas mensuales
│   ├── dia_semana/           # Análisis día de semana
│   └── mes_ventas/           # Análisis mensual
│
└── analytics/                # Reportes y dashboards
    └── informes/

chinook-emr-scripts/         # Scripts para EMR
├── scripts/
│   └── clustering/
│       └── customer_clustering.py
├── logs/
├── output/
└── notebooks/
```

### Módulos

**`data_lake.py`**: Crea la estructura completa del data lake en S3
```python
from data_lake import create_data_lake

result = create_data_lake('chinook-datalake')
```

**`crawler.py`**: Configura AWS Glue para catalogar datos
```python
from crawler import setup_glue_crawler

result = setup_glue_crawler('chinook-datalake', 'chinook_db')
```

### Athena
Los datos en formato Parquet son automáticamente consultables:
```sql
SELECT * FROM etl_ventas_por_dia 
WHERE fecha > '2024-01-01'
ORDER BY total_canciones_vendidas DESC;
```

## ✅ Punto 3: ETLs con AWS Glue

### ETL 0 - Full Copy
**Propósito**: Snapshot completo de todas las tablas

```python
from etls import run_full_copy_etl

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'chinook'
}

result = run_full_copy_etl(db_config, 'chinook-datalake')
```

**Salida**: 
- Todas las tablas en `raw/{tabla}/`
- Histórico cliente-empleado con jerarquía

### ETL 1 - Ventas por Día
**Métricas**:
- Total canciones vendidas por día
- Número de facturas y clientes únicos
- Monto total y ticket promedio
- Día de semana, mes, año, trimestre

### ETL 2 - Artista Más Vendido por Mes
**Métricas**:
- Top artista de cada mes
- Participación porcentual
- Ranking top 5 artistas por mes

### ETL 3 - Día de la Semana con Más Ventas
**Métricas**:
- Ventas por día de la semana
- Comparación semana vs fin de semana
- Promedios normalizados

### ETL 4 - Mes con Mayor Volumen
**Métricas**:
- Ranking histórico de meses
- Mejores meses por año
- Mejor trimestre
- Análisis de tendencias

## 🧪 Tests

### Ejecutar Tests Localmente
```bash
# Verificar configuración completa
./verify_setup.sh

# Todos los tests
PYTHONPATH=$(pwd) pytest tests/ -v

# Tests específicos
PYTHONPATH=$(pwd) pytest tests/test_etls.py -v
PYTHONPATH=$(pwd) pytest tests/test_data_lake.py -v
PYTHONPATH=$(pwd) pytest tests/test_emr_clustering.py -v
```

### Cobertura
- ✅ **54 tests** en total (24 ETL/infra + 30 EMR/S3)
- ✅ 100% de éxito
- ✅ Cobertura de ETLs, data lake, crawler, clustering y S3
- ✅ Uso de mocking para AWS (moto)
- ✅ Tests de PySpark con local[2]

```
================================ 54 passed ================================
```

## 🚀 CI/CD con GitHub Actions

### Configuración Automática

El proyecto incluye un pipeline completo de CI/CD que se ejecuta automáticamente en cada push a `main`:

```yaml
Test → Deploy Data Lake → Deploy EMR Scripts → Deploy ETLs → Notify
```

### Workflows Disponibles

1. **ci-cd-pipeline.yml** (Principal)
   - ✅ Ejecuta todos los tests con pytest
   - 📊 Genera reportes de cobertura
   - 🗄️ Despliega estructura de Data Lake en S3
   - 🕷️ Crea Glue Crawlers
   - 🚀 Sube scripts EMR a S3
   - 📊 Despliega ETLs a S3

2. **blankexample.yml** (Referencia)
   - Ejemplo de deployment de Glue jobs

### Configurar CI/CD

```bash
# 1. Ver guía completa
cat GITHUB_ACTIONS_SETUP.md

# 2. Configurar secretos en GitHub:
# - AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY
# - AWS_SESSION_TOKEN
# - DATA_LAKE_BUCKET_NAME
# - EMR_SCRIPTS_BUCKET_NAME
# - GLUE_DATABASE_NAME
# - GLUE_ROLE_ARN

# 3. Push para activar pipeline
git add .
git commit -m "chore: Activar CI/CD"
git push origin main
```

Ver documentación completa en [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md)

## 🚀 Instalación y Configuración

### Prerrequisitos
- Python 3.12+
- Node.js 18+
- Docker y Docker Compose
- AWS CLI configurado
- Cuenta AWS con permisos para S3, Glue, Athena

### Instalación

1. **Clonar repositorio**
```bash
git clone https://github.com/coffool/Bigdata-03.git
cd Bigdata-03
```

2. **Configurar entorno Python**
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

3. **Instalar dependencias**
```bash
pip install boto3 pymysql pandas pyarrow pytest moto
```

4. **Configurar variables de entorno**
```bash
export DB_HOST="localhost"
export DB_USER="root"
export DB_PASSWORD="password"
export DB_NAME="chinook"
export DB_PORT="3306"
export S3_BUCKET="chinook-datalake"
```

5. **Iniciar aplicación web**
```bash
cd WebApp
docker-compose up -d
```

## 📊 Uso

### Crear Data Lake
```bash
python data_lake.py
```

### Configurar Crawler
```bash
python crawler.py
```

### Ejecutar ETLs
```bash
# ETL individual
python etls/etl_1_ventas_por_dia.py

# Todos los ETLs (crear script)
for etl in etls/etl_*.py; do
    python $etl
done
```

### Consultar en Athena
```sql
-- Ver ventas por día
SELECT fecha, total_canciones_vendidas, monto_total
FROM etl_ventas_por_dia
ORDER BY fecha DESC
LIMIT 10;

-- Top artistas por mes
SELECT mes, nombre_artista, total_canciones_vendidas
FROM etl_top_artista_por_mes
ORDER BY mes DESC, total_canciones_vendidas DESC;
```

## 📈 Métricas del Proyecto

- **4 Puntos Completos**: WebApp + Data Lake + ETLs + EMR Clustering
- **5 ETLs** completamente funcionales
- **54 tests** con 100% de éxito (24 infra + 30 EMR)
- **Pipeline CI/CD** automatizado con GitHub Actions
- **Arquitectura escalable** y modular
- **Documentación completa** con ejemplos
- **Formato Parquet** para optimización
- **Compatible con Athena** out-of-the-box
- **ML Pipeline** con PySpark KMeans clustering

## ✅ Punto 4: EMR Clustering (Nuevo)

### Customer Clustering con PySpark

Pipeline completo de Machine Learning para agrupar clientes según sus preferencias musicales:

**Features Analizadas** (12 total):
- Total de compras y gasto
- Precio y duración promedio de canciones
- Diversidad de géneros (unique_genres)
- Porcentajes por género: Rock, Metal, Jazz, Latin, Blues, Classical, Otros

**Algoritmo**: KMeans con StandardScaler
**Evaluación**: Silhouette Score + método del codo

### Uso

```bash
# Setup de S3 para EMR
python setup_emr_s3.py \
  --bucket-name chinook-emr-scripts \
  --script-path emr_clustering.py

# Ejecutar clustering en EMR
spark-submit \
  --master yarn \
  s3://chinook-emr-scripts/scripts/clustering/customer_clustering.py \
  --data-source s3 \
  --s3-bucket chinook-datalake \
  --k 5
```

Ver documentación completa en [EMR_CLUSTERING.md](EMR_CLUSTERING.md)

## 🔧 Tecnologías Utilizadas

### Backend
- Python 3.12
- FastAPI
- SQLAlchemy
- Pandas
- Boto3 (AWS SDK)
- PyMySQL

### Frontend
- React 18
- TypeScript
- Vite
- Axios

### Infraestructura
- Docker & Docker Compose
- AWS S3
- AWS Glue
- Amazon Athena
- MySQL/RDS

### Testing
- pytest
- moto (AWS mocking)
- unittest.mock

## 📝 Documentación Adicional

- **[GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md)** - Guía completa de CI/CD
- **[EMR_CLUSTERING.md](EMR_CLUSTERING.md)** - Documentación de clustering
- **[etls/README.md](etls/README.md)** - Documentación detallada de ETLs
- **[instrucciones.md](instrucciones.md)** - Especificaciones originales
- **[WebApp/README.md](WebApp/README.md)** - Documentación de la aplicación web
- **[DEPLOYMENT.md](WebApp/DEPLOYMENT.md)** - Guía de despliegue en EC2

## 🤝 Contribución

Este es un proyecto académico. Para sugerencias o mejoras, crear un issue o pull request.

## 📄 Licencia

Este proyecto es parte de un trabajo académico de Big Data.

## 👥 Autor

Proyecto desarrollado como parte del curso de Big Data - 2025