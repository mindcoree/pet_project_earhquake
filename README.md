# pet_project_earhquake

Pet-проект по дата-инжинирингу для построения аналитического контура по землетрясениям на основе открытых данных USGS.

Проект реализует полный ETL/ELT-процесс: данные забираются из внешнего API, сохраняются в объектное хранилище, загружаются в staging-слой, затем преобразуются в ODS и агрегируются в витринный (DM) слой по модели «звезда» для последующей аналитики и визуализации.

Оркестрация пайплайнов выполняется в Apache Airflow через набор взаимосвязанных DAG'ов. Для локальной инфраструктуры используются Docker Compose, MinIO (S3-совместимое хранилище), PostgreSQL (DWH) и Metabase (BI-слой).

Основная цель проекта - отработать практический подход к построению многоуровневого DWH-пайплайна с идемпотентной загрузкой, декомпозицией по слоям данных и подготовкой данных для BI-отчетности.

## Создание виртуального окружения

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt
```

## Разворачивание инфраструктуры

```bash
docker-compose up -d
```

## Предварительная настройка MinIO and PostgreSQL для Airflow

После старта контейнеров необходимо настроить доступы, которые используются DAG-ами.

1. Откройте MinIO (`http://localhost:9001`) и войдите с дефолтными данными:
   - login: `minioadmin`
   - password: `minioadmin`
2. В MinIO создайте пару ключей доступа:
   - `access key`
   - `secret key`
3. Откройте Airflow (`http://localhost:8080`) и войдите с дефолтными данными:
    - login: `airflow`
    - password: `airflow`
4. В Airflow перейдите в `Admin -> Variables` и создайте переменные:
    - `access_key` - значение `access key` из MinIO
    - `secret_key` - значение `secret key` из MinIO
    - `pg_password` - `postgres` (как указано в `docker-compose.yaml` для `postgres_dwh`)
5. В Airflow перейдите в `Admin -> Connections` и создайте подключение к DWH:
   - Connection Id: `postgres_dwh`
   - Connection Type: `Postgres`
   - Host: `postgres_dwh`
   - Database: `postgres`
   - Login: `postgres`
   - Password: `postgres`
   - Port: `5432`

Без переменных и подключения `postgres_dwh` DAG-и не смогут корректно подключаться к MinIO и PostgreSQL.

## Предварительная настройка Metabase

Чтобы Metabase видел таблицы DWH, добавьте подключение к PostgreSQL:

1. Откройте Metabase (`http://localhost:3000`) и перейдите в создание/настройку подключения к базе.
2. Укажите параметры подключения:
   - Тип базы данных: `PostgreSQL`
   - Отображаемое имя: любое (например, `DWH`)
   - Host: `postgres_dwh`
   - Port: `5432`
   - Имя базы данных: `postgres`
   - Имя пользователя: `postgres`
   - Пароль: `postgres`
3. Сохраните подключение и выполните синхронизацию схемы, если таблицы не появились сразу.

Значения пользователя и пароля берутся из `docker-compose.yaml` (сервис `postgres_dwh`).

## Источники

- [Описание работы API](https://earthquake.usgs.gov/fdsnws/event/1/#methods)
- [Описание полей из API](https://earthquake.usgs.gov/data/comcat/index.php)

## Диаграмма проекта

```mermaid
flowchart LR
    subgraph API
        direction LR
        API_E["Earthquake API<br/>(USGS FDSN)"]
    end

    subgraph ETL
        direction LR
        AirFlow["Apache Airflow<br/>(Orchestration)"]
    end

    subgraph Storage
        direction LR
        S3["MinIO S3<br/>s3://prod/raw/earthquake/..."]
    end

    subgraph DWH ["DWH"]
        direction LR
        subgraph PostgreSQL ["PostgreSQL"]
            direction LR
            subgraph model ["Data Model"]
                direction LR
                stg["STG Layer<br/>stg.earthquake_raw"]
                ods["ODS Layer<br/>ods.earthquakes"]
                dm["Data Mart Layer<br/>Snowflake"]
            end
        end
    end

    subgraph BI
        direction LR
        MetaBase["Metabase"]
    end

    %% Main data flow
    API_E -->|"1. Extract Data<br/>(CSV via read_csv_auto)"| AirFlow
    AirFlow -->|"2. Load Data<br/>(.gz.parquet)"| S3
    S3 -->|"3. Extract Data<br/>(read_parquet)"| AirFlow
    AirFlow -->|"4. Load Data to STG<br/>(DELETE + INSERT)"| stg
    stg -->|"5. Transform to ODS<br/>(CAST, region logic, UPSERT)"| AirFlow
    AirFlow -->|"6. Load Data to ODS"| ods
    ods -->|"7. Transform to DM<br/>(Dimensions + Fact)"| AirFlow
    AirFlow -->|"8. Load Data to DM"| dm
    dm -->|"9. Visualize Data"| MetaBase

    %% Enhanced Styling for better visibility
    style API fill:#FFD1DC, stroke:#1F2A44, stroke-width:3px
    style ETL fill:#D9E5E4, stroke:#1F2A44, stroke-width:3px
    style Storage fill:#FFF2CC, stroke:#1F2A44, stroke-width:3px
    style DWH fill:#C9DAF7, stroke:#1F2A44, stroke-width:5px
    style PostgreSQL fill:#E2F0CB, stroke:#1F2A44, stroke-width:4px
    style BI fill:#B69CFA, stroke:#1F2A44, stroke-width:3px

    %% STG / ODS / DM — максимально читаемые
    classDef layer fill:#FFFFFF, stroke:#1F2A44, stroke-width:3px, rx:10, ry:10, font-size:15px, color:#1F2A44, font-weight:600
    class stg,ods,dm layer

    %% Subgraph titles
    style DWH color:#1F2A44, font-weight:bold, font-size:16px
    style PostgreSQL color:#1F2A44, font-weight:bold, font-size:15px
```

## SQL-файлы

- DDL схем и таблиц: `sql/schemas/01_create_schemas_and_tables.sql`
- SQL для визуализаций в Metabase: `sql/graphic/`
