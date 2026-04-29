#!/usr/bin/env python3
"""
Chicago Crime Data — Batch Layer (Apache Spark)
Complete implementation with exact schemas matching uploaded CSV headers.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, hour, dayofweek,
    count, sum as spark_sum, avg, when, isnan, lit,
    trim, upper, lower, coalesce, round as spark_round,
    to_date, regexp_extract, monotonically_increasing_id,
    length, substring, countDistinct, sqrt as spark_sqrt,
    cos as spark_cos, broadcast,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, BooleanType, DateType, FloatType,
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
import os, sys, json, re
from datetime import datetime, date
 

# =============================================================================
# CONFIGURATION
# =============================================================================
DATA_DIR   = os.getenv("DATA_DIR",   "/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")

CRIME_PATH           = os.path.join(DATA_DIR, "crimes.csv")
ARRESTS_PATH         = os.path.join(DATA_DIR, "arrests.csv")
POLICE_STATIONS_PATH = os.path.join(DATA_DIR, "Police_Stations_20260420.csv")
VIOLENCE_PATH        = os.path.join(DATA_DIR, "violence_reduction.csv")
SEX_OFFENDERS_PATH   = os.path.join(DATA_DIR, "sex_offenders.csv")

# Large join tables are written with Spark's streaming JSON writer (no collect).
# All analytical / aggregated tables are small enough to collect → pretty JSON + SQL.
# Threshold (rows) above which we switch to streaming write instead of collect.
LARGE_TABLE_THRESHOLD = 100_000
 
# =============================================================================
# OUTPUT HELPERS
# =============================================================================
 
def _sql_identifier(name: str) -> str:
    """Snake-case string → safe SQL table/column name."""
    return re.sub(r"[^a-z0-9_]", "_", name.lower())
 
 
def _py_to_sql_literal(value) -> str:
    """Convert a Python scalar to a SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return f"'{value}'"
    # String — escape single quotes
    return "'" + str(value).replace("'", "''") + "'"
 
 
def _spark_type_to_sql(spark_type) -> str:
    """Map a Spark DataType to a portable SQL column type."""
    from pyspark.sql.types import (
        IntegerType, LongType, DoubleType, FloatType,
        BooleanType, TimestampType, DateType, StringType,
    )
    mapping = {
        IntegerType:   "INTEGER",
        LongType:      "BIGINT",
        DoubleType:    "DOUBLE PRECISION",
        FloatType:     "REAL",
        BooleanType:   "BOOLEAN",
        TimestampType: "TIMESTAMP",
        DateType:      "DATE",
        StringType:    "TEXT",
    }
    for spark_cls, sql_type in mapping.items():
        if isinstance(spark_type, spark_cls):
            return sql_type
    return "TEXT"   # fallback
 
 
def write_output(df, table_name: str, output_dir: str, description: str = "") -> None:
    """
    Write a Spark DataFrame to:
      • <output_dir>/json/<table_name>.json   — pretty-printed JSON array
      • <output_dir>/sql/<table_name>.sql     — CREATE TABLE + INSERT INTO statements
 
    For large DataFrames (> LARGE_TABLE_THRESHOLD rows) the JSON is written
    using Spark's partitioned writer (newline-delimited) to avoid OOM on
    collect(), and only the SQL DDL (no INSERT rows) is written.
    """
    sql_dir  = os.path.join(output_dir, "sql")
    os.makedirs(sql_dir,  exist_ok=True)
 
    table_id = _sql_identifier(table_name)
    schema   = df.schema
 
    # ── 1. SQL DDL (CREATE TABLE) ────────────────────────────────────────────
    col_defs = ",\n    ".join(
        f"{_sql_identifier(f.name)} {_spark_type_to_sql(f.dataType)}"
        for f in schema.fields
        if f.name != "_corrupt_record"
    )
    sql_lines = []
    if description:
        sql_lines.append(f"-- {description}")
    sql_lines.append(f"-- Generated: {datetime.utcnow().isoformat()}Z")
    sql_lines.append(f"\nDROP TABLE IF EXISTS {table_id};")
    sql_lines.append(f"CREATE TABLE {table_id} (\n    {col_defs}\n);\n")
 
    row_count = df.count()
    print(f"    [{table_name}] {row_count:,} rows")
 
    clean_cols = [f.name for f in schema.fields if f.name != "_corrupt_record"]
    df_clean   = df.select(*clean_cols)
 
    if row_count <= LARGE_TABLE_THRESHOLD:
        # ── Small table: collect → pretty JSON array + SQL INSERT rows ────────
        rows = df_clean.collect()
 
        # SQL INSERT
        if rows:
            col_list = ", ".join(_sql_identifier(c) for c in clean_cols)
            insert_prefix = f"INSERT INTO {table_id} ({col_list}) VALUES\n"
            value_rows = []
            for row in rows:
                vals = ", ".join(
                    _py_to_sql_literal(row[c]) for c in clean_cols
                )
                value_rows.append(f"    ({vals})")
            sql_lines.append(insert_prefix + ",\n".join(value_rows) + ";")
 
    else:
        # ── Large table: DDL Only ────────────────────────────────────────────
        # Generating INSERTs for large datasets creates huge, slow SQL files.
        # We provide the schema so the table can be created, but data loading
        # should ideally be done via Parquet/CSV import in the target DB.
        sql_lines.append(f"-- NOTE: Table has {row_count:,} rows.")
        sql_lines.append(f"-- INSERT statements omitted for performance.")
        sql_lines.append(f"-- Please load data from source Parquet/CSV files directly into {table_id}.")
        print(f"    → DDL only (Data omitted due to size: {row_count:,} rows)")
 
    # Write SQL file
    sql_path = os.path.join(sql_dir, f"{table_id}.sql")
    with open(sql_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(sql_lines) + "\n")
    print(f"    → {sql_path}")
 
 
# =============================================================================
# EXACT SCHEMAS (matching uploaded CSV headers)
# =============================================================================

crime_schema = StructType([
    StructField("ID",                   StringType(), True),
    StructField("Case Number",          StringType(), True),
    StructField("Date",                 StringType(), True),   # MM/dd/yyyy hh:mm:ss a
    StructField("Block",                StringType(), True),
    StructField("IUCR",                 StringType(), True),
    StructField("Primary Type",         StringType(), True),
    StructField("Description",          StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest",               StringType(), True),   # "true"/"false"
    StructField("Domestic",             StringType(), True),   # "true"/"false"
    StructField("Beat",                 StringType(), True),
    StructField("District",             StringType(), True),
    StructField("Ward",                 StringType(), True),
    StructField("Community Area",       StringType(), True),
    StructField("FBI Code",             StringType(), True),
    StructField("X Coordinate",         StringType(), True),
    StructField("Y Coordinate",         StringType(), True),
    StructField("Year",                 StringType(), True),
    StructField("Updated On",           StringType(), True),
    StructField("Latitude",             StringType(), True),
    StructField("Longitude",            StringType(), True),
    StructField("Location",             StringType(), True),
])

arrests_schema = StructType([
    StructField("CB_NO",                  StringType(), True),
    StructField("CASE NUMBER",            StringType(), True),
    StructField("ARREST DATE",            StringType(), True),  # MM/dd/yyyy hh:mm:ss a
    StructField("RACE",                   StringType(), True),
    StructField("CHARGE 1 STATUTE",       StringType(), True),
    StructField("CHARGE 1 DESCRIPTION",   StringType(), True),
    StructField("CHARGE 1 TYPE",          StringType(), True),
    StructField("CHARGE 1 CLASS",         StringType(), True),
    StructField("CHARGE 2 STATUTE",       StringType(), True),
    StructField("CHARGE 2 DESCRIPTION",   StringType(), True),
    StructField("CHARGE 2 TYPE",          StringType(), True),
    StructField("CHARGE 2 CLASS",         StringType(), True),
    StructField("CHARGE 3 STATUTE",       StringType(), True),
    StructField("CHARGE 3 DESCRIPTION",   StringType(), True),
    StructField("CHARGE 3 TYPE",          StringType(), True),
    StructField("CHARGE 3 CLASS",         StringType(), True),
    StructField("CHARGE 4 STATUTE",       StringType(), True),
    StructField("CHARGE 4 DESCRIPTION",   StringType(), True),
    StructField("CHARGE 4 TYPE",          StringType(), True),
    StructField("CHARGE 4 CLASS",         StringType(), True),
    StructField("CHARGES STATUTE",        StringType(), True),
    StructField("CHARGES DESCRIPTION",    StringType(), True),
    StructField("CHARGES TYPE",           StringType(), True),
    StructField("CHARGES CLASS",          StringType(), True),
])

police_stations_schema = StructType([
    StructField("DISTRICT",      StringType(), True),   # "Headquarters", "18", etc.
    StructField("DISTRICT NAME", StringType(), True),
    StructField("ADDRESS",       StringType(), True),
    StructField("CITY",          StringType(), True),
    StructField("STATE",         StringType(), True),
    StructField("ZIP",           StringType(), True),   # keep as string — leading-zero safety
    StructField("WEBSITE",       StringType(), True),
    StructField("PHONE",         StringType(), True),
    StructField("FAX",           StringType(), True),
    StructField("TTY",           StringType(), True),
    StructField("X COORDINATE",  StringType(), True),
    StructField("Y COORDINATE",  StringType(), True),
    StructField("LATITUDE",      StringType(), True),
    StructField("LONGITUDE",     StringType(), True),
    StructField("LOCATION",      StringType(), True),
])

violence_schema = StructType([
    StructField("CASE_NUMBER",                  StringType(), True),
    StructField("DATE",                         StringType(), True),  # MM/dd/yyyy hh:mm:ss a
    StructField("BLOCK",                        StringType(), True),
    StructField("VICTIMIZATION_PRIMARY",        StringType(), True),
    StructField("INCIDENT_PRIMARY",             StringType(), True),
    StructField("GUNSHOT_INJURY_I",             StringType(), True),  # "YES"/"NO"
    StructField("UNIQUE_ID",                    StringType(), True),
    StructField("ZIP_CODE",                     StringType(), True),  # keep as string
    StructField("WARD",                         StringType(), True),
    StructField("COMMUNITY_AREA",               StringType(), True),
    StructField("STREET_OUTREACH_ORGANIZATION", StringType(), True),
    StructField("AREA",                         StringType(), True),
    StructField("DISTRICT",                     StringType(), True),
    StructField("BEAT",                         StringType(), True),
    StructField("AGE",                          StringType(), True),
    StructField("SEX",                          StringType(), True),
    StructField("RACE",                         StringType(), True),
    StructField("VICTIMIZATION_FBI_CD",         StringType(), True),
    StructField("INCIDENT_FBI_CD",              StringType(), True),
    StructField("VICTIMIZATION_FBI_DESCR",      StringType(), True),
    StructField("INCIDENT_FBI_DESCR",           StringType(), True),
    StructField("VICTIMIZATION_IUCR_CD",        StringType(), True),
    StructField("INCIDENT_IUCR_CD",             StringType(), True),
    StructField("VICTIMIZATION_IUCR_SECONDARY", StringType(), True),
    StructField("INCIDENT_IUCR_SECONDARY",      StringType(), True),
    StructField("HOMICIDE_VICTIM_FIRST_NAME",   StringType(), True),
    StructField("HOMICIDE_VICTIM_MI",           StringType(), True),
    StructField("HOMICIDE_VICTIM_LAST_NAME",    StringType(), True),
    StructField("MONTH",                        StringType(), True),
    StructField("DAY_OF_WEEK",                  StringType(), True),
    StructField("HOUR",                         StringType(), True),
    StructField("LOCATION_DESCRIPTION",         StringType(), True),
    StructField("STATE_HOUSE_DISTRICT",         StringType(), True),
    StructField("STATE_SENATE_DISTRICT",        StringType(), True),
    StructField("UPDATED",                      StringType(), True),
    StructField("LATITUDE",                     StringType(), True),
    StructField("LONGITUDE",                    StringType(), True),
    StructField("LOCATION",                     StringType(), True),
])

sex_offenders_schema = StructType([
    StructField("LAST",         StringType(), True),
    StructField("FIRST",        StringType(), True),
    StructField("BLOCK",        StringType(), True),
    StructField("GENDER",       StringType(), True),
    StructField("RACE",         StringType(), True),
    StructField("BIRTH DATE",   StringType(), True),  # MM/dd/yyyy
    StructField("HEIGHT",       StringType(), True),  # e.g. "509" = 5'9"
    StructField("WEIGHT",       StringType(), True),
    StructField("VICTIM MINOR", StringType(), True),  # "Y"/"N"
])


# =============================================================================
# GENERIC LOADER / CLEANER
# =============================================================================

def load_and_clean(spark, path, schema, dataset_name):
    """
    Generic loader: read CSV with explicit schema, handle nulls, trim strings,
    standardize column names to snake_case, parse timestamps, cast types.
    """
    print(f"\n{'='*60}")
    print(f"Loading: {dataset_name}")
    print(f"Path: {path}")
    print(f"{'='*60}")

    if not os.path.exists(path):
        print(f"ERROR: File not found: {path}")
        return None

    df = (spark.read
          .option("header",                  "true")
          .option("mode",                    "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("quote",                   "\"")
          .option("escape",                  "\"")
          # FIX 2 — District-12 address contains an embedded newline inside a
          # quoted field ("1412 S Blue Island Ave\n").  multiLine tells the CSV
          # reader to respect RFC-4180 multi-line quoted fields.
          .option("multiLine",               "true")
          .schema(schema)
          .csv(path))

    print(f"\nRaw sample (first 3 rows):")
    df.show(3, truncate=False)
    print(f"Raw row count: {df.count()}")

    df = df.dropna(how="all")

    # Standardise column names → snake_case
    for old_name in df.columns:
        new_name = old_name.lower().replace(" ", "_").replace("-", "_").replace(".", "")
        df = df.withColumnRenamed(old_name, new_name)

    # Trim strings and normalise sentinel-null values
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            c = field.name
            df = df.withColumn(c, trim(col(c)))
            df = df.withColumn(c,
                when(col(c).isin("", "NULL", "null", "NA", "N/A", "#N/A", "None", "NONE"), None)
                .otherwise(col(c))
            )

    # ------------------------------------------------------------------
    # Dataset-specific cleaning / type casting
    # ------------------------------------------------------------------
    if dataset_name == "crimes":
        df = df.withColumn("date",     to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"))
        df = df.withColumn("arrest",   when(lower(col("arrest"))   == "true", True).otherwise(False))
        df = df.withColumn("domestic", when(lower(col("domestic")) == "true", True).otherwise(False))
        df = df.withColumn("id",             col("id").cast(IntegerType()))
        df = df.withColumn("beat",           col("beat").cast(IntegerType()))
        df = df.withColumn("district",       col("district").cast(IntegerType()))
        df = df.withColumn("ward",           col("ward").cast(IntegerType()))
        df = df.withColumn("community_area", col("community_area").cast(IntegerType()))
        df = df.withColumn("year",           coalesce(col("year").cast(IntegerType()), year(col("date"))))
        df = df.withColumn("x_coordinate",   col("x_coordinate").cast(DoubleType()))
        df = df.withColumn("y_coordinate",   col("y_coordinate").cast(DoubleType()))
        df = df.withColumn("latitude",       col("latitude").cast(DoubleType()))
        df = df.withColumn("longitude",      col("longitude").cast(DoubleType()))
        df = df.withColumn("month",          month(col("date")))
        df = df.withColumn("hour",           hour(col("date")))
        df = df.withColumn("day_of_week",    dayofweek(col("date")))
        df = df.dropna(subset=["case_number", "district"])

    elif dataset_name == "arrests":
        df = df.withColumn("arrest_date", to_timestamp(col("arrest_date"), "MM/dd/yyyy hh:mm:ss a"))
        df = df.withColumn("race", upper(trim(col("race"))))
        df = df.dropna(subset=["case_number"])

    elif dataset_name == "police_stations":
        df = df.withColumn("district_clean",
            when(col("district") == "Headquarters", lit(0))
            .otherwise(col("district").cast(IntegerType()))
        )
        df = df.withColumn("x_coordinate", col("x_coordinate").cast(DoubleType()))
        df = df.withColumn("y_coordinate", col("y_coordinate").cast(DoubleType()))
        df = df.withColumn("latitude",     col("latitude").cast(DoubleType()))
        df = df.withColumn("longitude",    col("longitude").cast(DoubleType()))
        # Keep zip as StringType — ZIP codes must preserve leading zeros in general

    elif dataset_name == "violence":
        df = df.withColumn("date",          to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"))
        df = df.withColumn("gunshot_injury_i", when(upper(col("gunshot_injury_i")) == "YES", True).otherwise(False))
        df = df.withColumn("district",       col("district").cast(IntegerType()))
        df = df.withColumn("beat",           col("beat").cast(IntegerType()))
        df = df.withColumn("ward",           col("ward").cast(IntegerType()))
        df = df.withColumn("community_area", col("community_area").cast(IntegerType()))
        df = df.withColumn("age",            col("age").cast(IntegerType()))
        df = df.withColumn("month",          col("month").cast(IntegerType()))
        df = df.withColumn("day_of_week",    col("day_of_week").cast(IntegerType()))
        df = df.withColumn("hour",           col("hour").cast(IntegerType()))
        df = df.withColumn("latitude",       col("latitude").cast(DoubleType()))
        df = df.withColumn("longitude",      col("longitude").cast(DoubleType()))
        df = df.withColumn("state_house_district",  col("state_house_district").cast(IntegerType()))
        df = df.withColumn("state_senate_district", col("state_senate_district").cast(IntegerType()))
        df = df.dropna(subset=["case_number", "district"])

    elif dataset_name == "sex_offenders":
        df = df.withColumn("birth_date", to_date(col("birth_date"), "MM/dd/yyyy"))
        # "509" → 5*12 + 9 = 69 in;  "603" → 6*12 + 3 = 75 in
        df = df.withColumn("height_inches",
            when(length(col("height")) == 3,
                (substring(col("height"), 1, 1).cast(IntegerType()) * 12) +
                 substring(col("height"), 2, 2).cast(IntegerType())
            ).when(length(col("height")) == 4,
                (substring(col("height"), 1, 2).cast(IntegerType()) * 12) +
                 substring(col("height"), 3, 2).cast(IntegerType())
            ).otherwise(col("height").cast(IntegerType()))
        )
        df = df.withColumn("weight",       col("weight").cast(IntegerType()))
        df = df.withColumn("victim_minor", when(upper(col("victim_minor")) == "Y", True).otherwise(False))
        df = df.withColumn("gender",       upper(trim(col("gender"))))
        df = df.withColumn("race",         upper(trim(col("race"))))

    final_count = df.count()
    print(f"\nCleaned row count: {final_count}")
    print("Schema after cleaning:")
    df.printSchema()
    return df


# =============================================================================
# ANALYTICS
# =============================================================================
 
def compute_crime_trends(crimes_df, output_dir):
    print(f"\n{'='*60}\nCOMPUTING: Crime Trends\n{'='*60}")
 
    yearly = crimes_df.groupBy("year").agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
        spark_round(spark_sum(when(col("arrest") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
        ).alias("arrest_rate_pct")
    ).orderBy("year")
    yearly.show(5)
    write_output(yearly, "yearly_trends", output_dir, "Total crimes and arrests per year")
 
    monthly = crimes_df.groupBy("month").agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests")
    ).orderBy("month")
    write_output(monthly, "monthly_trends", output_dir, "Aggregated crime counts by month (all years)")
 
    monthly_by_year = crimes_df.groupBy("year", "month").agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests")
    ).orderBy("year", "month")
    write_output(monthly_by_year, "monthly_trends_by_year", output_dir, "Crime counts per month per year")
 
    hourly = crimes_df.groupBy("hour").agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests")
    ).orderBy("hour")
    write_output(hourly, "hourly_trends", output_dir, "Crime counts broken down by hour of day (0-23)")
 
    dow = crimes_df.groupBy("day_of_week").agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests")
    ).orderBy("day_of_week")
    write_output(dow, "day_of_week_trends", output_dir, "Crime counts by day of week (1=Sunday, 7=Saturday)")
 
    print("  [DONE] Crime trends written.")
 
 
def compute_arrest_rates(crimes_df, arrests_df, output_dir):
    print(f"\n{'='*60}\nCOMPUTING: Arrest Rates\n{'='*60}")
 
    by_type = crimes_df.groupBy("primary_type").agg(
        count("*").alias("total_incidents"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
        spark_round(spark_sum(when(col("arrest") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
        ).alias("arrest_rate_pct")
    ).orderBy(col("total_incidents").desc())
    by_type.show(5, truncate=False)
    write_output(by_type, "arrest_rate_by_crime_type", output_dir, "Arrest rate per primary crime type")
 
    by_district = crimes_df.groupBy("district").agg(
        count("*").alias("total_incidents"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
        spark_round(spark_sum(when(col("arrest") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
        ).alias("arrest_rate_pct")
    ).orderBy("district")
    write_output(by_district, "arrest_rate_by_district", output_dir, "Arrest rate per police district")
 
    by_race = arrests_df.groupBy("race").agg(
        count("*").alias("total_arrests")
    ).orderBy(col("total_arrests").desc())
    by_race.show(5, truncate=False)
    write_output(by_race, "arrest_rate_by_race", output_dir, "Total arrests by race (from arrests dataset)")
 
    crimes_with_race = crimes_df.join(
        arrests_df.select("case_number", "race").distinct(),
        on="case_number", how="left"
    )
    by_race_crime = crimes_with_race.filter(col("race").isNotNull()).groupBy("race").agg(
        count("*").alias("total_crime_incidents"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
        spark_round(spark_sum(when(col("arrest") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
        ).alias("arrest_rate_pct")
    ).orderBy(col("total_crime_incidents").desc())
    by_race_crime.show(5, truncate=False)
    write_output(by_race_crime, "arrest_rate_by_race_detailed", output_dir,
                 "Arrest rate by race (crimes joined with arrests)")
 
    print("  [DONE] Arrest rates written.")
 
 
def compute_joins(spark, crimes_df, arrests_df, stations_df, output_dir):
    print(f"\n{'='*60}\nCOMPUTING: Dataset Joins\n{'='*60}")
 
    # ── Crimes ⋈ Arrests ────────────────────────────────────────────────────
    print("  -> Crimes JOIN Arrests (case_number)...")
    crimes_cols  = ["case_number", "date", "primary_type", "description",
                    "arrest", "domestic", "district", "ward", "community_area",
                    "latitude", "longitude", "year", "month", "hour", "day_of_week"]
    arrests_cols = ["case_number", "arrest_date", "race",
                    "charge_1_description", "charge_1_type", "charge_1_class",
                    "charges_description", "charges_type", "charges_class"]
 
    crimes_arrests = crimes_df.select(*crimes_cols).join(
        arrests_df.select(*arrests_cols), on="case_number", how="inner"
    )
    write_output(crimes_arrests, "crimes_join_arrests", output_dir,
                 "Inner join of crimes and arrests on case_number")
 
    # ── Crimes ⋈ Police Stations ─────────────────────────────────────────────
    print("  -> Crimes JOIN Police Stations (district)...")
    stations_for_join = (stations_df
        .drop("district")
        .withColumnRenamed("district_clean", "station_district")
        .withColumnRenamed("latitude",       "station_lat")
        .withColumnRenamed("longitude",      "station_lon")
        .withColumnRenamed("address",        "station_address")
        .select("station_district", "district_name",
                "station_address", "station_lat", "station_lon", "phone", "zip")
    )
    crimes_stations = (
        crimes_df.select("case_number", "date", "primary_type", "arrest",
                         "district", "latitude", "longitude", "year")
        .join(broadcast(stations_for_join),
              crimes_df.district == stations_for_join.station_district,
              how="left")
        .drop("station_district")
    )
    write_output(crimes_stations, "crimes_join_stations", output_dir,
                 "Left join of crimes with police station details on district")
 
    print("  [DONE] Joins written.")
 
 
def compute_kmeans_hotspots(spark, crimes_df, output_dir, k=10):
    print(f"\n{'='*60}\nCOMPUTING: K-Means Crime Hotspots\n{'='*60}")
 
    geo_df = crimes_df.filter(
        col("latitude").isNotNull()  & col("longitude").isNotNull() &
        (~isnan(col("latitude")))    & (~isnan(col("longitude")))   &
        (col("latitude") != 0)       & (col("longitude") != 0)
    ).select("id", "case_number", "primary_type", "district", "latitude", "longitude")
 
    valid_count = geo_df.count()
    print(f"  Valid geospatial records: {valid_count:,}")
    if valid_count < k:
        print(f"  WARNING: Not enough records for k={k}. Skipping.")
        return
 
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features_raw")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                               withStd=True, withMean=True)
    kmeans    = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster_id",
                       maxIter=100, initSteps=10)
    model     = Pipeline(stages=[assembler, scaler, kmeans]).fit(geo_df)
    clustered = model.transform(geo_df)
    centers   = model.stages[-1].clusterCenters()
 
    print("\n  Cluster Centers (lat, lon):")
    for i, c in enumerate(centers):
        print(f"    Cluster {i}: ({c[0]:.6f}, {c[1]:.6f})")
 
    cluster_summary = clustered.groupBy("cluster_id").agg(
        count("*").alias("crime_count"),
        spark_round(avg("latitude"),  6).alias("avg_lat"),
        spark_round(avg("longitude"), 6).alias("avg_lon"),
        countDistinct("primary_type").alias("unique_crime_types"),
        spark_round(
            spark_sum(when(col("primary_type").isin(
                "HOMICIDE", "ASSAULT", "BATTERY", "ROBBERY"), 1).otherwise(0)
            ) * 100.0 / count("*"), 2
        ).alias("violent_crime_pct")
    ).orderBy(col("crime_count").desc())
    cluster_summary.show(10, truncate=False)
 
    centers_df = spark.createDataFrame(
        [(i, float(c[0]), float(c[1])) for i, c in enumerate(centers)],
        ["cluster_id", "model_center_lat", "model_center_lon"]
    )
    hotspots = cluster_summary.join(centers_df, on="cluster_id")
    write_output(hotspots,  "crime_hotspots_kmeans",  output_dir,
                 f"K-Means (k={k}) cluster summaries with centroid coordinates")
    write_output(clustered.select("id", "case_number", "primary_type",
                                  "district", "latitude", "longitude", "cluster_id"),
                 "crimes_with_clusters", output_dir,
                 "Each crime record annotated with its K-Means cluster ID")
    print("  [DONE] Hotspots written.")
 
 
def compute_correlations(crimes_df, arrests_df, violence_df, stations_df,
                         sex_offenders_df, output_dir):
    print(f"\n{'='*60}\nCOMPUTING: Cross-Dataset Correlations\n{'='*60}")
 
    # ── 1. Violence rate vs. Arrest rate by district ──────────────────────────
    print("  -> Violence rate vs. Arrest rate by district...")
    violence_by_district = violence_df.groupBy("district").agg(
        count("*").alias("violence_incidents"))
    crime_by_district = crimes_df.groupBy("district").agg(
        count("*").alias("total_crimes"),
        spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"))
    district_corr = (crime_by_district
        .join(violence_by_district, on="district", how="outer")
        .fillna(0)
        .withColumn("arrest_rate",
            when(col("total_crimes") == 0, lit(0))
            .otherwise(spark_round(col("total_arrests") / col("total_crimes"), 4)))
        .withColumn("violence_rate",
            when(col("total_crimes") == 0, lit(0))
            .otherwise(spark_round(col("violence_incidents") / col("total_crimes"), 4)))
        .withColumn("violence_per_1000",
            spark_round(col("violence_incidents") * 1000.0 / col("total_crimes"), 2))
    )
    corr_val = district_corr.stat.corr("violence_rate", "arrest_rate")
    print(f"      Pearson r (violence_rate vs arrest_rate by district): {corr_val:.4f}")
    district_corr.show(10, truncate=False)
    write_output(district_corr, "correlation_violence_arrest_by_district", output_dir,
                 f"Violence vs arrest rate by district (Pearson r={corr_val:.4f})")
 
    # ── 2. Crime rate vs. Distance to nearest police station ─────────────────
    print("  -> Crime rate vs. Station distance by district...")
    stations_select = (stations_df
        .filter(col("district_clean").isNotNull())
        .select(col("district_clean").alias("station_dist"),
                col("latitude").alias("station_lat"),
                col("longitude").alias("station_lon")))
    crimes_with_dist = (crimes_df
        .join(stations_select, crimes_df.district == stations_select.station_dist, how="left")
        .withColumn("lat_diff", (col("latitude") - col("station_lat")) * 111_000.0)
        .withColumn("lon_diff",
            (col("longitude") - col("station_lon")) * 111_000.0
            * spark_cos(col("latitude") * 3.14159265358979 / 180.0))
        .withColumn("distance_to_station_m",
            spark_round(spark_sqrt(col("lat_diff")**2 + col("lon_diff")**2), 2))
    )
    station_dist_stats = crimes_with_dist.groupBy("district").agg(
        spark_round(avg("distance_to_station_m"), 2).alias("avg_dist_to_station_m"),
        count("*").alias("crime_count"))
    station_dist_stats.show(10, truncate=False)
    write_output(station_dist_stats, "crime_station_distance_by_district", output_dir,
                 "Average crime-to-station distance (metres) and total crimes per district")
 
    # ── 3. Sex offender density vs. crime rate (by block prefix) ─────────────
    print("  -> Sex offender density vs. crime rate (by block)...")
    offender_blocks = (sex_offenders_df
        .filter(col("block").isNotNull())
        .withColumn("block_key", regexp_extract(col("block"), r"^(\d+)", 1))
        .groupBy("block_key").agg(count("*").alias("offender_count")))
    crime_blocks = (crimes_df
        .filter(col("block").isNotNull())
        .withColumn("block_key", regexp_extract(col("block"), r"^(\d+)", 1))
        .groupBy("block_key").agg(count("*").alias("crime_count")))
    block_corr = crime_blocks.join(offender_blocks, on="block_key", how="left").fillna(0)
    corr_block = block_corr.stat.corr("crime_count", "offender_count")
    print(f"      Pearson r (crime_count vs offender_count by block): {corr_block:.4f}")
    write_output(block_corr, "correlation_offenders_crime_by_block", output_dir,
                 f"Sex offender density vs crime count by block (Pearson r={corr_block:.4f})")
 
    print("  [DONE] Correlations written.")

# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    spark = (SparkSession.builder
             .appName("ChicagoCrimeBatchLayer")
             .config("spark.sql.adaptive.enabled",                        "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled",     "true")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             # Safety net: also disable constraint propagation here so the job
             # works even if the --conf flag is omitted from spark-submit.
             .config("spark.sql.optimizer.constraintPropagation.enabled", "false")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    # Checkpoint dir lets Spark truncate long lineage chains that accumulate
    # across multiple chained transforms on the same base DataFrame.
    spark.sparkContext.setCheckpointDir(os.path.join(OUTPUT_DIR, "_checkpoints"))

    print(f"\n{'#'*60}")
    print("#" + "  CHICAGO CRIME DATA - BATCH LAYER  ".center(58) + "#")
    print(f"{'#'*60}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---------- Load all datasets ----------
    crimes_df        = load_and_clean(spark, CRIME_PATH,           crime_schema,           "crimes")
    arrests_df       = load_and_clean(spark, ARRESTS_PATH,         arrests_schema,         "arrests")
    stations_df      = load_and_clean(spark, POLICE_STATIONS_PATH, police_stations_schema, "police_stations")
    violence_df      = load_and_clean(spark, VIOLENCE_PATH,        violence_schema,        "violence")
    sex_offenders_df = load_and_clean(spark, SEX_OFFENDERS_PATH,   sex_offenders_schema,   "sex_offenders")

    if any(df is None for df in [crimes_df, arrests_df, stations_df, violence_df, sex_offenders_df]):
        print("\nERROR: One or more datasets failed to load. Exiting.")
        sys.exit(1)

    # ---------- Analytics ----------
    compute_crime_trends(crimes_df, OUTPUT_DIR)
    compute_arrest_rates(crimes_df, arrests_df, OUTPUT_DIR)
    compute_joins(spark, crimes_df, arrests_df, stations_df, OUTPUT_DIR)
    compute_kmeans_hotspots(spark, crimes_df, OUTPUT_DIR, k=10)
    compute_correlations(crimes_df, arrests_df, violence_df, stations_df, sex_offenders_df, OUTPUT_DIR)

    # ---------- Summary ----------
    print(f"\n{'='*60}")
    print("BATCH LAYER COMPLETE")
    print(f"{'='*60}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("""
    Generated artifacts:
      ├── yearly_trends/
      ├── monthly_trends/
      ├── monthly_trends_by_year/
      ├── hourly_trends/
      ├── day_of_week_trends/
      ├── arrest_rate_by_crime_type/
      ├── arrest_rate_by_district/
      ├── arrest_rate_by_race/
      ├── arrest_rate_by_race_detailed/
      ├── crimes_join_arrests/
      ├── crimes_join_stations/
      ├── crime_hotspots_kmeans/
      ├── crimes_with_clusters/
      ├── correlation_violence_arrest_by_district/
      ├── crime_station_distance_by_district/
      └── correlation_offenders_crime_by_block/
    """)
    spark.stop()


if __name__ == "__main__":
    main()