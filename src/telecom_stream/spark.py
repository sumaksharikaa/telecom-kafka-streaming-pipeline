"""SparkSession factory. Local mode by default; set SPARK_MASTER to point at a cluster."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from telecom_stream.config import Config

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def get_spark(app_name: str, cfg: Config, hive: bool = False, kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )
    if kafka:
        # Pulled from Maven on first use; pre-bake it into the image for air-gapped clusters.
        builder = builder.config("spark.jars.packages", os.environ.get("SPARK_KAFKA_PACKAGE", KAFKA_PACKAGE))
    if hive:
        # Embedded Derby metastore for local runs; a real cluster points at a shared Hive metastore.
        cfg.hive_dir.mkdir(parents=True, exist_ok=True)
        builder = (
            builder.enableHiveSupport()
            .config("spark.sql.warehouse.dir", str(cfg.hive_dir / "warehouse"))
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={cfg.hive_dir / 'metastore_db'};create=true",
            )
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={cfg.hive_dir}")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
