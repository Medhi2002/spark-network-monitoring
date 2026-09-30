"""
Éléments partagés entre le job batch et le job streaming :
schéma des logs, session Spark, nettoyage et règles de détection d'anomalies.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# Schéma explicite : plus rapide et plus sûr que de laisser Spark le deviner (obligatoire en streaming).
SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("src_ip", StringType()),
    StructField("dst_ip", StringType()),
    StructField("dst_port", IntegerType()),
    StructField("protocol", StringType()),
    StructField("bytes", LongType()),
    StructField("latency_ms", DoubleType()),
    StructField("status", StringType()),
])

# Seuils de détection (par fenêtre de temps)
SEUIL_PIC_TRAFIC = 50        # événements envoyés par une même IP
SEUIL_SCAN_PORTS = 20        # ports distincts contactés par une même IP
SEUIL_TAUX_ERREUR = 0.25     # part d'erreurs/timeouts sur un serveur
MIN_EVENTS_SERVEUR = 30      # volume minimal pour juger un serveur


def creer_session(nom_app: str) -> SparkSession:
    spark = (SparkSession.builder
             .appName(nom_app)
             .master("local[*]")                                    # tous les cœurs de la machine
             .config("spark.sql.shuffle.partitions", "8")           # 200 par défaut : trop pour un PC
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def nettoyer(df: DataFrame) -> DataFrame:
    """Conversion du timestamp, suppression des lignes invalides, colonnes dérivées."""
    return (df
            .withColumn("ts", F.to_timestamp("timestamp"))
            .filter(F.col("ts").isNotNull() & F.col("src_ip").isNotNull() & F.col("dst_ip").isNotNull())
            .filter(F.col("bytes") >= 0)
            .withColumn("is_error", F.when(F.col("status") != "OK", 1).otherwise(0))
            .drop("timestamp"))


def _fenetre(duree):
    return F.window("ts", duree)


def stats_globales(df: DataFrame, duree: str) -> DataFrame:
    """Volume, débit, latence et taux d'erreur par fenêtre de temps."""
    return (df.groupBy(_fenetre(duree))
              .agg(F.count("*").alias("nb_evenements"),
                   F.sum("bytes").alias("total_octets"),
                   F.round(F.avg("latency_ms"), 1).alias("latence_moy_ms"),
                   F.round(F.avg("is_error"), 3).alias("taux_erreur"))
              .select(F.col("window.start").alias("debut_fenetre"),
                      F.col("window.end").alias("fin_fenetre"), "*")
              .drop("window"))


def anomalies_ip(df: DataFrame, duree: str) -> DataFrame:
    """Pics de trafic et scans de ports, par IP source et par fenêtre."""
    agg = (df.groupBy(_fenetre(duree), "src_ip")
             .agg(F.count("*").alias("nb_evenements"),
                  F.approx_count_distinct("dst_port").alias("ports_distincts")))
    return (agg
            .withColumn("type_anomalie",
                        F.when(F.col("ports_distincts") > SEUIL_SCAN_PORTS, "SCAN_PORTS")
                         .when(F.col("nb_evenements") > SEUIL_PIC_TRAFIC, "PIC_TRAFIC"))
            .filter(F.col("type_anomalie").isNotNull())
            .select(F.col("window.start").alias("debut_fenetre"),
                    F.col("window.end").alias("fin_fenetre"),
                    "type_anomalie",
                    F.col("src_ip").alias("entite"),
                    "nb_evenements",
                    F.col("ports_distincts").cast("double").alias("valeur")))


def anomalies_serveur(df: DataFrame, duree: str) -> DataFrame:
    """Serveurs dont le taux d'erreurs/timeouts explose sur une fenêtre."""
    agg = (df.groupBy(_fenetre(duree), "dst_ip")
             .agg(F.count("*").alias("nb_evenements"),
                  F.avg("is_error").alias("taux_erreur")))
    return (agg
            .filter((F.col("nb_evenements") >= MIN_EVENTS_SERVEUR) &
                    (F.col("taux_erreur") > SEUIL_TAUX_ERREUR))
            .select(F.col("window.start").alias("debut_fenetre"),
                    F.col("window.end").alias("fin_fenetre"),
                    F.lit("SERVEUR_DEGRADE").alias("type_anomalie"),
                    F.col("dst_ip").alias("entite"),
                    "nb_evenements",
                    F.round("taux_erreur", 3).alias("valeur")))
