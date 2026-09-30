"""
Job BATCH : traite l'historique des logs réseau.

1. Lecture des fichiers JSON de data/history avec un schéma explicite
2. Nettoyage (timestamps, doublons, valeurs invalides)
3. Stockage des données propres en Parquet, partitionnées par date
4. Agrégations : trafic par heure, statistiques par serveur
5. Détection des anomalies sur des fenêtres d'une minute
"""
import time
from pyspark.sql import functions as F
from common import creer_session, SCHEMA, nettoyer, anomalies_ip, anomalies_serveur

ENTREE = "data/history/*.json"
SORTIE = "output"


def main():
    debut = time.time()
    spark = creer_session("batch-network-monitoring")

    # 1. Lecture (transformation : rien n'est encore calculé)
    brut = spark.read.schema(SCHEMA).json(ENTREE)

    # 2. Nettoyage + suppression des doublons
    logs = (nettoyer(brut)
            .dropDuplicates(["event_id"])
            .withColumn("date", F.to_date("ts")))
    logs.cache()   # réutilisé par plusieurs agrégations : on le garde en mémoire

    nb_brut = brut.count()          # action
    nb_propre = logs.count()        # action (remplit le cache)
    print(f"\nÉvénements lus : {nb_brut:,} | après nettoyage : {nb_propre:,}")

    # 3. Données propres en Parquet, partitionnées par date
    logs.write.mode("overwrite").partitionBy("date").parquet(f"{SORTIE}/clean_logs")

    # 4a. Trafic par heure
    trafic_heure = (logs.groupBy(F.date_trunc("hour", "ts").alias("heure"))
                        .agg(F.count("*").alias("nb_evenements"),
                             F.round(F.sum("bytes") / 1e6, 2).alias("volume_mo"),
                             F.round(F.avg("latency_ms"), 1).alias("latence_moy_ms"),
                             F.round(F.avg("is_error"), 3).alias("taux_erreur"))
                        .orderBy("heure"))
    trafic_heure.write.mode("overwrite").parquet(f"{SORTIE}/batch/trafic_par_heure")

    # 4b. Statistiques par serveur (dont la latence au 95e percentile)
    stats_serveurs = (logs.groupBy("dst_ip")
                          .agg(F.count("*").alias("nb_evenements"),
                               F.round(F.avg("latency_ms"), 1).alias("latence_moy_ms"),
                               F.round(F.percentile_approx("latency_ms", 0.95), 1).alias("latence_p95_ms"),
                               F.round(F.avg("is_error"), 3).alias("taux_erreur"))
                          .orderBy(F.desc("nb_evenements")))
    stats_serveurs.write.mode("overwrite").parquet(f"{SORTIE}/batch/stats_serveurs")

    # 5. Anomalies sur des fenêtres d'une minute
    anomalies = (anomalies_ip(logs, "1 minute")
                 .unionByName(anomalies_serveur(logs, "1 minute"))
                 .orderBy("debut_fenetre"))
    anomalies.write.mode("overwrite").parquet(f"{SORTIE}/batch/anomalies")

    # Résumé dans le terminal
    print("\nStatistiques par serveur :")
    stats_serveurs.show(truncate=False)
    print("Anomalies détectées par type :")
    anomalies.groupBy("type_anomalie").count().show()
    print("Exemples d'anomalies :")
    anomalies.show(10, truncate=False)
    print(f"Job terminé en {time.time() - debut:.1f} s. Résultats dans {SORTIE}/")
    spark.stop()


if __name__ == "__main__":
    main()
