"""
Job STREAMING (Spark Structured Streaming) : surveille data/raw en continu.

Chaque nouveau fichier écrit par le générateur est lu automatiquement.
Trois requêtes tournent en parallèle sur des fenêtres de 10 secondes :
  - métriques globales (volume, latence, taux d'erreur)
  - anomalies par IP source (pic de trafic, scan de ports)
  - anomalies par serveur (taux d'erreur anormal)

Le watermark indique à Spark combien de temps attendre les événements en retard
avant de considérer une fenêtre comme terminée et de l'écrire (mode append).
"""
from common import creer_session, SCHEMA, nettoyer, stats_globales, anomalies_ip, anomalies_serveur

ENTREE = "data/raw"
SORTIE = "output/stream"
CHECKPOINTS = "checkpoints"
FENETRE = "10 seconds"
WATERMARK = "10 seconds"
DECLENCHEMENT = "5 seconds"


def lancer(df, nom, console=False):
    """Écrit un flux agrégé en Parquet (et optionnellement dans la console)."""
    requete = (df.writeStream
                 .outputMode("append")
                 .format("parquet")
                 .option("path", f"{SORTIE}/{nom}")
                 .option("checkpointLocation", f"{CHECKPOINTS}/{nom}")
                 .trigger(processingTime=DECLENCHEMENT)
                 .queryName(nom)
                 .start())
    if console:
        (df.writeStream
           .outputMode("append")
           .format("console")
           .option("truncate", "false")
           .option("checkpointLocation", f"{CHECKPOINTS}/{nom}_console")
           .trigger(processingTime=DECLENCHEMENT)
           .start())
    return requete


def main():
    spark = creer_session("streaming-network-monitoring")

    flux = (spark.readStream
                 .schema(SCHEMA)                    # obligatoire en streaming
                 .option("maxFilesPerTrigger", 50)  # limite le nombre de fichiers par micro-lot
                 .json(ENTREE))

    logs = nettoyer(flux).withWatermark("ts", WATERMARK)

    lancer(stats_globales(logs, FENETRE), "metriques")
    lancer(anomalies_ip(logs, FENETRE), "anomalies_ip", console=True)
    lancer(anomalies_serveur(logs, FENETRE), "anomalies_serveur", console=True)

    print("Streaming démarré. Les anomalies s'affichent ci-dessous (Ctrl+C pour arrêter).")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
