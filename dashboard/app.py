"""
Tableau de bord Streamlit : lit les résultats Parquet produits par Spark.
Lancement : streamlit run dashboard/app.py
"""
import os
import time
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Surveillance réseau – Spark", page_icon="📡", layout="wide")


def lire(chemin):
    """Lit un dossier Parquet écrit par Spark ; renvoie un DataFrame vide s'il n'existe pas encore."""
    if not os.path.isdir(chemin):
        return pd.DataFrame()
    try:
        return pd.read_parquet(chemin)
    except Exception:
        return pd.DataFrame()


st.title("📡 Surveillance réseau en temps réel")
st.caption("Pipeline PySpark : traitement batch de l'historique + Structured Streaming sur les nouveaux logs")

onglet_rt, onglet_batch = st.tabs(["Temps réel", "Historique (batch)"])

# ---------------- Temps réel ----------------
with onglet_rt:
    metriques = lire("output/stream/metriques")
    anomalies_rt = pd.concat([lire("output/stream/anomalies_ip"),
                              lire("output/stream/anomalies_serveur")], ignore_index=True)

    if metriques.empty:
        st.info("Aucune donnée temps réel pour l'instant : lance le générateur (mode stream) et le job streaming, "
                "puis patiente 20 à 30 secondes.")
    else:
        metriques = metriques.sort_values("debut_fenetre").tail(60)   # ~10 dernières minutes
        derniere = metriques.iloc[-1]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Événements (10 s)", f"{int(derniere['nb_evenements']):,}")
        c2.metric("Latence moyenne", f"{derniere['latence_moy_ms']:.0f} ms")
        c3.metric("Taux d'erreur", f"{derniere['taux_erreur']:.1%}")
        c4.metric("Anomalies détectées", len(anomalies_rt))

        serie = metriques.set_index("debut_fenetre")
        g1, g2 = st.columns(2)
        g1.subheader("Événements par fenêtre de 10 s")
        g1.line_chart(serie["nb_evenements"])
        g2.subheader("Taux d'erreur")
        g2.line_chart(serie["taux_erreur"])

    st.subheader("Dernières anomalies")
    if anomalies_rt.empty:
        st.write("Aucune anomalie pour le moment.")
    else:
        st.dataframe(anomalies_rt.sort_values("debut_fenetre", ascending=False).head(20),
                     use_container_width=True, hide_index=True)

# ---------------- Historique ----------------
with onglet_batch:
    trafic = lire("output/batch/trafic_par_heure")
    serveurs = lire("output/batch/stats_serveurs")
    anomalies_b = lire("output/batch/anomalies")

    if trafic.empty:
        st.info("Lance d'abord le job batch : python src/batch_job.py")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Événements traités", f"{int(trafic['nb_evenements'].sum()):,}")
        c2.metric("Volume total", f"{trafic['volume_mo'].sum():,.0f} Mo")
        c3.metric("Anomalies détectées", len(anomalies_b))

        st.subheader("Trafic par heure")
        st.line_chart(trafic.sort_values("heure").set_index("heure")["nb_evenements"])

        g1, g2 = st.columns(2)
        g1.subheader("Statistiques par serveur")
        g1.dataframe(serveurs, use_container_width=True, hide_index=True)
        if not anomalies_b.empty:
            g2.subheader("Anomalies par type")
            g2.bar_chart(anomalies_b["type_anomalie"].value_counts())

# Rafraîchissement automatique
if st.sidebar.toggle("Rafraîchissement auto (5 s)", value=True):
    time.sleep(5)
    st.rerun()
