"""
Générateur de logs réseau simulés (JSON Lines).

Deux modes :
  history : génère un historique (plusieurs jours) pour le traitement batch.
  stream  : écrit en continu un petit fichier par seconde pour le streaming.

Des anomalies sont injectées volontairement :
  - pic de trafic (type DDoS) depuis une IP
  - scan de ports (une IP qui tente beaucoup de ports différents)
  - hausse d'erreurs sur un serveur
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

SERVEURS = [f"10.0.0.{i}" for i in range(1, 6)]
CLIENTS = [f"192.168.1.{i}" for i in range(2, 60)]
IPS_SUSPECTES = ["203.0.113.66", "198.51.100.23"]
PORTS = [22, 53, 80, 443, 3306, 8080]
PROTOCOLES = ["TCP", "UDP"]


def evenement(ts, src_ip=None, dst_ip=None, dst_port=None, status=None):
    """Crée un événement réseau 'normal', éventuellement forcé sur certains champs."""
    latence = max(1.0, random.gauss(40, 15))
    st = status or random.choices(["OK", "ERROR", "TIMEOUT"], weights=[94, 4, 2])[0]
    if st == "TIMEOUT":
        latence = random.uniform(1000, 3000)
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(timespec="milliseconds"),
        "src_ip": src_ip or random.choice(CLIENTS),
        "dst_ip": dst_ip or random.choice(SERVEURS),
        "dst_port": dst_port or random.choice(PORTS),
        "protocol": random.choice(PROTOCOLES),
        "bytes": int(random.lognormvariate(7, 1)),
        "latency_ms": round(latence, 1),
        "status": st,
    }


def anomalies(ts):
    """Renvoie une rafale d'événements anormaux (ou une liste vide)."""
    r = random.random()
    if r < 0.02:   # pic de trafic depuis une IP suspecte
        ip = random.choice(IPS_SUSPECTES)
        return [evenement(ts, src_ip=ip, dst_ip="10.0.0.1", dst_port=443) for _ in range(random.randint(80, 150))]
    if r < 0.03:   # scan de ports
        ip = random.choice(IPS_SUSPECTES)
        return [evenement(ts, src_ip=ip, dst_port=p, status="ERROR") for p in random.sample(range(1, 10000), 60)]
    if r < 0.04:   # serveur en difficulté
        srv = random.choice(SERVEURS)
        return [evenement(ts, dst_ip=srv, status=random.choice(["ERROR", "TIMEOUT"])) for _ in range(40)]
    return []


def ecrire_fichier(dossier, events):
    """Écriture atomique : fichier temporaire puis renommage, pour que Spark ne lise jamais un fichier à moitié écrit."""
    os.makedirs(dossier, exist_ok=True)
    nom = f"logs_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
    tmp = os.path.join(dossier, "." + nom + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    os.replace(tmp, os.path.join(dossier, nom))


def mode_history(dossier, jours, events_par_minute):
    """Un fichier par jour, une minute simulée à la fois."""
    os.makedirs(dossier, exist_ok=True)
    fin = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t = fin - timedelta(days=jours)
    total, fichiers = 0, {}
    try:
        while t < fin:
            lot = [evenement(t + timedelta(seconds=random.uniform(0, 60)))
                   for _ in range(random.randint(events_par_minute // 2, events_par_minute))]
            lot += anomalies(t)
            jour = f"{t:%Y%m%d}"
            if jour not in fichiers:
                fichiers[jour] = open(os.path.join(dossier, f"history_{jour}.json"), "w", encoding="utf-8")
            for e in lot:
                fichiers[jour].write(json.dumps(e) + "\n")
            total += len(lot)
            t += timedelta(minutes=1)
    finally:
        for f in fichiers.values():
            f.close()
    print(f"Historique généré : {total} événements sur {jours} jour(s) dans {dossier}")


def mode_stream(dossier, events_par_seconde):
    print(f"Streaming en cours vers {dossier} (Ctrl+C pour arrêter)...")
    try:
        while True:
            maintenant = datetime.now(timezone.utc)
            lot = [evenement(maintenant) for _ in range(random.randint(events_par_seconde // 2, events_par_seconde))]
            lot += anomalies(maintenant)
            ecrire_fichier(dossier, lot)
            print(f"{maintenant:%H:%M:%S}  {len(lot):4d} événements")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Arrêt du générateur.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Générateur de logs réseau simulés")
    p.add_argument("mode", choices=["history", "stream"])
    p.add_argument("--jours", type=int, default=3, help="history : nombre de jours à générer")
    p.add_argument("--debit", type=int, default=50, help="événements max par minute (history) ou par seconde (stream)")
    args = p.parse_args()

    if args.mode == "history":
        mode_history("data/history", args.jours, args.debit)
    else:
        mode_stream("data/raw", args.debit)
