import requests


def geocode(adresse):
    """
    Convertit une adresse en coordonnées
    """
    url = "https://nominatim.openstreetmap.org/search"
    r = requests.get(
        url,
        params={"q": adresse, "format": "json", "limit": 1},
        headers={"User-Agent": "mon-app"},
        timeout=(3, 10),
    )
    result = r.json()[0]
    return float(result["lat"]), float(result["lon"])


# Exemple d'utilisation (build sur andorre pour un premier test rapide) :
lat1, lon1 = geocode("Andorra la Vella, Andorra")
lat2, lon2 = geocode("Escaldes-Engordany, Andorra")

# Deux points en Andorra (coordonnées)
payload = {
    "locations": [
        {"lat": lat1, "lon": lon1},
        {"lat": lat2, "lon": lon2},
    ],
    "costing": "auto",
    "directions_options": {"language": "fr-FR"},  # instructions en français
}

response = requests.post("http://localhost:8002/route", json=payload, timeout=(3, 10))
data = response.json()

print(data)

trip = data["trip"]
summary = trip["summary"]

print(f"Distance : {summary['length']:.2f} km")
print(f"Durée    : {summary['time'] // 60:.0f} min {summary['time'] % 60:.0f} sec")

# Instructions de navigation
for leg in trip["legs"]:
    for maneuver in leg["maneuvers"]:
        print(f"  → {maneuver['instruction']}")
