import os
import requests
from pprint import pprint
from dotenv import load_dotenv

load_dotenv()  # carga .env desde el directorio actual

API_KEY = os.getenv("SHIPSTATION_API_KEY")
API_SECRET = os.getenv("SHIPSTATION_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "Faltan SHIPSTATION_API_KEY o SHIPSTATION_API_SECRET en el .env")

url = "https://ssapi.shipstation.com/orders"
params = {
    "orderStatus": "awaiting_shipment",
    "tagId": "56240",
    "page": 1,
    "pageSize": 10
}

r = requests.get(url, params=params, auth=(API_KEY, API_SECRET), timeout=60)
r.raise_for_status()
data = r.json()

all_rows = []

for order in data.get("orders", []):
    rows = flatten_order_for_csv(order, tag_id="56240")
    all_rows.extend(rows)

print(f"Filas generadas: {len(all_rows)}")
print(all_rows[0])

print("keys:", list(data.keys()))
print("orders:", len(data.get("orders", [])))

if data.get("orders"):
    pprint(data["orders"][0])
