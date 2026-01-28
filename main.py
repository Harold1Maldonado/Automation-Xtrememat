import os
import csv
from datetime import datetime
import requests
from dotenv import load_dotenv
import logging
from pathlib import Path

from shipstation_utils import flatten_order_for_csv
from sftp_utils import sftp_upload


SHIPSTATION_URL = "https://ssapi.shipstation.com/orders"

CSV_COLUMNS = [
    "JobID",
    "Order - Number",
    "Order - Channel",
    "BoxContent",
    "MFPN",
    "Item - SKU",
    "FulfillableQty",
    "Carrier - Service Requested",

    # Campos adicionales
    "Fulfillment SKU",
    "Warehouse Location",
    "UPC",
    "Item Name",
    "Product ID",

    # Auditoría
    "tagId",
    "orderId",
    "orderItemId",
]

# =========================
# LOGGING CONFIG
# =========================
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / f"shipstation_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def fetch_orders(tag_id: str, page_size: int = 100) -> list[dict]:
    api_key = os.environ["SHIPSTATION_API_KEY"]
    api_secret = os.environ["SHIPSTATION_API_SECRET"]

    page = 1
    orders_all = []

    while True:
        params = {
            "orderStatus": "awaiting_shipment",
            "tagId": str(tag_id),
            "page": page,
            "pageSize": page_size,
        }

        r = requests.get(
            SHIPSTATION_URL,
            params=params,
            auth=(api_key, api_secret),
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        orders = data.get("orders", [])
        orders_all.extend(orders)

        if len(orders) < page_size:
            break

        page += 1

    return orders_all


def write_csv(rows: list[dict], filename: str):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})


def run_export(tag_id: str, remote_dir: str) -> dict:
    """
    Genera el CSV (si hay filas) y luego intenta subirlo.
    Nunca lanza excepción hacia afuera: devuelve un resumen para logging/alertas.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    job_id = f"XTREME_{tag_id}_{ts}"
    csv_filename = f"{job_id}.csv"

    logger.info(f"[{tag_id}] Iniciando exportación")

    orders = fetch_orders(tag_id=tag_id)
    all_rows = []
    for order in orders:
        all_rows.extend(
            flatten_order_for_csv(order, tag_id=str(tag_id), job_id=job_id)
        )

    if not all_rows:
        logger.info(f"[{tag_id}] No hay filas para exportar")
        return {"tag_id": tag_id, "csv": None, "rows": 0, "uploaded": False, "error": None}

    write_csv(all_rows, csv_filename)
    logger.info(
        f"[{tag_id}] CSV creado local: {csv_filename} (filas: {len(all_rows)})")

    try:
        sftp_upload(csv_filename, remote_dir)
        logger.info(f"[{tag_id}] Subido por SFTP: {csv_filename}")
        return {
            "tag_id": tag_id,
            "csv": csv_filename,
            "rows": len(all_rows),
            "uploaded": True,
            "error": None
        }
    except Exception:
        logger.exception(
            f"[{tag_id}] ERROR subiendo por SFTP ({csv_filename})")
        return {
            "tag_id": tag_id,
            "csv": csv_filename,
            "rows": len(all_rows),
            "uploaded": False,
            "error": "SFTP upload failed"
        }


def main():
    load_dotenv()
    logger.info("==== Inicio ejecución ShipStation export ====")

    remote_dir = os.environ["FTP_BASE_DIR"]

    tag_golf = os.environ.get("TAG_GOLF", "56240")
    tag_cabinet = os.environ.get("TAG_CABINET", "56239")

    results = []
    results.append(run_export(tag_golf, remote_dir))
    results.append(run_export(tag_cabinet, remote_dir))

    any_upload_failed = any(r["csv"] and not r["uploaded"] for r in results)
    if any_upload_failed:
        logger.error("Una o más exportaciones fallaron")
        raise SystemExit(1)

    logger.info("==== Fin ejecución ShipStation export ====")


if __name__ == "__main__":
    main()
