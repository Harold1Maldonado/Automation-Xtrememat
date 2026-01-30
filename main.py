import os
import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from shipstation_utils import flatten_order_for_csv, fetch_stores_map
from sftp_utils import sftp_upload


SHIPSTATION_ORDERS_URL = "https://ssapi.shipstation.com/orders"

# Columnas EXACTAS requeridas por el receptor (no agregar más)
CSV_COLUMNS = [
    "Order - Status",
    "Carrier - Service Sel",
    "Date - Order Date",
    "Date - Ship By Date",
    "Order - Number",
    "Item - SKU",
    "MFPN",
    "Item - Qty",
    "Item - Name",
    "Source",
    "Market - Store Name",
    "Order - Weight",
    "Service - Package Type",
]

TAG_NAME_MAP = {
    "56240": "GOLF",
    "56239": "CABINET",
}

# =========================
# LOGS CONFIGURATION
# =========================
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
EXPORT_DIR = BASE_DIR / "exports"
LOG_DIR.mkdir(exist_ok=True)
EXPORT_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / f"shipstation_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class ConfigError(RuntimeError):
    pass


def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise ConfigError(f"Missing required env var: {name}")
    return v


def fetch_orders(tag_id: str, page_size: int = 100, retries: int = 4) -> List[Dict[str, Any]]:

    api_key = _require_env("SHIPSTATION_API_KEY")
    api_secret = _require_env("SHIPSTATION_API_SECRET")

    page = 1
    orders_all: List[Dict[str, Any]] = []

    while True:
        params = {
            "orderStatus": "awaiting_shipment",
            "tagId": str(tag_id),
            "page": page,
            "pageSize": page_size,
        }

        logger.info(f"[{tag_id}] Fetch ShipStation page={page}")

        last_err: Optional[Exception] = None
        r = None

        for attempt in range(1, retries + 1):
            try:
                r = requests.get(
                    SHIPSTATION_ORDERS_URL,
                    params=params,
                    auth=(api_key, api_secret),
                    timeout=(10, 60),
                )

                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else (
                        2 ** attempt)
                    logger.warning(
                        f"[{tag_id}] Rate limited (429). Sleeping {sleep_s}s (attempt {attempt}/{retries})")
                    time.sleep(sleep_s)
                    continue

                r.raise_for_status()
                break
            except Exception as e:
                last_err = e
                sleep_s = min(30, 2 ** attempt)
                logger.warning(
                    f"[{tag_id}] ShipStation request failed (page={page}, attempt={attempt}/{retries}): {e}"
                )
                if attempt < retries:
                    time.sleep(sleep_s)
                else:
                    logger.exception(
                        f"[{tag_id}] ShipStation request failed permanently (page={page})")
                    raise

        assert r is not None
        data = r.json() if r.content else {}
        orders = data.get("orders", []) or []
        orders_all.extend(orders)

        logger.info(
            f"[{tag_id}] ShipStation response page={page} orders={len(orders)}")

        if len(orders) < page_size:
            break

        page += 1

    return orders_all


def write_csv(rows: List[Dict[str, Any]], filepath: Path) -> None:
    """
    Escribe CSV con columnas fijas EXACTAS. Cualquier clave extra se ignora.
    """
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})


def run_export(tag_id: str, remote_dir: str, stores_map: Dict[str, str]) -> Dict[str, Any]:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    tag_name = TAG_NAME_MAP.get(str(tag_id), str(tag_id))
    job_id = f"XTREME_{tag_name}_{ts}"
    csv_filename = f"{job_id}.csv"
    csv_path = EXPORT_DIR / csv_filename

    logger.info(f"[{tag_id}] Iniciando exportación job_id={job_id}")

    orders = fetch_orders(tag_id=tag_id)
    logger.info(f"[{tag_id}] Orders descargadas: {len(orders)}")

    all_rows: List[Dict[str, Any]] = []
    for order in orders:
        all_rows.extend(flatten_order_for_csv(order, stores_map=stores_map))

    if not all_rows:
        logger.info(f"[{tag_id}] No hay filas para exportar (no CSV)")
        return {"tag_id": tag_id, "csv": None, "rows": 0, "uploaded": False, "error": None}

    write_csv(all_rows, csv_path)
    logger.info(
        f"[{tag_id}] CSV creado local: {csv_path.name} (filas: {len(all_rows)})")

    try:
        sftp_upload(
            local_path=str(csv_path),
            remote_dir=remote_dir,
            retries=4,
            delay_sec=5,
            timeout_sec=15,
            ensure_dir=False,
            atomic=True,
        )
        logger.info(f"[{tag_id}] Subido por SFTP: {csv_path.name}")
        return {"tag_id": tag_id, "csv": str(csv_path), "rows": len(all_rows), "uploaded": True, "error": None}
    except Exception as e:
        logger.exception(
            f"[{tag_id}] ERROR subiendo por SFTP ({csv_path.name}): {e}")
        return {"tag_id": tag_id, "csv": str(csv_path), "rows": len(all_rows), "uploaded": False, "error": "SFTP upload failed"}


def main() -> None:
    load_dotenv()
    logger.info("==== Inicio ejecución ShipStation export ====")

    # Validación de env mínimos
    _require_env("SHIPSTATION_API_KEY")
    _require_env("SHIPSTATION_API_SECRET")
    remote_dir = _require_env("FTP_BASE_DIR")
    _require_env("FTP_HOST")
    _require_env("FTP_USER")
    _require_env("FTP_PASS")

    tag_golf = os.environ.get("TAG_GOLF", "56240")
    tag_cabinet = os.environ.get("TAG_CABINET", "56239")

    # Stores map (Store Name)
    try:
        stores_map = fetch_stores_map()
        logger.info(f"Stores cargadas: {len(stores_map)}")
    except Exception as e:
        # Si el receptor requiere store name, mejor fallar aquí.
        logger.exception(f"ERROR cargando stores desde ShipStation: {e}")
        raise SystemExit(1)

    results: List[Dict[str, Any]] = []
    results.append(run_export(tag_golf, remote_dir, stores_map))
    results.append(run_export(tag_cabinet, remote_dir, stores_map))

    any_upload_failed = any(r["csv"] and not r["uploaded"] for r in results)
    if any_upload_failed:
        logger.error("Una o más exportaciones fallaron")
        raise SystemExit(1)

    logger.info("==== Fin ejecución ShipStation export ====")


if __name__ == "__main__":
    main()
