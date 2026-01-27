import os
import csv
from datetime import datetime
import requests
from dotenv import load_dotenv

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


def run_export(tag_id: str, remote_dir: str):
    orders = fetch_orders(tag_id=tag_id)
    all_rows = []

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    job_id = f"XTREME_{tag_id}_{ts}"
    csv_filename = f"{job_id}.csv"

    for order in orders:
        all_rows.extend(
            flatten_order_for_csv(
                order,
                tag_id=str(tag_id),
                job_id=job_id
            )
        )

    if not all_rows:
        print(f"[{tag_id}] No hay filas para exportar.")
        return

    write_csv(all_rows, csv_filename)
    sftp_upload(csv_filename, remote_dir)

    print(f"[{tag_id}] Exportado: {csv_filename} (filas: {len(all_rows)})")


def main():
    load_dotenv()

    remote_dir = os.environ["FTP_BASE_DIR"]

    tag_golf = os.environ.get("TAG_GOLF", "56240")
    tag_cabinet = os.environ.get("TAG_CABINET", "56239")

    run_export(tag_golf, remote_dir)
    run_export(tag_cabinet, remote_dir)


if __name__ == "__main__":
    main()
