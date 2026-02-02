import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


SHIPSTATION_STORES_URL = "https://ssapi.shipstation.com/stores"


def fetch_stores_map(retries: int = 4) -> Dict[str, str]:

    api_key = os.environ["SHIPSTATION_API_KEY"]
    api_secret = os.environ["SHIPSTATION_API_SECRET"]

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                SHIPSTATION_STORES_URL,
                auth=(api_key, api_secret),
                timeout=(10, 60),
            )
            # 429 handling
            if r.status_code == 429:
                sleep_s = min(30, 2 ** attempt)
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_s = int(retry_after)
                time_to_sleep = max(1, sleep_s)
                # no logging here to keep this file decoupled; caller logs
                import time
                time.sleep(time_to_sleep)
                continue

            r.raise_for_status()
            stores = r.json() or []
            return {str(s.get("storeId")): (s.get("storeName") or "") for s in stores}
        except Exception as e:
            last_err = e
            if attempt < retries:
                import time
                time.sleep(min(30, 2 ** attempt))
            else:
                raise last_err

    return {}


def _parse_ss_dt(s: str) -> Optional[datetime]:

    if not s:
        return None
    s = s.strip()
    if "." in s:
        left, right = s.split(".", 1)
        digits = "".join(ch for ch in right if ch.isdigit())
        digits = (digits + "000000")[:6]
        s = f"{left}.{digits}"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _fmt_dt_for_csv(s: Optional[str]) -> str:
    dt = _parse_ss_dt(s or "")
    if not dt:
        return ""

    hour12 = dt.hour % 12
    if hour12 == 0:
        hour12 = 12
    ampm = "AM" if dt.hour < 12 else "PM"
    return f"{dt.month}/{dt.day}/{dt.year} {hour12}:{dt.minute:02d}:{dt.second:02d} {ampm}"


def _title_status(s: str) -> str:
    s = (s or "").strip()
    return s.replace("_", " ").title()


def _package_type(order: Dict[str, Any]) -> str:
    code = (order.get("packageCode") or "").strip()
    if not code:
        return ""
    return code.replace("_", " ").title()


def _weight_value(order: Dict[str, Any]) -> str:
    """
    Mantiene el valor tal cual viene en order.weight.value.
    En tu ejemplo es 'ounces' (77.2). Si necesitas convertir a lb, dímelo y lo ajusto.
    """
    w = order.get("weight") or {}
    val = w.get("value")
    if val is None:
        return ""
    return str(val)


def human_service(order: Dict[str, Any]) -> str:
    """
    Para que coincida con el Excel:
    - Prioriza requestedShippingService si contiene una descripción humana útil (como "UPS Ground")
    - Fallback a mapping por serviceCode
    """
    requested = (order.get("requestedShippingService") or "").strip()
    upper_req = requested.upper()

    if "UPS" in upper_req and "GROUND" in upper_req:
        return "UPS® Ground"

    service_code = (order.get("serviceCode") or "").strip().lower()
    carrier_code = (order.get("carrierCode") or "").strip().lower()

    SERVICE_MAP = {
        "ups_ground": "UPS® Ground",
        "ups_2nd_day_air": "UPS 2nd Day Air",
        "ups_next_day_air": "UPS Next Day Air",
        "usps_parcel_select": "USPS Parcel Select",
        "usps_parcel_select_ground": "USPS Parcel Select Ground",
        "usps_priority_mail": "USPS Priority Mail",
    }

    if service_code in SERVICE_MAP:
        return SERVICE_MAP[service_code]

    # "stamps_com + usps_parcel_select" -> "STAMPS_COM Usps Parcel Select"
    if carrier_code and service_code:
        pretty = service_code.replace("_", " ").title()
        return f"{carrier_code.upper()} {pretty}".strip()

    return requested


def flatten_order_for_csv(order: Dict[str, Any], stores_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Devuelve una fila por item con EXACTAMENTE las columnas requeridas por el receptor.
    """
    adv = order.get("advancedOptions") or {}
    source = adv.get("source") or ""

    store_id = str(adv.get("storeId") or order.get("storeId") or "")
    store_name = ""
    if store_id:
        store_name = stores_map.get(store_id, "")

    order_number = order.get("orderNumber") or ""
    order_status = _title_status(order.get("orderStatus") or "")
    order_date = _fmt_dt_for_csv(order.get("orderDate"))
    ship_by_date = _fmt_dt_for_csv(order.get("shipByDate"))
    carrier_service = human_service(order)
    order_weight = _weight_value(order)
    package_type = _package_type(order)

    rows: List[Dict[str, Any]] = []
    items = order.get("items") or []

    for item in items:
        raw_qty = item.get("quantity", 0)
        try:
            qty = int(float(raw_qty))
        except (TypeError, ValueError):
            qty = 0

        sku = item.get("sku") or ""
        name = item.get("name") or ""

        #  MFPN warehouseLocation (RFM06/RFMPB)
        mfpn = item.get("warehouseLocation") or item.get(
            "fulfillmentSku") or ""

        rows.append({
            "Order - Status": order_status,
            "Carrier - Service Sel": carrier_service,
            "Date - Order Date": order_date,
            "Date - Ship By Date": ship_by_date,
            "Order - Number": order_number,
            "Item - SKU": sku,
            "MFPN": mfpn,
            "Item - Qty": qty,
            "Item - Name": name,
            "Source": source,
            "Market - Store Name": store_name,
            "Order - Weight": order_weight,
            "Service - Package Type": package_type,
        })

    return rows
