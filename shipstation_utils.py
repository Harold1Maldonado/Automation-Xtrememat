def human_service(order: dict) -> str:
    carrier = (order.get("carrierCode") or "").strip().upper()
    service_code = (order.get("serviceCode") or "").strip().lower()

    SERVICE_MAP = {
        "ups_ground": "UPS Ground",
        "ups_2nd_day_air": "UPS 2nd Day Air",
        "ups_next_day_air": "UPS Next Day Air",
        "usps_parcel_select_ground": "USPS Parcel Select Ground",
        "usps_priority_mail": "USPS Priority Mail",
    }

    if service_code in SERVICE_MAP:
        return SERVICE_MAP[service_code]

    if carrier and service_code:
        pretty = service_code.replace("_", " ").title()
        return f"{carrier} {pretty}".strip()

    return (order.get("requestedShippingService") or "").strip()


def flatten_order_for_csv(order: dict, tag_id: str, job_id: str) -> list[dict]:
    order_number = order.get("orderNumber", "")
    order_channel = (order.get("advancedOptions", {})
                     or {}).get("source", "") or ""
    carrier_service_requested = human_service(order)

    rows = []
    for item in order.get("items", []):
        sku = item.get("sku", "") or ""
        qty = int(item.get("quantity") or 0)

        rows.append({
            "JobID": job_id,
            "Order - Number": order_number,
            "Order - Channel": order_channel,

            "BoxContent": sku,
            "MFPN": item.get("warehouseLocation") or item.get("fulfillmentSku") or "",
            "Item - SKU": sku,
            "FulfillableQty": qty,
            "Carrier - Service Requested": carrier_service_requested,


            "Fulfillment SKU": item.get("fulfillmentSku") or "",
            "Warehouse Location": item.get("warehouseLocation") or "",
            "UPC": item.get("upc") or "",
            "Item Name": item.get("name") or "",
            "Product ID": item.get("productId") or "",


            "tagId": str(tag_id),
            "orderId": str(order.get("orderId", "")),
            "orderItemId": str(item.get("orderItemId", "")),
        })

    return rows
