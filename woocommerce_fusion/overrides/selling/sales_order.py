#/apps/woocommerce_fusion/woocommerce_fusion/overrides/selling
import json

import frappe
from datetime import datetime
from erpnext.selling.doctype.sales_order.sales_order import SalesOrder
from frappe import _
from frappe.model.naming import get_default_naming_series, make_autoname

from woocommerce_fusion.tasks.sync_sales_orders import run_sales_order_sync
from woocommerce_fusion.woocommerce.woocommerce_api import (
    generate_woocommerce_record_name_from_domain_and_id,
)


class CustomSalesOrder(SalesOrder):
    """
    This class extends ERPNext's Sales Order doctype to override the autoname method
    This allows us to name the Sales Order conditionally.

    We also add logic to set the WooCommerce Status field on validate.
    """

    def autoname(self):
        """
        Always use ERPNext's default naming series, regardless of WooCommerce linkage.
        """
        naming_series = get_default_naming_series("Sales Order")
        self.name = make_autoname(key=naming_series)

    def on_change(self):
        """
        This is called when a document's values has been changed (including db_set).
        """
        # If Sales Order Status Sync is enabled, update the WooCommerce status of the Sales Order
        if self.custom_woocommerce_order_id and self.woocommerce_server:
            wc_server = frappe.get_cached_doc("WooCommerce Server", self.woocommerce_server)
            if wc_server.enable_so_status_sync:
                mapping = next(
                    (
                        row
                        for row in wc_server.sales_order_status_map
                        if row.erpnext_sales_order_status == self.status
                    ),
                    None,
                )
                if mapping:
                    if self.woocommerce_status != mapping.woocommerce_sales_order_status:
                        frappe.db.set_value(
                            "Sales Order", self.name, "woocommerce_status", mapping.woocommerce_sales_order_status
                        )
                        frappe.enqueue(run_sales_order_sync, queue="long", sales_order_name=self.name)


@frappe.whitelist()
def get_woocommerce_order_shipment_trackings(sales_order_name):
    so = frappe.get_doc('Sales Order', sales_order_name)
    if not so.custom_woocommerce_order_id or not so.woocommerce_server:
        return []

    wc_order = get_woocommerce_order(so.woocommerce_server, so.custom_woocommerce_order_id)
    meta_data_str = wc_order.meta_data or '[]'  # JSON string field
    try:
        meta_data = json.loads(meta_data_str)
    except json.JSONDecodeError:
        return []  # Fallback if invalid JSON

    trackings = []
    for meta in meta_data:
        if meta.get('key') == '_wc_shipment_tracking_items':
            shipment_items = meta.get('value', [])  # Already a list
            for item in shipment_items:
                date_shipped_unix = item.get('date_shipped', '')
                date_shipped = ''
                if date_shipped_unix:
                    try:
                        date_shipped = datetime.fromtimestamp(int(date_shipped_unix)).strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        pass  # Keep empty if invalid

                provider = item.get('tracking_provider') or item.get('custom_tracking_provider') or 'Unknown'
                number = item.get('tracking_number', '')
                link = item.get('custom_tracking_link') or ''

                if not link and number:
                    if number.startswith('1Z') and len(number) == 18:
                        provider = 'UPS' if provider == 'Unknown' else provider
                        link = f'https://www.ups.com/track?tracknum={number}'
                    # Add other patterns as needed

                if number:  # Skip empty entries
                    trackings.append({
                        'date_shipped': date_shipped,
                        'tracking_provider': provider,
                        'tracking_number': number,
                        'tracking_link': link
                    })
            break

    return trackings


@frappe.whitelist()
def update_woocommerce_order_shipment_trackings(doc, shipment_trackings):
    """
    Updates the shipment tracking details of a specific WooCommerce order.
    """
    doc = frappe._dict(json.loads(doc))
    if doc.woocommerce_server and doc.custom_woocommerce_order_id:
        wc_order = get_woocommerce_order(doc.woocommerce_server, doc.custom_woocommerce_order_id)
    wc_order.shipment_trackings = shipment_trackings
    wc_order.save()
    return wc_order.shipment_trackings


def get_woocommerce_order(woocommerce_server, custom_woocommerce_order_id):
    """
    Retrieves a specific WooCommerce order based on its site and ID.
    """
    # First verify if the WooCommerce site exits, and it sync is enabled
    wc_order_name = generate_woocommerce_record_name_from_domain_and_id(
        woocommerce_server, custom_woocommerce_order_id
    )
    wc_server = frappe.get_cached_doc("WooCommerce Server", woocommerce_server)

    if not wc_server:
        frappe.throw(
            _(
                "This Sales Order is linked to WooCommerce site '{0}', but this site can not be found in 'WooCommerce Servers'"
            ).format(woocommerce_server)
        )

    if not wc_server.enable_sync:
        frappe.throw(
            _(
                "This Sales Order is linked to WooCommerce site '{0}', but Synchronisation for this site is disabled in 'WooCommerce Server'"
            ).format(woocommerce_server)
        )

    wc_order = frappe.get_doc({"doctype": "WooCommerce Order", "name": wc_order_name})
    wc_order.load_from_db()
    return wc_order
