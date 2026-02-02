import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple, Union
import pytz

import frappe
import hashlib
import re
from erpnext.selling.doctype.sales_order.sales_order import SalesOrder
from erpnext.selling.doctype.sales_order_item.sales_order_item import SalesOrderItem
from erpnext.selling.doctype.sales_order.sales_order import make_sales_invoice
from erpnext.accounts.utils import get_balance_on
from frappe import _
from frappe.utils import get_datetime, now, add_days
from frappe.utils.data import cstr, now
from jsonpath_ng.ext import parse
from woocommerce import API
from woocommerce_fusion.exceptions import SyncDisabledError, WooCommerceOrderNotFoundError
from woocommerce_fusion.tasks.sync import SynchroniseWooCommerce
from woocommerce_fusion.tasks.sync_items import run_item_sync
from woocommerce_fusion.woocommerce.doctype.woocommerce_order.woocommerce_order import (
    WC_ORDER_STATUS_MAPPING,
    WC_ORDER_STATUS_MAPPING_REVERSE,
    WooCommerceOrder,
)
from woocommerce_fusion.woocommerce.woocommerce_api import (
    generate_woocommerce_record_name_from_domain_and_id,
)
import time
from frappe.exceptions import QueryTimeoutError

class WooCommerceMissingItemError(frappe.ValidationError):
    def __init__(self, woocomm_item_id, woocommerce_server, product_name, order_id):
        self.woocomm_item_id = woocomm_item_id
        self.woocommerce_server = woocommerce_server
        self.product_name = product_name
        self.order_id = order_id
        msg = f"No matching item found in ERPNext for WooCommerce product ID {woocomm_item_id} ({product_name}) on server {woocommerce_server}"
        super().__init__(msg)
        
def to_proper_case(text: str) -> str:
    if not text:
        return text
    # Split into words, title case each, join back
    words = text.split()
    proper_words = []
    for word in words:
        # Handle special cases like 'PO' for PO Box, or state codes
        if len(word) == 2 and word.isupper(): # Likely state code like 'NV'
            proper_words.append(word) # Keep uppercase
        else:
            proper_words.append(word.title())
    proper_text = ' '.join(proper_words)
    # Handle Mc/Mac prefixes (e.g., "Mcdonald" → "McDonald")
    proper_text = re.sub(r"\b(Mac|Mc)(\w)", lambda m: m.group(1) + m.group(2).upper(), proper_text)
    # Lower 'S to 's for possessives (e.g., "Nelson'S" → "Nelson's")
    proper_text = re.sub(r"'S\b", r"'s", proper_text)
    # Handle common business suffixes to all uppercase (e.g., "Llc" → "LLC")
    proper_text = re.sub(r"\b(llc|inc|corp|ltd|co|usa)\b", lambda m: m.group(0).upper(), proper_text, flags=re.IGNORECASE)
    return proper_text

def clean_company_name(company_name: str) -> str:
    """
    Filter out known/predictable acronyms or fillers from company name.
    Returns the cleaned company name or empty string if it's a filler.
    """
    invalid_fillers = {
        "N/A", "NA", "NONE", "NOT APPLICABLE", "UNKNOWN", "TBD", "N.A.", "N.A"
    }
    normalized = company_name.strip().upper()
    if normalized in invalid_fillers:
        return ""
    return company_name

def normalize_phone(phone: str) -> str:
    """
    Normalize phone number by removing all non-digit characters.
    """
    if not phone:
        return ""
    return re.sub(r'\D', '', phone)

def clean_phone_number(phone: str, country: str = "US") -> str:
    """
    Clean and format phone number to modern standards.
    For US numbers, format as (XXX) XXX-XXXX, removing country code 1 if present.
    For non-US, return stripped original.
    """
    if not phone:
        return ""
    
    if country.upper() != "US":
        return phone.strip()
    
    digits = re.sub(r'\D', '', phone)
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]
    
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    
    return phone.strip()
        
def run_sales_order_sync_from_hook(doc, method):
    if (
        doc.doctype == "Sales Order"
        and not doc.flags.get("created_by_sync", None)
        and doc.woocommerce_server
    ):
        frappe.enqueue(run_sales_order_sync, queue="long", sales_order_name=doc.name)

def get_address_title(address_name):
    return frappe.db.get_value("Address", address_name, "address_title") if address_name else ''

@frappe.whitelist()
def run_sales_order_sync(
    sales_order_name: Optional[str] = None,
    sales_order: Optional[SalesOrder] = None,
    woocommerce_order_name: Optional[str] = None,
    woocommerce_order: Optional[WooCommerceOrder] = None,
    enqueue=False,
):
    """
    Helper funtion that prepares arguments for order sync
    """
    # Validate inputs, at least one of the parameters should be provided
    if not any([sales_order_name, sales_order, woocommerce_order_name, woocommerce_order]):
        raise ValueError(
            "At least one of sales_order_name, sales_order, woocommerce_order_name, woocommerce_order is required"
        )

    # Get ERPNext Sales Order and WooCommerce Order if they exist
    if woocommerce_order or woocommerce_order_name:
        if not woocommerce_order:
            woocommerce_order = frappe.get_doc(
                {"doctype": "WooCommerce Order", "name": woocommerce_order_name}
            )
            woocommerce_order.load_from_db()

        # Trigger sync
        sync = SynchroniseSalesOrder(woocommerce_order=woocommerce_order)
        if enqueue:
            frappe.enqueue(sync.run)
        else:
            sync.run()

    elif sales_order_name or sales_order:
        if not sales_order:
            sales_order = frappe.get_doc("Sales Order", sales_order_name)
        if not sales_order.woocommerce_server:
            frappe.throw(_("No WooCommerce Server defined for Sales Order {0}").format(sales_order_name))
        # Trigger sync for every linked server
        sync = SynchroniseSalesOrder(sales_order=sales_order)
        if enqueue:
            frappe.enqueue(sync.run)
        else:
            sync.run()

    return (
        sync.sales_order if sync else None,
        sync.woocommerce_order if sync else None,
    )

def safe_insert(doc, retries=3, base_sleep=0.7):
    for i in range(retries):
        try:
            return doc.insert()
        except QueryTimeoutError as e:
            if "1205" in str(e):  # MySQL lock timeout
                frappe.db.rollback()
                time.sleep(base_sleep * (i + 1))
                continue
            raise
        
def safe_submit(doc, retries=3, base_sleep=0.7):
    for i in range(retries):
        try:
            doc.submit()
            return doc
        except frappe.QueryDeadlockError as e:
            frappe.db.rollback()
            time.sleep(base_sleep * (i + 1))
            doc.load_from_db()  # Reload to ensure fresh state before retry
            continue
        raise

def safe_get_wc_orders(date_time_from=None, status=None):
    try:
        return get_list_of_wc_orders(date_time_from=date_time_from, status=status)
    except Exception as e:
        if "DNS" in str(e) or "NameResolutionError" in str(e) or "Temporary failure in name resolution" in str(e):
            frappe.log_error(message=f"Skipped WC fetch for status={status}: {str(e)}", title="WC DNS Skip")
            return []
        raise
    
"""
frappe.call("woocommerce_fusion.tasks.sync_sales_orders.sync_woocommerce_orders_modified_since")
"""
# Primary method to sync (both create/import and update) sales orders from woocommerce to erpnext
def sync_woocommerce_orders_modified_since(date_time_from=None):
    """
    Get list of WooCommerce orders modified since date_time_from
    """
    wc_settings = frappe.get_doc("WooCommerce Integration Settings")

    if not date_time_from:
        date_time_from = wc_settings.wc_last_sync_date

    # Validate
    if not date_time_from:
        error_text = _(
            "'Last Items Syncronisation Date' field on 'WooCommerce Integration Settings' is missing"
        )
        frappe.log_error(
            "WooCommerce Items Sync Task Error",
            error_text,
        )
        raise ValueError(error_text)
    
    lock_key = "wc_order_sync_lock"
    if frappe.cache().get_value(lock_key):
        frappe.log_error(message="Batch sync already in progress (lock held).", title="WC Order Sync Skipped")
        return  # Skip if another sync is running
    
    try:
        frappe.cache().set_value(lock_key, True, expires_in_sec=1800)  # Lock for 30 minutes max
        """
        #ADDITIONAL STATUSES CAN BE ADDED, BUT...
        1) WC_ORDER_STATUS_MAPPING within 'woocommerce_order.py' must be updated
        2) 'options' for "name": "Sales Order-woocommerce_status" within custom_field.json (along with the status list in the sales order UI under "connections")
            -> for reference after updating custom_field.json, the fields will persist after running bench --site erp.caseclub.com migrate (otherwise they will be erased after each migrate)
        """
        # These are the type of woocommerce orders to import into erpnext (based on woocommerce status)
        wc_orders = safe_get_wc_orders(date_time_from=date_time_from, status="processing")
        wc_orders += safe_get_wc_orders(date_time_from=date_time_from, status="completed")
        wc_orders += safe_get_wc_orders(date_time_from=date_time_from, status="fba-hold")
        wc_orders += safe_get_wc_orders(date_time_from=date_time_from, status="sent-to-fba")
        wc_orders += safe_get_wc_orders(date_time_from=date_time_from, status="fail-to-fba")
        wc_orders += safe_get_wc_orders(date_time_from=date_time_from, status="portal")

        #print(wc_orders)
        
        all_successful = True  # Flag to track if all orders synced without issues

        for wc_order in wc_orders:
            try:
                if wc_order.status == "portal":
                    success = process_portal_payment(wc_order)
                    if not success:
                        all_successful = False
                else:
                    sales_order, _ = run_sales_order_sync(woocommerce_order=wc_order, enqueue=False)
                    if sales_order is None:
                        all_successful = False
                frappe.db.commit()
            except Exception:
                all_successful = False
                pass

        # Only update sync date if all orders in the batch succeeded
        if all_successful:
            # Update using the loaded doc to avoid Doctype mismatch
            wc_settings.wc_last_sync_date = now()
            wc_settings.flags.ignore_permissions = True  # If needed for automated saves
            wc_settings.save()
            frappe.db.commit()  # Ensure commit
    finally:
        frappe.cache().delete_value(lock_key)  # Release lock

"""
frappe.call("woocommerce_fusion.tasks.sync_sales_orders.sync_erpnext_to_woocommerce_orders")
"""
# Primary method to update orders from erpnext to woocommerce (it is assumed erpnext is the primary source of truth)
def sync_erpnext_to_woocommerce_orders():
    """
    Hourly sync: Fetch WooCommerce orders in 'processing' status,
    compare to linked ERPNext Sales Orders, and update WC status if they don't match.
    """
    # Pull woocommerce orders within the last year, ensure no long term bloat
    date_time_from = add_days(now(), -365)  # Last year
    
    wc_orders = get_list_of_wc_orders(date_time_from=date_time_from, status="processing")
    for wc_order in wc_orders:
        try:
            # Find linked ERPNext SO (via custom_woocommerce_order_id and woocommerce_server)
            sales_order = None
            filters = [
                ["Sales Order", "custom_woocommerce_order_id", "=", wc_order.id],
                ["Sales Order", "woocommerce_server", "=", wc_order.woocommerce_server],
                ["Sales Order", "docstatus", "!=", 2]  # Not cancelled
            ]
            so_names = frappe.get_all("Sales Order", filters=filters, pluck="name")
            if so_names:
                sales_order = frappe.get_doc("Sales Order", so_names[0])

            if not sales_order:
                # No linked SO; skip or log
                #frappe.log_error(f"No ERPNext SO found for WC Order {wc_order.id}", "WooCommerce Sync Skip")
                continue

            # Map ERPNext status to WC equivalent (customize this dict as needed)
            ERP_STATUS_TO_WC_MAPPING = {
                "Completed": "completed",
                "On Hold": "on-hold",  # Example for future expansion
                "To Deliver": "processing",
                "To Bill": "processing",
                "Cancelled": "cancelled"
            }
            erpnext_wc_status = ERP_STATUS_TO_WC_MAPPING.get(sales_order.status, wc_order.status)  # Default to no change

            wc_order_dirty = False  # Reset flag for this order

            original_wc_status = wc_order.status  # Save original for comparison
            
            added_new = False

            # Update WC status if needed
            if erpnext_wc_status != wc_order.status:
                wc_order.status = erpnext_wc_status
                wc_order_dirty = True

            # Conditional logic for adding tracking
            if sales_order.status == "Completed":
                # Step 1: Get linked delivery note names via items
                dn_items = frappe.get_all(
                    "Delivery Note Item",
                    filters={"against_sales_order": sales_order.name, "docstatus": 1},
                    fields=["parent"]
                )
                dn_names = list(set(d.parent for d in dn_items))
                
                # Step 2: Get details for non-cancelled delivery notes
                delivery_notes = frappe.get_all(
                    "Delivery Note",
                    filters={"name": ["in", dn_names], "status": ["!=", "Cancelled"]},
                    fields=["posting_date", "parcel_service", "tracking_number"]
                )
                
                if delivery_notes:
                    # Parse existing meta_data
                    meta_list = json.loads(wc_order.meta_data) if wc_order.meta_data else []
                    
                    # Find or create the _wc_shipment_tracking_items entry
                    tracking_meta = next((m for m in meta_list if m['key'] == '_wc_shipment_tracking_items'), None)
                    if tracking_meta:
                        if isinstance(tracking_meta['value'], str):
                            tracking_list = json.loads(tracking_meta['value']) if tracking_meta['value'] else []
                        else:
                            tracking_list = tracking_meta['value'] or []
                    else:
                        tracking_list = []
                        tracking_meta = {'key': '_wc_shipment_tracking_items', 'value': []}
                        meta_list.append(tracking_meta)
                    
                    # Set to track changes
                    added_new = False
                    
                    # Prepare existing set for duplicate check
                    existing_set = set((item.get('tracking_number'), item.get('tracking_provider')) for item in tracking_list)
                    
                    # Add new trackings
                    for dn in delivery_notes:
                        provider = get_provider(dn.parcel_service)
                        if provider and dn.tracking_number:
                            # Calculate Unix timestamp as int (assuming midnight UTC)
                            dt = datetime(dn.posting_date.year, dn.posting_date.month, dn.posting_date.day)
                            date_shipped = int(dt.timestamp())
                            
                            tracking_numbers = [tn.strip() for tn in dn.tracking_number.split(',') if tn.strip()]
                            for tn in tracking_numbers:
                                if (tn, provider) not in existing_set:
                                    # Generate tracking_id (emulate plugin's MD5)
                                    unique_str = provider + tn + str(date_shipped)
                                    tracking_id = hashlib.md5(unique_str.encode()).hexdigest()
                                    
                                    new_tracking = {
                                        "tracking_provider": provider,
                                        "custom_tracking_provider": "",
                                        "custom_tracking_link": "",
                                        "tracking_number": tn,
                                        "date_shipped": date_shipped,
                                        "tracking_id": tracking_id
                                    }
                                    tracking_list.append(new_tracking)
                                    added_new = True
                                    existing_set.add((tn, provider))  # Update set to prevent intra-loop duplicates
                    
                    if added_new:
                        # Prepare for direct API update instead of relying on save for meta
                        wc_server = frappe.get_doc("WooCommerce Server", wc_order.woocommerce_server)
                        wcapi = API(
                            url="https://" + wc_order.woocommerce_server,
                            consumer_key=wc_server.api_consumer_key,  # Adjust field name if different (e.g., api_key)
                            consumer_secret=wc_server.api_consumer_secret,  # Adjust field name if different (e.g., api_secret)
                            version="wc/v3"
                        )
                        update_data = {
                            "meta_data": [
                                {
                                    "key": "_wc_shipment_tracking_items",
                                    "value": tracking_list
                                }
                            ]
                        }
                        if tracking_meta and 'id' in tracking_meta:
                            update_data["meta_data"][0]["id"] = tracking_meta['id']
                        # Include status if it was changed or to be safe
                        if wc_order.status != original_wc_status:
                            update_data["status"] = wc_order.status
                        response = wcapi.put(f"orders/{wc_order.id}", update_data)

                        if response.status_code == 200:
                            data = response.json()
                            # Update local wc_order fields from response (in-memory only)
                            wc_order.status = data['status']
                            wc_order.woocommerce_date_modified = data['date_modified_gmt']
                            # Reload from API to ensure consistency
                            wc_order.load_from_db()

            # Save if changes were made and not already handled by direct API (e.g., if no tracking but status change)
            if wc_order_dirty and not added_new:  # Only if no direct API was done
                wc_order.save()
                # Update SO hash to prevent loops
                sales_order.custom_woocommerce_last_sync_hash = wc_order.woocommerce_date_modified
                if sales_order.docstatus == 1:
                    frappe.db.set_value("Sales Order", sales_order.name, "custom_woocommerce_last_sync_hash", wc_order.woocommerce_date_modified)
                else:
                    sales_order.save(ignore_permissions=True)
                sales_order.reload()  # Refresh in-memory object
            elif wc_order_dirty:
                # If direct API was done, update SO hash here
                sales_order.custom_woocommerce_last_sync_hash = wc_order.woocommerce_date_modified
                if sales_order.docstatus == 1:
                    frappe.db.set_value("Sales Order", sales_order.name, "custom_woocommerce_last_sync_hash", wc_order.woocommerce_date_modified)
                else:
                    sales_order.save(ignore_permissions=True)
                sales_order.reload()  # Refresh in-memory object

            frappe.db.commit()
        except Exception as e:
            frappe.log_error(message=frappe.get_traceback(), title=f"WC Order Sync Fail: {wc_order.id}")
            pass  # Continue to next order
        
def get_provider(parcel_service):
    if not parcel_service:
        return None
    lower = parcel_service.lower()
    if 'ups' in lower:
        return 'UPS'
    if 'fedex' in lower:
        return 'Fedex'
    if 'usps' in lower:
        return 'USPS'
    return None

def process_portal_payment(wc_order: WooCommerceOrder) -> bool:
    """
    Process a WooCommerce 'portal' order as a payment to apply to an ERPNext Sales Order or Sales Invoice.
    Returns True if successful (payment created or already exists).
    """
    try:
        # Extract from fee_lines (portal orders use fees instead of line_items)
        if not hasattr(wc_order, 'fee_lines') or not wc_order.fee_lines:
            raise ValueError(f"No fee_lines attribute or data found in WC Order {wc_order.id}")
        fee_lines = json.loads(wc_order.fee_lines)
        if not fee_lines:
            raise ValueError(f"No fee lines found in WC Order {wc_order.id}")
        
        # Get description from first fee line
        desc = fee_lines[0].get("name", "")
        if "Online Payment for Invoice / Reference #:" not in desc:
            raise ValueError(f"Invalid description in WC Order {wc_order.id}: {desc}")
        reference_no = desc.split("Online Payment for Invoice / Reference #:")[-1].strip()
        #print(reference_no)
        # Determine doctype
        if reference_no.startswith("Q"):
            doctype = "Quotation"
        elif reference_no.startswith("SO"):
            doctype = "Sales Order"
        elif reference_no.startswith("INV"):
            doctype = "Sales Invoice"
        else:
            return True  # Ignore if not a standard reference number, treat as success
        
        # Validate document exists and get outstanding amount
        if not frappe.db.exists(doctype, reference_no):
            return True  # Ignore if not found, treat as success
        doc = frappe.get_doc(doctype, reference_no)
        
        # Handle Quotation by creating and submitting a Sales Order from it
        if doctype == "Quotation":
            try:
                from erpnext.selling.doctype.quotation.quotation import make_sales_order
                
                # Load Quotation
                quot = frappe.get_doc("Quotation", reference_no)
                
                # Auto-submit draft Quotation before creating SO
                if quot.docstatus == 0:
                    try:
                        quot.flags.ignore_mandatory = True
                        # Clear payment terms if any (moved from below to here for drafts)
                        original_quot_terms = quot.payment_terms_template
                        if quot.payment_terms_template:
                            quot.payment_terms_template = None
                        if hasattr(quot, "payment_schedule"):
                            quot.payment_schedule = []
                        quot.save(ignore_permissions=True)
                        quot.load_from_db()
                        
                        # Temporarily clear customer's payment_terms
                        original_cust_terms = frappe.db.get_value("Customer", quot.party_name, "payment_terms")  # Note: Quotation uses party_name, not customer
                        try:
                            frappe.db.set_value("Customer", quot.party_name, "payment_terms", None)
                            frappe.clear_cache(doctype="Customer")
                            quot.submit()
                        finally:
                            frappe.db.set_value("Customer", quot.party_name, "payment_terms", original_cust_terms)
                            frappe.clear_cache(doctype="Customer")
                            if original_quot_terms:
                                frappe.db.set_value("Quotation", quot.name, "payment_terms_template", original_quot_terms)
                            frappe.db.commit()
                        
                        quot.load_from_db()  # Reload after submission
                    except Exception as e:
                        error_msg = f"Failed to auto-submit draft Quotation {reference_no} for WC Order {wc_order.id}: {str(e)}\n{frappe.get_traceback()}"
                        frappe.log_error(message=error_msg, title="WC Auto-Submit Quotation Error")
                        return False
                
                # Temporarily clear payment terms on Quotation to avoid inheritance issues
                original_quot_terms = quot.payment_terms_template
                if original_quot_terms:
                    frappe.db.set_value("Quotation", quot.name, "payment_terms_template", None)
                    frappe.db.commit()  # Ensure the change is committed and visible

                # Create Sales Order from Quotation
                so = make_sales_order(quot.name)

                # Set delivery date based on item groups (patch addition)
                wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)
                extended_group = wc_server.custom_extended_delivery_date_item_group
                has_extended = any(
                    frappe.db.get_value("Item", item.item_code, "item_group") == extended_group
                    for item in so.items
                )
                delivery_after = wc_server.delivery_after_days or 7
                if has_extended:
                    delivery_after *= 2
                delivery_date = frappe.utils.add_days(frappe.utils.nowdate(), delivery_after)
                so.delivery_date = delivery_date
                for item in so.items:
                    item.delivery_date = delivery_date

                # Restore Quotation's payment terms immediately after
                if original_quot_terms:
                    frappe.db.set_value("Quotation", quot.name, "payment_terms_template", original_quot_terms)
                    frappe.db.commit()
                
                # Temporarily clear payment terms on new SO (mirroring existing logic)
                original_so_terms = so.payment_terms_template
                if so.payment_terms_template:
                    so.payment_terms_template = None
                if hasattr(so, "payment_schedule"):
                    so.payment_schedule = []
                so.save(ignore_permissions=True)
                so.load_from_db()
                
                # Temporarily clear customer's payment_terms to prevent inheritance during submit
                original_cust_terms = frappe.db.get_value("Customer", so.customer, "payment_terms")
                try:
                    frappe.db.set_value("Customer", so.customer, "payment_terms", None)
                    frappe.clear_cache(doctype="Customer")
                    so.flags.ignore_mandatory = True
                    so.submit()
                finally:
                    # Restore original terms
                    frappe.db.set_value("Customer", so.customer, "payment_terms", original_cust_terms)
                    frappe.clear_cache(doctype="Customer")
                    if original_quot_terms:
                        frappe.db.set_value("Quotation", quot.name, "payment_terms_template", original_quot_terms)
                        frappe.clear_cache(doctype="Quotation")  # Optional, for consistency
                    if original_so_terms:
                        frappe.db.set_value("Sales Order", so.name, "payment_terms_template", original_so_terms)
                    frappe.db.commit()
                
                so.load_from_db()  # Reload to ensure updated state
                
                # Switch doctype/reference to the new SO for payment application
                doctype = "Sales Order"
                reference_no = so.name
                doc = so  # Update doc reference for outstanding calculation below
                
            except Exception as e:
                error_msg = f"Failed to create/submit SO from Quotation {reference_no} for WC Order {wc_order.id}: {str(e)}\n{frappe.get_traceback()}"
                frappe.log_error(message=error_msg, title="WC Quotation to SO Error")
                return False  # Bail out if creation fails        
        
        # Auto-submit draft Sales Order or Sales Invoice before applying payment
        if doctype in ["Sales Order", "Sales Invoice"] and doc.docstatus == 0: # Quotation handled separately above
            try:
                doc.flags.ignore_mandatory = True  # Bypass any non-critical validations if needed
                # Clear payment_terms_template and payment_schedule on draft SO/SI to avoid due date conflicts during submit/SI creation
                if doctype in ["Sales Order", "Sales Invoice"] and doc.payment_terms_template:
                    doc.payment_terms_template = None  # Remove terms to prevent validation errors
                if hasattr(doc, "payment_schedule"):
                    doc.payment_schedule = []                    
                    doc.save(ignore_permissions=True)  # Save the change before submit
                    doc.load_from_db()  # Reload to ensure the field is cleared
                # Temporarily clear customer's payment_terms to prevent SI from inheriting during set_missing_values
                original_terms = frappe.db.get_value("Customer", doc.customer, "payment_terms")
                try:
                    frappe.db.set_value("Customer", doc.customer, "payment_terms", None)
                    frappe.clear_cache(doctype="Customer")
                    doc.submit()
                finally:
                    frappe.db.set_value("Customer", doc.customer, "payment_terms", original_terms)
                    frappe.clear_cache(doctype="Customer")
                    frappe.db.commit()  # Ensure the restore is committed immediately

                doc.load_from_db()  # Reload to ensure updated state (e.g., docstatus=1, any triggered updates)
                frappe.db.commit()  # Ensure changes are committed
            except Exception as e:
                error_msg = f"Failed to auto-submit draft {doctype} {reference_no} for WC Order {wc_order.id}: {str(e)}\n{frappe.get_traceback()}"
                frappe.log_error(message=error_msg, title=f"WC Auto-Submit {doctype} Error")
                return False  # Bail out if submission fails to avoid invalid PE insert 
 
        outstanding = doc.outstanding_amount if doctype == "Sales Invoice" else (doc.rounded_total - doc.advance_paid)
        if outstanding <= 0:
            return True  # Already fully paid; treat as success
        
        # Extract trans_id from meta_data.
        # Preferred source: Elavon Converge meta key.
        # Fallback: WooCommerce order "transaction_id" field.
        meta_data = json.loads(wc_order.meta_data) if wc_order.meta_data else []
        trans_id = next(
            (m.get("value") for m in meta_data if m.get("key") == "_wc_elavon_converge_credit_card_trans_id"),
            None,
        )

        if not trans_id:
            # WC API commonly exposes this as "transaction_id" (string). Some plugins store it as empty.
            trans_id = getattr(wc_order, "transaction_id", None)

        # Final validation
        if not trans_id:
            raise ValueError(
                f"No payment transaction id found for WC Order {wc_order.id} "
                f"(missing _wc_elavon_converge_credit_card_trans_id and transaction_id)"
            )
        
        # Check existing payments for duplicate (via reference_no containing trans_id)
        payments = frappe.get_all(
            "Payment Entry Reference",
            filters={"reference_doctype": doctype, "reference_name": reference_no},
            fields=["parent"]
        )
        payment_names = list(set(p.parent for p in payments))
        is_duplicate = False
        for pe_name in payment_names:
            pe = frappe.get_doc("Payment Entry", pe_name)
            if pe.docstatus == 1 and trans_id in (pe.reference_no or ""):
                is_duplicate = True
                break
        if is_duplicate:
            return True  # Already applied; treat as success
        
        # Create and submit Payment Entry (use fee total for amount)
        payment_amount = float(fee_lines[0].get("total", wc_order.total or 0))
        if payment_amount <= 0 or payment_amount > outstanding:
            raise ValueError(f"Invalid payment amount {payment_amount} for WC Order {wc_order.id} (outstanding: {outstanding})")
        
        wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)
       
        company = doc.company
        customer = doc.customer
        currency = doc.currency
        pe = frappe.new_doc("Payment Entry")
        pe.payment_type = "Receive"
        pe.company = company
        pe.posting_date = now().split(" ")[0]  # Use current date
        pe.mode_of_payment = wc_server.custom_default_mode_of_payment
        pe.party_type = "Customer"
        pe.party = customer
        pe.paid_amount = payment_amount
        pe.received_amount = payment_amount
        pe.paid_to = wc_server.custom_credit_card_clearing_account
        pe.bank_account = wc_server.custom_credit_card_clearing_account
        pe.paid_to_account_currency = currency
        pe.reference_no = f"WC Portal {wc_order.id} - Trans ID: {trans_id}"
        pe.reference_date = wc_order.date_paid or now().split(" ")[0]
        
        # Add reference
        ref = pe.append("references")
        ref.reference_doctype = doctype
        ref.reference_name = reference_no
        ref.total_amount = doc.grand_total
        ref.outstanding_amount = outstanding
        ref.allocated_amount = payment_amount
        
        pe.flags.ignore_mandatory = True
        pe.insert()
        pe.submit()
        frappe.db.commit()
        return True
    except Exception as e:
        error_msg = f"Failed to process portal payment for WC Order {wc_order.id}: {str(e)}\n{frappe.get_traceback()}"
        frappe.log_error(message=error_msg, title="WC Portal Payment Error")
        return False

"""
frappe.call("woocommerce_fusion.tasks.sync_sales_orders.clear_credit_card_clearing")
"""
# Transfer total creidt card clearing account balance to bank of america account once per day at 9m when the settlement happens (elevon credit card processor does not have an api so we must clear blindly)
def clear_credit_card_clearing():
    """
    Run nightly at 9 PM PST/PDT: Transfer balance from Credit Card Clearing to Bank of America account via Internal Transfer Payment Entry.
    Uses ZoneInfo for time zone handling; assumes server clock is accurate.
    """
    # Get current time in PST/PDT (handles DST automatically)
    pst_tz = ZoneInfo("America/Los_Angeles")
    now_pst = datetime.now(tz=pst_tz)

    if now_pst.hour != 21:
        return  # Only run at 9 PM PST/PDT

    wc_servers = frappe.get_all("WooCommerce Server", filters={"enable_sync": 1}, limit=1)  # Adjust filter if needed (e.g., by name)
    if not wc_servers:
        frappe.log_error("No active WooCommerce Server found", "clear_credit_card_clearing")
        return
    wc_server = frappe.get_cached_doc("WooCommerce Server", wc_servers[0].name)

    clearing_account = wc_server.custom_credit_card_clearing_account
    bank_account = wc_server.custom_bank_account
    mode_of_payment = wc_server.custom_bank_transfer_mode_of_payment
    
    company = frappe.db.get_value("Account", clearing_account, "company")
    if not company:
        frappe.log_error("Company not found for account", clearing_account)
        return

    balance = get_balance_on(account=clearing_account, date=now_pst.date(), company=company)
    if balance <= 0:
        return  # No positive balance to transfer (or log if negative)

    # Assume same currency for both accounts
    currency = frappe.db.get_value("Account", clearing_account, "account_currency")

    pe = frappe.new_doc("Payment Entry")
    pe.payment_type = "Internal Transfer"
    pe.company = company
    pe.posting_date = now_pst.date()
    pe.mode_of_payment = mode_of_payment
    pe.paid_from = clearing_account
    pe.paid_from_account_currency = currency
    pe.paid_to = bank_account
    pe.paid_to_account_currency = currency
    pe.paid_amount = balance
    pe.received_amount = balance
    pe.reference_no = f"Credit Card Batch Settlement {now_pst.strftime('%Y-%m-%d')} - Nightly Clearing"
    pe.reference_date = now_pst.date()

    pe.flags.ignore_mandatory = True
    pe.insert()
    pe.submit()

    frappe.db.commit()

class SynchroniseSalesOrder(SynchroniseWooCommerce):
    """
    Class for managing synchronisation of a WooCommerce Order with an ERPNext Sales Order
    """

    def __init__(
        self,
        sales_order: Optional[SalesOrder] = None,
        woocommerce_order: Optional[WooCommerceOrder] = None,
    ) -> None:
        super().__init__()
        self.sales_order = sales_order
        self.woocommerce_order = woocommerce_order
        self.settings = frappe.get_cached_doc("WooCommerce Integration Settings")

    def run(self):
        """
        Run synchronisation
        """
        try:
            self.get_corresponding_sales_order_or_woocommerce_order()
            self.sync_wc_order_with_erpnext_order()
        except WooCommerceMissingItemError as err:
            simple_msg = f"Could not sync WooCommerce Order {err.order_id} from {err.woocommerce_server} because no matching item was found in ERPNext for product ID {err.woocomm_item_id} ({err.product_name}). To resolve the issue, map the item in erpNext."
            title_msg = f"WooCommerce Item Not Found: {err.woocomm_item_id}"
            frappe.log_error(message=simple_msg, title=title_msg)
            self.sales_order = None
            return  # don't re-raise      
        except Exception as err:
            error_message = f"{frappe.get_traceback()}\n\nSales Order Data: \n{str(self.sales_order.as_dict()) if self.sales_order else ''}\n\nWC Product Data \n{str(self.woocommerce_order.as_dict()) if self.woocommerce_order else ''})"
            frappe.log_error(message=error_message, title="WC Error")
            raise err

    def get_third_party_shipping_details(self, wc_order: WooCommerceOrder) -> Optional[Dict]:
        try:
            meta_list = json.loads(wc_order.meta_data)
            shipping_desc = next((m['value'] for m in meta_list if m['key'] == '_cc_shipping_description'), None)
            if shipping_desc and "Ship on your own account #" in shipping_desc:
                parts = [p.strip() for p in shipping_desc.split(" - ")]
                if len(parts) == 3:
                    # Extract account (after "#: ")
                    account_part = parts[0].split("#: ", 1)[1].strip()
                    
                    # Extract service (after "Service: ")
                    service_part = parts[1].split(": ", 1)[1].strip()
                    
                    # Extract ZIP (after "ZIP: ")
                    zip_part = parts[2].split(": ", 1)[1].strip()
                    
                    # Mapping from WooCommerce service to ERPNext shipping method
                    # Expand this dict with more mappings as needed, e.g., {"UPS 2nd Day": "UPS 2 Day Air"}
                    shipping_map = {
                        "FedEx Ground": "FedEx Ground",
                        "UPS Ground": "UPS Ground",
                        #"UPS 2nd Day": "UPS 2 Day Air",
                        # Add more here, e.g., "UPS Next Day": "UPS Next Day Air"
                    }
                    mapped_method = shipping_map.get(service_part, "Best Way")  # Fallback if no match
                    
                    # ZIP logic
                    if zip_part.lower() == "billing":
                        billing_data = json.loads(wc_order.billing)
                        zip_value = billing_data.get('postcode', '')
                    else:
                        zip_value = zip_part if zip_part.isdigit() else ''
                    
                    return {
                        "shipping_method": mapped_method,
                        "account": account_part,
                        "postal": zip_value
                    }
        except Exception as e:
            # Optional: Log the error for debugging
            frappe.log_error(message=f"Error parsing shipping description for WC Order {wc_order.id}: {str(e)}", title="WC Shipping Parse Error")
        
        return None

    def get_corresponding_sales_order_or_woocommerce_order(self):
        """
        If we have an ERPNext Sales Order, get the corresponding WooCommerce Order
        If we have a WooCommerce Order, get the corresponding ERPNext Sales Order

        Assumes that both exist, and that the Sales Order is linked to the WooCommerce Order
        """
        if self.sales_order and not self.woocommerce_order and self.sales_order.custom_woocommerce_order_id:
            # Validate that this Sales Order's WooCommerce Server has sync enabled
            wc_server = frappe.get_cached_doc("WooCommerce Server", self.sales_order.woocommerce_server)
            if not wc_server.enable_sync:
                raise SyncDisabledError(wc_server)

            wc_orders = get_list_of_wc_orders(sales_order=self.sales_order)

            # If we can't find a linked WooCommerce Order (it may have been deleted), we can't proceed
            if len(wc_orders) == 0:
                raise WooCommerceOrderNotFoundError(self.sales_order)

            self.woocommerce_order = wc_orders[0]

        if self.woocommerce_order and not self.sales_order:
            self.get_erpnext_sales_order()

    def get_erpnext_sales_order(self):
        """
        Get erpnext item for a WooCommerce Product
        """
        filters = [
            ["Sales Order", "custom_woocommerce_order_id", "is", "set"],
            ["Sales Order", "woocommerce_server", "is", "set"],
        ]
        filters.append(["Sales Order", "custom_woocommerce_order_id", "=", self.woocommerce_order.id])
        filters.append(
            [
                "Sales Order",
                "woocommerce_server",
                "=",
                self.woocommerce_order.woocommerce_server,
            ]
        )

        sales_orders = frappe.get_all(
            "Sales Order",
            filters=filters,
            fields=["name"],
        )
        if len(sales_orders) > 0:
            self.sales_order = frappe.get_doc("Sales Order", sales_orders[0].name)

    def sync_wc_order_with_erpnext_order(self):
        """
        Syncronise Sales Order between ERPNext and WooCommerce
        """
        if self.sales_order and not self.woocommerce_order:
            # create missing order in WooCommerce
            pass
        elif self.woocommerce_order and not self.sales_order:
            # create missing order in ERPNext
            self.create_sales_order(self.woocommerce_order)
        elif self.sales_order and self.woocommerce_order:
            # both exist, check sync hash
            if (self.woocommerce_order.woocommerce_date_modified != self.sales_order.custom_woocommerce_last_sync_hash):
                # Woocommerce order has been modified recently. Update the erpNext Order
                if get_datetime(self.woocommerce_order.woocommerce_date_modified) > get_datetime(self.sales_order.modified):
                    self.update_sales_order(self.woocommerce_order, self.sales_order)
                # erpnNext order has been modified recently. Update the Woocommerce Order
                #if get_datetime(self.woocommerce_order.woocommerce_date_modified) < get_datetime(self.sales_order.modified):
                    #self.update_woocommerce_order(self.woocommerce_order, self.sales_order)

            # If the Sales Order exists and has been submitted in the mean time, sync Payment Entries
            if (self.sales_order.docstatus == 1 and not self.sales_order.woocommerce_payment_entry and not self.sales_order.custom_attempted_woocommerce_auto_payment_entry):
                self.sales_order.reload()
                if self.create_and_link_payment_entry(self.woocommerce_order, self.sales_order):
                    self.sales_order.save()

    def update_sales_order(self, woocommerce_order: WooCommerceOrder, sales_order: SalesOrder):
        """
        Update the ERPNext Sales Order with fields from it's corresponding WooCommerce Order
        """
        # Ignore cancelled Sales Orders
        if sales_order.docstatus != 2:
            so_dirty = False

            # Update the woocommerce_status field if necessary
            wc_order_status = WC_ORDER_STATUS_MAPPING_REVERSE[woocommerce_order.status]
            if sales_order.woocommerce_status != wc_order_status:
                sales_order.woocommerce_status = wc_order_status
                so_dirty = True

            if sales_order.custom_woocommerce_customer_note != woocommerce_order.customer_note:
                sales_order.custom_woocommerce_customer_note = woocommerce_order.customer_note
                so_dirty = True  # Added dirty flag for customer_note

            # Update the payment_method_title field if necessary, use the payment method ID
            # if the title field is too long
            payment_method = (
                woocommerce_order.payment_method_title
                if len(woocommerce_order.payment_method_title) < 140
                else woocommerce_order.payment_method
            )
            if sales_order.woocommerce_payment_method != payment_method:
                sales_order.woocommerce_payment_method = payment_method
                so_dirty = True

            if not sales_order.woocommerce_payment_entry:
                pe_created, pe_name, attempted_flag = self.create_and_link_payment_entry(woocommerce_order, sales_order)
                if pe_created:
                    # Set woocommerce_payment_entry safely
                    if sales_order.docstatus == 1:
                        frappe.db.set_value("Sales Order", sales_order.name, "woocommerce_payment_entry", pe_name)
                    else:
                        sales_order.woocommerce_payment_entry = pe_name
                        so_dirty = True  # Mark dirty if not submitted
                    sales_order.reload()  # Reload after update
                    so_dirty = True  # Ensure dirty for any further saves
                if attempted_flag is not None:
                    # Set attempted flag safely (consistent with create_sales_order)
                    if sales_order.docstatus == 1:
                        frappe.db.set_value("Sales Order", sales_order.name, "custom_attempted_woocommerce_auto_payment_entry", attempted_flag)
                    else:
                        sales_order.custom_attempted_woocommerce_auto_payment_entry = attempted_flag
                        so_dirty = True
                    sales_order.reload()

            if so_dirty:
                sales_order.flags.created_by_sync = True
                if sales_order.docstatus == 1:
                    # For submitted, use db_set for all changed fields (batch them for efficiency)
                    updates = {
                        "woocommerce_status": sales_order.woocommerce_status,
                        "woocommerce_payment_method": sales_order.woocommerce_payment_method,
                        "custom_woocommerce_customer_note": sales_order.custom_woocommerce_customer_note,
                        # Add any other fields that might have changed
                    }
                    frappe.db.set_value("Sales Order", sales_order.name, updates)
                else:
                    sales_order.save()
                sales_order.reload()  # Ensure latest state

            # Update hash to reflect successful sync
            if sales_order.docstatus == 1:
                frappe.db.set_value("Sales Order", sales_order.name, "custom_woocommerce_last_sync_hash", woocommerce_order.woocommerce_date_modified)
            else:
                sales_order.custom_woocommerce_last_sync_hash = woocommerce_order.woocommerce_date_modified
                sales_order.flags.created_by_sync = True
                sales_order.save()
            sales_order.reload()

            # If WC status is "completed" and SO not fully billed, create SI
            if woocommerce_order.status == "completed" and sales_order.per_billed < 100:
                self.create_and_submit_sales_invoice(sales_order)
                # Reload after SI to ensure fresh state (if further ops)
                sales_order.reload()

        frappe.db.commit()

    def create_and_link_payment_entry(
        self, wc_order: WooCommerceOrder, sales_order: SalesOrder
    ) -> Tuple[bool, Optional[str], Optional[int]]:
        """
        Create a Payment Entry for a WooCommerce Order that has been marked as Paid.
        Returns (success, pe_name, attempted_flag_value)
        """
        wc_server = frappe.get_cached_doc("WooCommerce Server", sales_order.woocommerce_server)
        if not wc_server:
            raise ValueError("Could not find woocommerce_server in list of servers")
        
        wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)
        
        # Hardcoded mappings (replace JSON fields) -- NEW
        PAYMENT_METHOD_BANK_ACCOUNT_MAPPING = {
            "elavon_converge_credit_card": wc_server.custom_credit_card_clearing_account,
            "amazon_payments_advanced": wc_server.custom_amazon_pay_clearing_account,
            "Manually Processed": wc_server.custom_credit_card_clearing_account,
            # Add more methods here if needed
        }
        
        PAYMENT_METHOD_GL_ACCOUNT_MAPPING = {
            "elavon_converge_credit_card": wc_server.custom_credit_card_clearing_account,
            "amazon_payments_advanced": wc_server.custom_amazon_pay_clearing_account,
            "Manually Processed": wc_server.custom_credit_card_clearing_account,
            # Mirror bank mappings or adjust if GL differs
        }

        attempted_flag = None  # To return the flag value for db_set

        # Validate that WooCommerce order has been paid, and that sales order doesn't have a linked Payment Entry yet
        if (
            wc_server.enable_payments_sync
            and wc_order.payment_method
            and ((wc_server.ignore_date_paid) or (not wc_server.ignore_date_paid and wc_order.date_paid))
            and not sales_order.woocommerce_payment_entry
            and sales_order.docstatus == 1
        ):
            
            # --- PATCH: fix mis-stored totals on split child WooCommerce orders ---
            try:
                meta_list = json.loads(wc_order.meta_data) if wc_order.meta_data else []
            except (TypeError, json.JSONDecodeError):
                meta_list = []

            order_type = None
            if isinstance(meta_list, list):
                for m in meta_list:
                    if isinstance(m, dict) and m.get("key") == "order_type":
                        order_type = m.get("value")
                        break

            if order_type == "child":
                try:
                    line_items = json.loads(wc_order.line_items) if wc_order.line_items else []
                    shipping_lines = json.loads(wc_order.shipping_lines) if wc_order.shipping_lines else []
                    fee_lines = json.loads(wc_order.fee_lines) if wc_order.fee_lines else []
                except (TypeError, json.JSONDecodeError):
                    line_items, shipping_lines, fee_lines = [], [], []

                recomputed_total = 0.0

                # Sum this child's own lines (including taxes)
                for li in line_items:
                    recomputed_total += float(li.get("total") or 0) + float(li.get("total_tax") or 0)

                for sl in shipping_lines:
                    recomputed_total += float(sl.get("total") or 0) + float(sl.get("total_tax") or 0)

                for fl in fee_lines:
                    recomputed_total += float(fl.get("total") or 0) + float(fl.get("total_tax") or 0)

                # Override wc_order.total for payment purposes if recomputed differs (indicating leaked parent fee)
                original_wc_total = float(wc_order.total)
                if abs(recomputed_total - original_wc_total) > 0.01:
                    wc_order_total_for_payment = recomputed_total  # Use recomputed for PE
                else:
                    wc_order_total_for_payment = original_wc_total
            else:
                wc_order_total_for_payment = float(wc_order.total)
            # --- END PATCH ---
            
            # If the grand total is 0, skip payment entry creation
            if sales_order.grand_total is None or wc_order_total_for_payment == 0:
                attempted_flag = 1
                return True, None, attempted_flag

            # Get Company Bank Account for this Payment Method -- MODIFIED: Use hardcoded dict instead of JSON
            if wc_order.payment_method not in PAYMENT_METHOD_BANK_ACCOUNT_MAPPING:
                raise KeyError(
                    f"WooCommerce payment method {wc_order.payment_method} not found in hardcoded mappings"
                )
            company_bank_account = PAYMENT_METHOD_BANK_ACCOUNT_MAPPING[wc_order.payment_method]

            if company_bank_account:
                # Get G/L Account for this Payment Method -- MODIFIED: Use hardcoded dict instead of JSON
                if wc_order.payment_method not in PAYMENT_METHOD_GL_ACCOUNT_MAPPING:
                    raise KeyError(
                        f"WooCommerce payment method {wc_order.payment_method} not found in hardcoded GL mappings"
                    )
                company_gl_account = PAYMENT_METHOD_GL_ACCOUNT_MAPPING[wc_order.payment_method]

                # Create a new Payment Entry
                company = frappe.get_value("Account", company_gl_account, "company")                
                meta_data = wc_order.get("meta_data", None)

                # Attempt to get Payfast Transaction ID
                payment_reference_no = wc_order.get("transaction_id", None)

                # Attempt to get Yoco Transaction ID
                if not payment_reference_no:
                    payment_reference_no = (
                        next(
                            (data["value"] for data in meta_data if data["key"] == "yoco_order_payment_id"),
                            None,
                        )
                        if meta_data and type(meta_data) is list
                        else None
                    )

                # Determine if the reference should be Sales Order or Sales Invoice
                reference_doctype = "Sales Order"
                reference_name = sales_order.name
                total_amount = sales_order.grand_total
                if sales_order.per_billed > 0:
                    si_item_details = frappe.get_all(
                        "Sales Invoice Item",
                        fields=["name", "parent"],
                        filters={"sales_order": sales_order.name},
                    )
                    if len(si_item_details) > 0:
                        reference_doctype = "Sales Invoice"
                        reference_name = si_item_details[0].parent
                        total_amount = sales_order.grand_total  # Note: This might need adjustment if multiple SIs

                # Check outstanding amount before creating PE to avoid duplicates/errors
                if reference_doctype == "Sales Order":
                    so_values = frappe.db.get_value("Sales Order", reference_name, ["rounded_total", "advance_paid"], as_dict=True) or {}
                    outstanding = so_values.get("rounded_total", 0) - so_values.get("advance_paid", 0)
                else:  # Sales Invoice
                    outstanding = frappe.db.get_value("Sales Invoice", reference_name, "outstanding_amount") or 0

                if outstanding <= 0:
                    # Already fully paid; skip creation and set flag to prevent retries
                    attempted_flag = 1
                    return True, None, attempted_flag

                # Set flag early (attempted, even if fails later)
                attempted_flag = 1

                # Create Payment Entry
                payment_entry_dict = {
                    "company": company,
                    "payment_type": "Receive",
                    "reference_no": payment_reference_no or wc_order.payment_method_title,
                    "reference_date": wc_order.date_paid or sales_order.transaction_date,
                    "party_type": "Customer",
                    "party": sales_order.customer,
                    "posting_date": wc_order.date_paid or sales_order.transaction_date,
                    "paid_amount": wc_order_total_for_payment,
                    "received_amount": wc_order_total_for_payment,
                    "bank_account": company_bank_account,
                    "paid_to": company_gl_account,
                }
                
                if wc_order.payment_method in ["elavon_converge_credit_card", "Manually Processed"]:
                    payment_entry_dict["mode_of_payment"] = wc_server.custom_default_mode_of_payment
                elif wc_order.payment_method == "amazon_payments_advanced":
                    payment_entry_dict["mode_of_payment"] = wc_server.custom_amazon_pay_mode_of_payment
                
                payment_entry = frappe.new_doc("Payment Entry")
                payment_entry.update(payment_entry_dict)
                row = payment_entry.append("references")
                row.reference_doctype = reference_doctype
                row.reference_name = reference_name
                row.total_amount = total_amount
                row.outstanding_amount = outstanding  # Set explicitly for clarity
                row.allocated_amount = min(wc_order_total_for_payment, outstanding)  # Use recomputed to prevent over-allocation on children
                payment_entry.save()
                
                # Auto-submit the Payment Entry to record the advance in the clearing account
                try:
                    payment_entry.submit()  # Allocates as advance against SO/customer
                    return True, payment_entry.name, attempted_flag
                except Exception as e:
                    frappe.log_error(f"Failed to submit Payment Entry for SO {sales_order.name}: {str(e)}")
                    return False, None, attempted_flag  # Log error but return False (attempted but failed)

        # If conditions not met, still set flag if we reached here (e.g., no bank account)
        attempted_flag = 1
        return False, None, attempted_flag

    def update_woocommerce_order(self, wc_order: WooCommerceOrder, sales_order: SalesOrder) -> None:
        """
        Update the WooCommerce Order with fields from it's corresponding ERPNext Sales Order
        """
        wc_order_dirty = False

        # Update the woocommerce_status field if necessary
        sales_order_wc_status = (
            WC_ORDER_STATUS_MAPPING[sales_order.woocommerce_status]
            if sales_order.woocommerce_status
            else None
        )
        if sales_order_wc_status != wc_order.status:
            wc_order.status = sales_order_wc_status
            wc_order_dirty = True

        # Get the Item WooCommerce ID's
        for so_item in sales_order.items:
            so_item.woocommerce_id = frappe.get_value(
                "Item WooCommerce Server",
                filters={"parent": so_item.item_code, "woocommerce_server": wc_order.woocommerce_server},
                fieldname="woocommerce_id",
            )

        # Update the line_items field if necessary
        wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)
        if wc_server.sync_so_items_to_wc:
            sales_order_items_changed = False
            line_items = json.loads(wc_order.line_items)
            # Check if count of line items are different
            if len(line_items) != len(sales_order.items):
                sales_order_items_changed = True
            # Check if any line item properties changed
            else:
                for i, so_item in enumerate(sales_order.items):
                    if not so_item.woocommerce_id:
                        break
                    elif (
                        int(so_item.woocommerce_id) != line_items[i]["product_id"]
                        or so_item.qty != line_items[i]["quantity"]
                        or so_item.rate != get_tax_inc_price_for_woocommerce_line_item(line_items[i])
                    ):
                        sales_order_items_changed = True
                        break

            if sales_order_items_changed:
                # Build the correct lines
                new_line_items = [
                    {
                        "product_id": so_item.woocommerce_id,
                        "quantity": so_item.qty,
                        "price": so_item.rate,
                        "meta_data": line_items[i].get("meta_data", []) if i < len(line_items) else [],
                    }
                    for i, so_item in enumerate(sales_order.items)
                ]
                # Process Order Item Line Field Mappings
                for i, line_item in enumerate(new_line_items):
                    self.set_wc_order_line_items_mapped_fields(line_item, sales_order.items[i])

                # Set the product_id for existing lines to null, to clear the line items for the WooCommerce order
                replacement_line_items = [
                    {"id": line_item["id"], "product_id": None} for line_item in json.loads(wc_order.line_items)
                ]
                replacement_line_items.extend(new_line_items)

                wc_order.line_items = json.dumps(replacement_line_items)
                wc_order_dirty = True

        if wc_order_dirty:
            wc_order.save()

    def set_wc_order_line_items_mapped_fields(
        self, woocommerce_order_line_item: Dict, so_item: SalesOrderItem
    ) -> Tuple[bool, WooCommerceOrder]:
        """
        If there exist any Field Mappings on `WooCommerce Server`, attempt to set their values from
        ERPNext to WooCommerce
        """
        wc_line_item_dirty = False
        if woocommerce_order_line_item and self.sales_order and self.woocommerce_order:
            wc_server = frappe.get_cached_doc(
                "WooCommerce Server", self.woocommerce_order.woocommerce_server
            )
            if wc_server.order_line_item_field_map:
                for map in wc_server.order_line_item_field_map:
                    erpnext_item_field_name = map.erpnext_field_name.split(" | ")
                    erpnext_item_field_value = getattr(so_item, erpnext_item_field_name[0])

                    # We expect woocommerce_field_name to be valid JSONPath
                    jsonpath_expr = parse(map.woocommerce_field_name)
                    woocommerce_order_line_field_matches = jsonpath_expr.find(woocommerce_order_line_item)

                    if len(woocommerce_order_line_field_matches) == 0:
                        if self.woocommerce_order.name:
                            # The field should exist, else raise an error
                            raise ValueError(
                                _("Field <code>{0}</code> not found in Item Line of WooCommerce Order {1}").format(
                                    map.woocommerce_field_name, self.woocommerce_order.name
                                )
                            )

                    # JSONPath parsing typically returns a list, we'll only take the first value
                    woocommerce_order_line_field_value = woocommerce_order_line_field_matches[0].value

                    if erpnext_item_field_value != woocommerce_order_line_field_value:
                        jsonpath_expr.update(woocommerce_order_line_item, erpnext_item_field_value)
                        wc_line_item_dirty = True

        return wc_line_item_dirty, woocommerce_order_line_item

    def create_sales_order(self, wc_order: WooCommerceOrder) -> None:
        """
        Create an ERPNext Sales Order from the given WooCommerce Order
        """
        customer_docname = self.create_or_link_customer_and_address(wc_order)
        #self.create_missing_items(wc_order, json.loads(wc_order.line_items), wc_order.woocommerce_server)

        new_sales_order = frappe.new_doc("Sales Order")
        self.sales_order = new_sales_order
        new_sales_order.customer = customer_docname
        
        # Extract po_number from WooCommerce meta_data if present
        try:
            meta_list = json.loads(wc_order.meta_data)
            po_number = next((m['value'] for m in meta_list if m['key'] == 'po_number'), None)
        except (json.JSONDecodeError, TypeError):
            po_number = None
        new_sales_order.po_no = po_number  # (sets to None/blank if not found)
        new_sales_order.custom_woocommerce_order_id = wc_order.id
        
        new_sales_order.custom_woocommerce_customer_note = wc_order.customer_note

        new_sales_order.woocommerce_status = WC_ORDER_STATUS_MAPPING_REVERSE[wc_order.status]
        wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)

        new_sales_order.woocommerce_server = wc_order.woocommerce_server
        # Set the payment_method_title field if necessary, use the payment method ID if the title field is too long
        payment_method = (
            wc_order.payment_method_title
            if len(wc_order.payment_method_title) < 140
            else wc_order.payment_method
        )
        new_sales_order.woocommerce_payment_method = payment_method
        
        # ⚠️ Do NOT uncomment — setting payment_terms_template here causes Sales Invoice due-date validation errors (ERPNext enforces template rules, leading to “Due / Reference Date cannot be after …”)
        # Map to ERPNext payment terms template based on WooCommerce Payment Method Title
#         if payment_method == "Amazon Pay":
#             new_sales_order.payment_terms_template = "AMAZON PAY"
#         elif payment_method == "Credit Card":
#             new_sales_order.payment_terms_template = "CREDIT CARD"

        created_date = wc_order.date_created.split("T")       
        
        created_date = wc_order.date_created.split("T")
        new_sales_order.transaction_date = created_date[0]
        delivery_after = wc_server.delivery_after_days or 7
        new_sales_order.delivery_date = frappe.utils.add_days(created_date[0], delivery_after)
        new_sales_order.company = wc_server.company
        new_sales_order.currency = wc_order.currency

        if (
            (wc_server.enable_shipping_methods_sync)
            and (shipping_lines := json.loads(wc_order.shipping_lines))
            and len(wc_server.shipping_rule_map) > 0
        ):
            if len(wc_order.shipping_lines) > 0:
                shipping_rule_mapping = next(
                    (
                        rule
                        for rule in wc_server.shipping_rule_map
                        if rule.wc_shipping_method_id == shipping_lines[0]["method_title"]
                    ),
                    None,
                )
                new_sales_order.shipping_rule = shipping_rule_mapping.shipping_rule

        # Parse and set custom shipping fields for "Ship on own account" orders
        shipping_info = self.get_third_party_shipping_details(wc_order)

        if shipping_info:
            new_sales_order.custom_shipping_method = shipping_info['shipping_method']
            new_sales_order.custom_ship_on_third_party = 1
            new_sales_order.custom_third_party_account = shipping_info['account']
            new_sales_order.custom_third_party_postal = shipping_info['postal']

        self.set_items_in_sales_order(new_sales_order, wc_order)

        # Explicitly set addresses and custom title fields before insert (replicating client-side script logic)
        new_sales_order.set_missing_lead_customer_details()  # Ensures customer_address and shipping_address_name are set
        new_sales_order.custom_billing_address_title = get_address_title(new_sales_order.customer_address) or ''
        new_sales_order.custom_shipping_address_title = get_address_title(new_sales_order.shipping_address_name) or ''

        try:
            new_sales_order.flags.ignore_mandatory = True
            new_sales_order.flags.created_by_sync = True
            safe_insert(new_sales_order)
            new_sales_order.reload()

            # Set hash before submit (still draft, so .save() is fine, but make conditional for safety)
            new_sales_order.custom_woocommerce_last_sync_hash = wc_order.woocommerce_date_modified
            if new_sales_order.docstatus == 1:  # Unlikely here, but safe
                frappe.db.set_value("Sales Order", new_sales_order.name, "custom_woocommerce_last_sync_hash", wc_order.woocommerce_date_modified)
            else:
                new_sales_order.save()
            new_sales_order.reload()

            if wc_server.submit_sales_orders:
                safe_submit(new_sales_order)
                new_sales_order.reload()
        except Exception as e:
            error_msg = f"Failed to create/submit SO for WC Order {wc_order.id}: {str(e)}\nItem details may require supplier when drop ship is enabled."
            frappe.log_error(message=error_msg, title="WC SO Creation Error")
            raise  # Re-raise to maintain existing behavior

        # Create PE if conditions met; use db_set for links/flags (no .save() on submitted SO)
        pe_created, pe_name, attempted_flag = self.create_and_link_payment_entry(wc_order, new_sales_order)
        if pe_created and pe_name:
            frappe.db.set_value("Sales Order", new_sales_order.name, "woocommerce_payment_entry", pe_name)
        if attempted_flag is not None:
            frappe.db.set_value("Sales Order", new_sales_order.name, "custom_attempted_woocommerce_auto_payment_entry", attempted_flag)

        # If WC order is already "completed" (shipped/FBA fulfilled), create SI
        if wc_order.status == "completed":
            self.create_and_submit_sales_invoice(new_sales_order)
            
        frappe.db.commit()

    def create_or_link_customer_and_address(self, wc_order: WooCommerceOrder) -> str:
        """
        Create or update Customer and Address records, with special handling for guest orders using order ID.
        Improved to merge guest and registered customers based on email and other matching info.
        """
        raw_billing_data = json.loads(wc_order.billing)
        raw_shipping_data = json.loads(wc_order.shipping)
        first_name = raw_billing_data.get("first_name", "").strip().title()
        last_name = raw_billing_data.get("last_name", "").strip().title()
        email = raw_billing_data.get("email", "").strip()
        company_name = clean_company_name(raw_billing_data.get("company", "").strip().title())
        individual_name = f"{first_name} {last_name}".strip() or email

        # Determine if the order is from a guest user
        is_guest = wc_order.customer_id is None or wc_order.customer_id == 0

        # Use the WooCommerce order ID as the identifier for guest orders
        order_id = wc_order.id

        if not email and not is_guest:
            # Log raw_billing_data
            frappe.log_error(message=f"Email is required to create or link a customer. \n\nCustomer Data: {raw_billing_data}", title="WC Customer Error")
            return None

        wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)

        existing_customer = None
        customer = None

        # Step 1: Always search for existing customers by email (via linked contacts)
        if email:
            contact_names = frappe.db.get_all("Contact Email", filters={"email_id": email}, pluck="parent")
            potential_customers = []
            for contact_name in contact_names:
                links = frappe.db.get_all("Dynamic Link", filters={
                    "parent": contact_name,
                    "parenttype": "Contact",
                    "link_doctype": "Customer"
                }, fields=["link_name", "parent as contact_name"])
                for link in links:
                    cust = frappe.get_doc("Customer", link.link_name)
                    # Collect with creation date for sorting
                    potential_customers.append({
                        "name": cust.name,
                        "creation": cust.creation,
                        "company": cust.customer_name if cust.customer_type == "Company" else "",
                        "is_guest": cust.woocommerce_is_guest,
                        "billing_address": self._get_billing_address_summary(cust.name)  # Helper to summarize address for comparison
                    })

            if potential_customers:
                # Sort by creation date (oldest first) and pick the best match
                potential_customers.sort(key=lambda x: x["creation"])
                for pc in potential_customers:
                    # Confirm match by additional fields (name/company/address)
                    if self._customer_info_matches(pc, company_name, individual_name, raw_billing_data):
                        existing_customer = pc["name"]
                        break

        if existing_customer:
            customer = frappe.get_doc("Customer", existing_customer)
            # Update identifier based on order type
            if is_guest:
                new_id = f"Guest-{order_id}"
                ids = [i.strip() for i in (customer.woocommerce_identifier or "").split(",") if i.strip()]
                if new_id not in ids:
                    customer.woocommerce_identifier = ", ".join(ids + [new_id])
            else:
                # For non-guest, set to email (or email+company), replacing any guest IDs
                new_identifier = f"{email}-{company_name}" if company_name and wc_server.enable_dual_accounts else email
                customer.woocommerce_identifier = new_identifier
                customer.woocommerce_is_guest = 0  # Upgrade from guest

            customer.flags.ignore_mandatory = True
            customer.save()
        else:
            # No match; determine customer_identifier and create new
            if is_guest:
                customer_identifier = f"Guest-{order_id}"
            elif company_name and wc_server.enable_dual_accounts:
                customer_identifier = f"{email}-{company_name}"
            else:
                customer_identifier = email

            customer = frappe.new_doc("Customer")
            customer.woocommerce_identifier = customer_identifier
            customer.customer_type = "Company" if company_name else "Individual"
            customer.woocommerce_is_guest = is_guest
            customer.customer_group = "Online - Case Club"
            customer.customer_name = company_name if company_name else individual_name

        # Check if vat_id exists in raw_billing_data and is a valid string
        vat_id = raw_billing_data.get("vat_id")
        if isinstance(vat_id, str) and vat_id.strip():
            customer.tax_id = vat_id

        customer.flags.ignore_mandatory = True

        try:
            customer.save()
        except Exception:
            error_message = f"{frappe.get_traceback()}\n\nCustomer Data{str(customer.as_dict())}"
            frappe.log_error("WooCommerce Error", error_message)
        finally:
            self.customer = customer

        self.create_or_update_address(wc_order)
        contact = create_contact(raw_billing_data, self.customer)
        self.customer.reload()
        self.customer.customer_primary_contact = contact.name
        try:
            self.customer.save()
        except Exception:
            error_message = f"{frappe.get_traceback()}\n\nCustomer Data{str(customer.as_dict())}"
            frappe.log_error("WooCommerce Error", error_message)

        return customer.name

    # Helper methods
    def _get_billing_address_summary(self, customer_name):
        """Summarize billing address for comparison (e.g., 'Address1|City|State|Zip|Country')."""
        addresses = get_addresses_linking_to("Customer", customer_name, fields=["address_line1", "city", "state", "pincode", "country"])
        billing_addr = next((addr for addr in addresses if addr.is_primary_address == 1), None)
        if billing_addr:
            return f"{billing_addr.address_line1}|{billing_addr.city}|{billing_addr.state}|{billing_addr.pincode}|{billing_addr.country}"
        return ""

    def _customer_info_matches(self, potential_cust, company_name, individual_name, raw_billing_data):
        """Check if company, name, and billing address match (case-insensitive, normalized)."""
        # Company match with logging
        existing_company = potential_cust["company"].lower()
        expected_company = company_name.lower()
        if existing_company != expected_company:
            #frappe.log_error(message=f"Email Match, but Company mismatch for potential customer {potential_cust['name']}: Expected '{company_name}', Found '{potential_cust['company']}'", title="WC Merge Skip - Company Mismatch")
            pass
        # Improved Name match: Compare against primary contact's name if available
        # Fetch primary contact for the potential customer
        primary_contact = frappe.db.get_value("Customer", potential_cust['name'], "customer_primary_contact")
        if primary_contact:
            contact = frappe.get_doc("Contact", primary_contact)
            existing_full_name = f"{contact.first_name or ''} {contact.last_name or ''}".strip().lower()
        else:
            existing_full_name = ""  # No primary contact; will handle below
        
        expected_name_lower = individual_name.lower()
        
        # If no existing contact name, skip name check (or log and proceed/ fail based on policy)
        # Here, we'll skip (assume match if company matches and no contact to compare)
        if not existing_full_name:
            frappe.log_error(message=f"No primary contact found for potential customer {potential_cust['name']}. Skipping name check. Expected name: '{individual_name}'", title="WC Merge Skip - No Contact")
            # Proceed as if name matches, since company already matches
        else:
            # Loose substring match on contact name
            if expected_name_lower not in existing_full_name and existing_full_name not in expected_name_lower:
                frappe.log_error(message=f"Name mismatch for potential customer {potential_cust['name']}: Expected '{individual_name}', Found '{existing_full_name.title()}' (from primary contact)", title="WC Merge Skip - Name Mismatch")
                return False
        
        # Address summary match with detailed diff logging
        # Compute expected values with normalizations
        expected = {
            "address_line1": to_proper_case(raw_billing_data.get('address_1', '')),
            "city": to_proper_case(raw_billing_data.get('city', '')),
            "state": raw_billing_data.get('state', '').upper(),
            "pincode": raw_billing_data.get('postcode', ''),
            "country": frappe.get_value('Country', {'code': raw_billing_data.get('country', 'US').lower()}) or ''
        }
        expected_summary = '|'.join(expected.values())
        
        # Fetch existing primary address details
        existing_addresses = frappe.get_all(
            "Address",
            filters={"link_name": potential_cust['name'], "is_primary_address": 1},
            fields=["address_line1", "address_line2", "city", "state", "pincode", "country", "phone"],
            limit=1
        )
        if not existing_addresses:
            frappe.log_error(message=f"No primary billing address found for potential customer {potential_cust['name']}. Expected summary: {expected_summary}", title="WC Merge Skip - Missing Address")
            return False
        
        existing = existing_addresses[0]
        found_summary = f"{existing.address_line1 or ''}|{existing.city or ''}|{existing.state or ''}|{existing.pincode or ''}|{existing.country or ''}"
        
        if found_summary != expected_summary:
            # Identify specific mismatches
            mismatches = []
            fields_to_compare = ['address_line1', 'city', 'state', 'pincode', 'country']
            for field in fields_to_compare:
                exp_val = expected.get(field, '')
                found_val = existing.get(field, '')
                if exp_val.lower() != found_val.lower():  # Case-insensitive comparison
                    mismatches.append(f"{field}: Expected '{exp_val}', Found '{found_val}'")
            
            if mismatches:
                log_message = f"Address mismatch for potential customer {potential_cust['name']}:\n" \
                              f"Expected summary: {expected_summary}\n" \
                              f"Found summary: {found_summary}\n" \
                              f"Specific mismatches:\n" + "\n".join(mismatches)
                #frappe.log_error(message=log_message, title="WC Merge Skip - Address Mismatch")
            return False
        
        return True

    def create_missing_items(self, wc_order, items_list, woocommerce_site):
        """
        Searching for items linked to multiple WooCommerce sites
        """
        for item_data in items_list:
            item_woo_com_id = cstr(item_data.get("variation_id") or item_data.get("product_id"))

            # Deleted items will have a "0" for variation_id/product_id
            if item_woo_com_id != "0":
                woocommerce_product_name = generate_woocommerce_record_name_from_domain_and_id(
                    woocommerce_site, item_woo_com_id
                )
                run_item_sync(woocommerce_product_name=woocommerce_product_name)

    def set_items_in_sales_order(self, new_sales_order, wc_order):
        """
        Customised version of set_items_in_sales_order to allow searching for items linked to
        multiple WooCommerce sites
        """
        wc_server = frappe.get_cached_doc("WooCommerce Server", new_sales_order.woocommerce_server)
        if not wc_server.warehouse:
            frappe.throw(_("Please set Warehouse in WooCommerce Server"))

        # Define tax_template before the loop if needed
        tax_template = None
        if not wc_server.use_actual_tax_type:
            tax_template = frappe.get_cached_doc(
                "Sales Taxes and Charges Template", wc_server.sales_taxes_and_charges_template
            )

        for wc_item in json.loads(wc_order.line_items):
            product_id = wc_item.get("product_id")
            variation_id = wc_item.get("variation_id")
            if variation_id and variation_id != 0:
                woocomm_item_id = f"{product_id}-{variation_id}"
            else:
                woocomm_item_id = product_id

            # Custom logic for splitting WooCommerce ID 125127 (Solid Flat Foam) based on description
            if str(woocomm_item_id) == '125127':
                desc = wc_item.get("name", "").lower()
                if "polyethylene" in desc:
                    woocomm_item_id = "125127-PE"
                elif "polyurethane" in desc:
                    woocomm_item_id = "125127-PU"
                # If neither keyword is found, woocomm_item_id remains unchanged

            # Deleted items will have a "0" for variation_id/product_id
            if woocomm_item_id == 0 or woocomm_item_id == "0":
                found_item = create_placeholder_item(new_sales_order)
                is_drop_ship = 0
                supplier = None
            else:
                iws = frappe.qb.DocType("Item WooCommerce Server")
                itm = frappe.qb.DocType("Item")
                item_codes = (
                    frappe.qb.from_(iws)
                    .join(itm)
                    .on(iws.parent == itm.name)
                    .where(
                        (iws.woocommerce_id == cstr(woocomm_item_id))
                        & (iws.woocommerce_server == new_sales_order.woocommerce_server)
                        & (itm.disabled == 0)
                    )
                    .select(iws.parent)
                    .limit(1)
                ).run(as_dict=True)

                if item_codes:
                    found_item = frappe.get_doc("Item", item_codes[0].parent)
                    # Auto-enable is_sales_item for mapped items to fix validation errors during Sales Order sync.
                    if not found_item.is_sales_item:
                        found_item.is_sales_item = 1
                        found_item.flags.ignore_permissions = True
                        found_item.save()
                        found_item.reload()  # Refresh to ensure updated state
                        
                    # Check for custom_is_drop_ship in Item WooCommerce Server child table
                    is_drop_ship = 0
                    supplier = None
                    for iws in found_item.woocommerce_servers:
                        if iws.woocommerce_server == new_sales_order.woocommerce_server and iws.custom_is_drop_ship:
                            is_drop_ship = 1
                            # Look up the first supplier from Item's suppliers child table
                            if found_item.supplier_items:
                                supplier = found_item.supplier_items[0].supplier
                            break  # Stop after finding the matching server entry
                else:
                    product_name = wc_item.get("name", "Unknown Product")
                    raise WooCommerceMissingItemError(woocomm_item_id, new_sales_order.woocommerce_server, product_name, wc_order.id)

            # Build enhanced description
            base_description = wc_item.get("name") or found_item.item_name  # Start with WooCommerce name or ERPNext fallback
            description = base_description

            # If it's a variation, append attributes from meta_data if not already in base
            if wc_item.get("variation_id", 0) > 0:
                meta_data = wc_item.get("meta_data", [])  # List of dicts
                attributes = []
                for meta in meta_data:
                    if "display_key" in meta and "display_value" in meta:
                        value = meta.get("display_value")
                        key = meta.get("key")
                        if value and value not in base_description and key != "sv_warehouse":
                            attributes.append(value)  # Append the display_value (e.g., "Black")

                if attributes:
                    description += " - " + ", ".join(attributes)

            # Determine warehouse based on sv_warehouse in item meta_data
            item_warehouse = wc_server.warehouse  # Default
            meta_data = wc_item.get("meta_data", [])
            sv_warehouse = next((meta.get("value") for meta in meta_data if meta.get("key") == "sv_warehouse"), None)
            if sv_warehouse == "amazon":
                item_warehouse = wc_server.custom_amazon_warehouse

            new_sales_order_line = {
                "item_code": found_item.name,
                "item_name": found_item.item_name,
                "description": description,
                "delivery_date": new_sales_order.delivery_date,
                "qty": wc_item.get("quantity"),
                "rate": wc_item.get("price")
                if wc_server.use_actual_tax_type or not tax_template.taxes[0].included_in_print_rate
                else get_tax_inc_price_for_woocommerce_line_item(wc_item),
                "warehouse": item_warehouse,
                "delivered_by_supplier": is_drop_ship,
                "supplier": supplier,
                "discount_percentage": 100 if wc_item.get("price") == 0 else 0,
            }

            # Process Order Item Line Field Mappings
            self.set_sales_order_item_fields(woocommerce_order_line_item=wc_item, so_item=new_sales_order_line)

            new_sales_order.append(
                "items",
                new_sales_order_line,
            )

            if wc_server.use_actual_tax_type:
                ordered_items_tax = wc_item.get("total_tax")
                if float(ordered_items_tax or 0) != 0:
                    add_tax_details(new_sales_order, ordered_items_tax, "Ordered Item tax", wc_server.tax_account)

        # Set overall set_warehouse based on first item (assume all same)
        if new_sales_order.items:
            new_sales_order.set_warehouse = new_sales_order.items[0].warehouse

        if not wc_server.use_actual_tax_type:
            new_sales_order.taxes_and_charges = wc_server.sales_taxes_and_charges_template

            # Trigger taxes calculation
            new_sales_order.set_missing_lead_customer_details()

        # If a Shipping Rule is added, shipping charges will be determined by the Shipping Rule. If not, then
        # get it from the WooCommerce Order
        if not new_sales_order.shipping_rule:
            if float(wc_order.shipping_tax or 0) != 0:
                add_tax_details(new_sales_order, wc_order.shipping_tax,"Shipping Tax", wc_server.f_n_f_account) #In CA there is no tax charge on shipping. If however we ever need to charge shipping tax, update the "Shipping Tax Account" here
            
            # NEW: Detect if this is a "ship on own account" order and handle fee separately
            shipping_info = self.get_third_party_shipping_details(wc_order)
            shipping_total = float(wc_order.shipping_total or 0)
            if shipping_info and shipping_total == 5.00:  # Assuming $5 is your fixed handling fee
                if shipping_total != 0:
                    add_tax_details(new_sales_order, shipping_total, "Handling/Packaging Fee", wc_server.custom_package__handling_account)
            else:
                if shipping_total != 0:
                    add_tax_details(new_sales_order, shipping_total,"Shipping Total",wc_server.f_n_f_account)

        # NEW: Handle fee lines (e.g., "Custom Raw Foam Fee")
        fee_lines = json.loads(wc_order.fee_lines) if wc_order.fee_lines else []
        for fee in fee_lines:
            if fee.get("name") == "Custom Raw Foam Fee":
                fee_total = float(fee.get("total", 0))
                fee_tax = float(fee.get("total_tax", 0))
                if fee_total != 0:
                    add_tax_details(new_sales_order, fee_total, "Custom Raw Foam Fee", wc_server.custom_minimum_order_surcharge_account)
                if fee_tax != 0:
                    add_tax_details(new_sales_order, fee_tax, "Custom Raw Foam Fee Tax", wc_server.tax_account)

        # Handle scenario where Woo Order has no items, then manually set the total
        if len(new_sales_order.items) == 0:
            if float(wc_order.cart_tax or 0) != 0:
                add_tax_details(new_sales_order, wc_order.cart_tax, "Cart Tax", wc_server.tax_account)
            new_sales_order.base_grand_total = float(wc_order.total)
            new_sales_order.grand_total = float(wc_order.total)
            new_sales_order.base_rounded_total = float(wc_order.total)
            new_sales_order.rounded_total = float(wc_order.total)
            new_sales_order.set_warehouse = wc_server.warehouse
            
        # Rounding adjustment to match WooCommerce total
        new_sales_order.calculate_taxes_and_totals()  # Explicitly calculate totals
        calculated_grand_total = new_sales_order.get("grand_total")
        wc_grand_total = float(wc_order.total)
        
        # --- PATCH: fix mis-stored totals on split child WooCommerce orders ---
        # Some split "child" orders (e.g. 150289) have the parent-level "Custom Raw Foam Fee"
        # included in wc_order.total even though the fee is not present in this child's
        # own line_items / fee_lines. This causes a Grand Total mismatch when creating
        # the ERPNext Sales Order. To compensate, we recompute the Woo total from the
        # child's own lines and, if that matches ERPNext's grand_total, use it instead.

        try:
            meta_list = json.loads(wc_order.meta_data) if wc_order.meta_data else []
        except (TypeError, json.JSONDecodeError):
            meta_list = []

        order_type = None
        if isinstance(meta_list, list):
            for m in meta_list:
                if isinstance(m, dict) and m.get("key") == "order_type":
                    order_type = m.get("value")
                    break

        if order_type == "child":
            try:
                line_items = json.loads(wc_order.line_items) if wc_order.line_items else []
                shipping_lines = json.loads(wc_order.shipping_lines) if wc_order.shipping_lines else []
                fee_lines = json.loads(wc_order.fee_lines) if wc_order.fee_lines else []
            except (TypeError, json.JSONDecodeError):
                line_items, shipping_lines, fee_lines = [], [], []

            recomputed_total = 0.0

            # Sum this child's own lines
            for li in line_items:
                recomputed_total += float(li.get("total") or 0) + float(li.get("total_tax") or 0)

            for sl in shipping_lines:
                recomputed_total += float(sl.get("total") or 0) + float(sl.get("total_tax") or 0)

            # Only fees actually attached to THIS child (usually none in the bad case)
            for fl in fee_lines:
                recomputed_total += float(fl.get("total") or 0) + float(fl.get("total_tax") or 0)

            # If the recomputed child total matches ERPNext but differs from the Woo stored total,
            # assume a parent-level fee leaked into wc_order.total and override it.
            if (
                abs(recomputed_total - calculated_grand_total) <= 0.01
                and abs(recomputed_total - wc_grand_total) > 0.01
            ):
                wc_grand_total = recomputed_total
        
        diff = wc_grand_total - calculated_grand_total

        wc_server = frappe.get_cached_doc("WooCommerce Server", wc_order.woocommerce_server)
        if abs(diff) > 0.005 and abs(diff) <= 1:
            # Append rounding adjustment as a tax row (add 'rounding_account' field to WooCommerce Server doctype)
            new_sales_order.append(
                "taxes",
                {
                    "charge_type": "Actual",
                    "account_head": wc_server.custom_roundoff_account,
                    "description": "Rounding Adjustment",
                    "tax_amount": diff,
                },
            )
            new_sales_order.calculate_taxes_and_totals()  # Recalculate with adjustment
        elif abs(diff) > 1:
            frappe.throw(_("Grand Total mismatch between WooCommerce ({0}) and ERPNext ({1}) by {2}").format(wc_grand_total, calculated_grand_total, diff))
            
        # Set tax_category based on actual SALES TAX only:
        # Only consider rows posted to wc_server.tax_account (avoid handling/shipping charges in taxes table).
        taxable_cat = getattr(wc_server, "custom_tax_category_taxable", None)
        no_tax_cat = getattr(wc_server, "custom_tax_category_no_tax", None)
        tax_account = getattr(wc_server, "tax_account", None)

        sales_tax_total = 0.0
        if tax_account:
            for t in (new_sales_order.get("taxes") or []):
                # child rows may be dict-like or DocType objects depending on context
                acc = t.get("account_head") if hasattr(t, "get") else getattr(t, "account_head", None)
                amt = t.get("tax_amount") if hasattr(t, "get") else getattr(t, "tax_amount", 0)
                if acc == tax_account:
                    try:
                        sales_tax_total += float(amt or 0)
                    except (TypeError, ValueError):
                        pass

        if sales_tax_total > 0:
            if taxable_cat:
                new_sales_order.tax_category = taxable_cat
        else:
            if no_tax_cat:
                new_sales_order.tax_category = no_tax_cat


    def set_sales_order_item_fields(
        self, woocommerce_order_line_item: Dict, so_item: Union[SalesOrderItem, Dict]
    ) -> Tuple[bool, WooCommerceOrder]:
        """
        If there exist any Order Item Line Field Mappings on `WooCommerce Server`, attempt to set their values from
        the WooCommerce Order Line Item to the ERPNext Sales Order Item

        Returns true if woocommerce_order_line_item was changed
        """
        so_item_dirty = False
        if so_item and self.woocommerce_order:
            wc_server = frappe.get_cached_doc(
                "WooCommerce Server", self.woocommerce_order.woocommerce_server
            )
            if wc_server.order_line_item_field_map:
                for map in wc_server.order_line_item_field_map:
                    erpnext_item_field_name = map.erpnext_field_name.split(" | ")

                    # We expect woocommerce_field_name to be valid JSONPath
                    jsonpath_expr = parse(map.woocommerce_field_name)
                    woocommerce_order_line_item_field_matches = jsonpath_expr.find(woocommerce_order_line_item)

                    if len(woocommerce_order_line_item_field_matches) > 0:
                        if type(so_item) is dict:
                            so_item[erpnext_item_field_name[0]] = woocommerce_order_line_item_field_matches[0].value
                        else:
                            setattr(
                                so_item, erpnext_item_field_name[0], woocommerce_order_line_item_field_matches[0].value
                            )
                            so_item_dirty = True
            return so_item_dirty, so_item

    def create_or_update_address(self, wc_order: WooCommerceOrder):
        """
        If the address(es) exist, update it, else create it
        """
        addresses = get_addresses_linking_to(
            "Customer", self.customer.name, fields=["name", "is_primary_address", "is_shipping_address"]
        )

        existing_billing_address = next(
            (addr for addr in addresses if addr.is_primary_address == 1), None
        )
        existing_shipping_address = next(
            (addr for addr in addresses if addr.is_shipping_address == 1), None
        )

        raw_billing_data = json.loads(wc_order.billing)
        raw_shipping_data = json.loads(wc_order.shipping)

        address_keys_to_compare = [
            "first_name",
            "last_name",
            "company",
            "address_1",
            "address_2",
            "city",
            "state",
            "postcode",
            "country",
        ]
        address_keys_same = [
            True if raw_billing_data[key] == raw_shipping_data[key] else False
            for key in address_keys_to_compare
        ]

        if all(address_keys_same):
            # Use one address for both billing and shipping
            address = existing_billing_address or existing_shipping_address
            if address:
                self.update_address(
                    address.name, raw_billing_data, self.customer, is_primary_address=1, is_shipping_address=1
                )
            else:
                self.create_address(
                    raw_billing_data, self.customer, "Billing", is_primary_address=1, is_shipping_address=1
                )
        else:
            # Handle billing address
            if existing_billing_address:
                self.update_address(
                    existing_billing_address.name,
                    raw_billing_data,
                    self.customer,
                    is_primary_address=1,
                    is_shipping_address=0,
                )
            else:
                self.create_address(
                    raw_billing_data, self.customer, "Billing", is_primary_address=1, is_shipping_address=0
                )

            # Handle shipping address
            if existing_shipping_address:
                self.update_address(
                    existing_shipping_address.name,
                    raw_shipping_data,
                    self.customer,
                    is_primary_address=0,
                    is_shipping_address=1,
                )
            else:
                self.create_address(
                    raw_shipping_data, self.customer, "Shipping", is_primary_address=0, is_shipping_address=1
                )

    def create_address(
        self, raw_data: Dict, customer, address_type, is_primary_address=0, is_shipping_address=0
    ):
        title_convention = frappe.db.get_value(
            "WooCommerce Server", self.woocommerce_order.woocommerce_server, "address_title_convention"
        )
        address = frappe.new_doc("Address")

        address.address_type = address_type
        address.address_line1 = to_proper_case(raw_data.get("address_1", "Not Provided"))
        address.address_line2 = to_proper_case(raw_data.get("address_2", "Not Provided"))
        address.city = to_proper_case(raw_data.get("city", "Not Provided"))
        address.country = frappe.get_value("Country", {"code": raw_data.get("country", "IN").lower()})
        state_value = raw_data.get("state", "")
        if len(state_value) == 2:
            address.state = state_value.upper()
        else:
            address.state = to_proper_case(state_value)
        address.pincode = raw_data.get("postcode")
        country_code = raw_data.get("country", "US")
        phone = raw_data.get("phone")
        address.phone = clean_phone_number(phone, country_code)
        
        # Compute per-address title based on raw_data
        cleaned_company = clean_company_name(raw_data.get("company", "").strip())
        proper_company = to_proper_case(cleaned_company) if cleaned_company else ""
        individual_name = f"{to_proper_case(raw_data.get('first_name', ''))} {to_proper_case(raw_data.get('last_name', ''))}".strip()
        if not individual_name:
            individual_name = raw_data.get("email", "") or "Unknown"  # Fallback if no name/company (rare for shipping)
        address_title_base = proper_company if cleaned_company else individual_name

        if title_convention == "Customer Name only":
            address.address_title = address_title_base
        else:
            address.address_title = f"{address_title_base}-{address.address_type}"
        
        address.is_primary_address = is_primary_address
        address.is_shipping_address = is_shipping_address
        address.append("links", {"link_doctype": "Customer", "link_name": customer.name})

        address.flags.ignore_mandatory = True
        address.save()

    def update_address(
        self, address_name, raw_data: Dict, customer, is_primary_address=0, is_shipping_address=0
    ):
        title_convention = frappe.db.get_value(
            "WooCommerce Server", self.woocommerce_order.woocommerce_server, "address_title_convention"
        )
        address = frappe.get_doc("Address", address_name)

        address.address_line1 = to_proper_case(raw_data.get("address_1", "Not Provided"))
        address.address_line2 = to_proper_case(raw_data.get("address_2", "Not Provided"))
        address.city = to_proper_case(raw_data.get("city", "Not Provided"))
        address.country = frappe.get_value("Country", {"code": raw_data.get("country", "IN").lower()})
        state_value = raw_data.get("state", "")
        if len(state_value) == 2:
            address.state = state_value.upper()
        else:
            address.state = to_proper_case(state_value)
        address.pincode = raw_data.get("postcode")
        country_code = raw_data.get("country", "US")
        phone = raw_data.get("phone")
        address.phone = clean_phone_number(phone, country_code)
        
        # Compute per-address title based on raw_data
        cleaned_company = clean_company_name(raw_data.get("company", "").strip())
        proper_company = to_proper_case(cleaned_company) if cleaned_company else ""
        individual_name = f"{to_proper_case(raw_data.get('first_name', ''))} {to_proper_case(raw_data.get('last_name', ''))}".strip()
        if not individual_name:
            individual_name = raw_data.get("email", "") or "Unknown"  # Fallback if no name/company (rare for shipping)
        address_title_base = proper_company if cleaned_company else individual_name

        if title_convention == "Customer Name only":
            address.address_title = address_title_base
        else:
            address.address_title = f"{address_title_base}-{address.address_type}"
        
        address.is_primary_address = is_primary_address
        address.is_shipping_address = is_shipping_address

        address.flags.ignore_mandatory = True
        address.save()
        
    def create_and_submit_sales_invoice(self, sales_order: SalesOrder) -> bool:
        """
        Create and submit a Sales Invoice from the Sales Order if not already invoiced,
        applying any advances from Payment Entries. Assumes no Delivery Note needed.
        Handles existing drafts by reusing and submitting them to avoid duplicates.
        Returns True if successful.
        """
        # Check if SO is fully billed or SI already exists (submitted)
        if sales_order.per_billed == 100:
            return True  # Already closed out

        existing_submitted_si = frappe.db.exists("Sales Invoice", {"sales_order": sales_order.name, "docstatus": 1})
        if existing_submitted_si:
            return True  # Already invoiced (submitted)

        # Check for existing draft SI linked to this SO
        existing_drafts = frappe.get_all(
            "Sales Invoice",
            filters={"sales_order": sales_order.name, "docstatus": 0},
            pluck="name"
        )
        
        if existing_drafts:
            #if len(existing_drafts) > 1:
            #    frappe.log_error(f"Multiple drafts found for SO {sales_order.name}; using first: {existing_drafts[0]}", "WooCommerce Sync Warning")
            
            # Load the existing draft
            si = frappe.get_doc("Sales Invoice", existing_drafts[0])
            
            # Apply advances (if any)
            advances = si.get_advance_entries()  # Fetches linked advances
            if advances:
                si.set_advances()  # Applies them to the SI
            
            # Set any additional flags/properties (from original logic)
            si.set_posting_time = 1
            si.flags.ignore_mandatory = True
            
            # Submit the draft
            try:
                si.submit()  # Books revenue, applies payment, updates SO to billed/closed
                sales_order.reload()  # Refresh SO to reflect changes
                # Optional: Force SO status to "Completed" if not auto-updated
                if sales_order.status != "Completed":
                    sales_order.db_set("status", "Completed")
                return True
            except Exception as e:
                frappe.log_error(message=f"Failed to submit existing draft SI {si.name} for SO {sales_order.name}: {str(e)}", title="WC SI Submit Error")
                return False
        else:
            # No draft found; create new SI (original logic)
            si = make_sales_invoice(sales_order.name)
            si.set_posting_time = 1  # Use current time
            si.flags.ignore_mandatory = True  # Bypass mandatory fields if needed

            # Apply advances from submitted PEs
            advances = si.get_advance_entries()  # Fetches linked advances
            if advances:
                si.set_advances()  # Applies them to SI

            try:
                si.insert()
                si.submit()  # Books revenue, applies payment, updates SO to billed/closed
                sales_order.reload()  # Refresh SO to reflect changes
                # Optional: Force SO status to "Completed" if not auto-updated
                if sales_order.status != "Completed":
                    sales_order.db_set("status", "Completed")
                return True
            except Exception as e:
                frappe.log_error(message=f"Failed to create/submit SI for SO {sales_order.name}: {str(e)}", title="WC SI Creation Error")
                return False     


def get_list_of_wc_orders(
    date_time_from: Optional[datetime] = None,
    sales_order: Optional[SalesOrder] = None,
    status: Optional[str] = None,
):
    """
    Fetches a list of WooCommerce Orders within a specified date range or linked with a Sales Order, using pagination.

    At least one of date_time_from, or sales_order parameters are required
    """
    if not any([date_time_from, sales_order]):
        raise ValueError("At least one of date_time_from or sales_order parameters are required")

    wc_records_per_page_limit = 100
    page_length = wc_records_per_page_limit
    new_results = True
    start = 0
    filters = []
    wc_orders = []

    wc_settings = frappe.get_cached_doc("WooCommerce Integration Settings")
    minimum_creation_date = wc_settings.minimum_creation_date

    # Build filters
    if date_time_from:
        filters.append(["WooCommerce Order", "date_modified", ">", date_time_from])
    if minimum_creation_date:
        filters.append(["WooCommerce Order", "date_created", ">", minimum_creation_date])
    if sales_order:
        filters.append(["WooCommerce Order", "id", "=", sales_order.custom_woocommerce_order_id])
    if status:
        filters.append(["WooCommerce Order", "status", "=", status])

    while new_results:
        woocommerce_order = frappe.get_doc({"doctype": "WooCommerce Order"})
        new_results = woocommerce_order.get_list(
            args={"filters": filters, "page_length": page_length, "start": start, "as_doc": True}
        )
        for wc_order in new_results:
            wc_orders.append(wc_order)
        start += page_length
        if len(new_results) < page_length:
            new_results = []

    return wc_orders


def rename_address(address, customer):
    old_address_title = address.name
    new_address_title = customer.name + "-" + address.address_type
    address.address_title = customer.customer_name
    address.save()

    frappe.rename_doc("Address", old_address_title, new_address_title)


def create_contact(data, customer):
    email = data.get("email", None)
    phone = data.get("phone", None)
    country = data.get("country", "US")
    cleaned_phone = clean_phone_number(phone, country) if phone else None
    normalized_phone = normalize_phone(phone) if phone else ""
    first_name = data.get("first_name", "").title()
    last_name = data.get("last_name", "").title()

    # Get all contacts linked to this customer
    linked_contacts = get_contacts_linking_to("Customer", customer.name, fields=["name"])

    matching_contact = None
    for lc in linked_contacts:
        contact = frappe.get_doc("Contact", lc.name)
        # Check for matching email or phone
        has_email = any(e.email_id == email for e in contact.email_ids) if email else False
        has_phone = any(normalize_phone(p.phone) == normalized_phone for p in contact.phone_nos) if normalized_phone else False
        if (email and has_email) or (normalized_phone and has_phone):
            matching_contact = contact
            break

    if matching_contact:
        # Update existing contact if necessary
        dirty = False
        if matching_contact.first_name != first_name:
            matching_contact.first_name = first_name
            dirty = True
        if matching_contact.last_name != last_name:
            matching_contact.last_name = last_name
            dirty = True
        matching_contact.is_primary_contact = 1
        matching_contact.is_billing_contact = 1
        # Add email/phone if missing
        if email and not any(e.email_id == email for e in matching_contact.email_ids):
            matching_contact.add_email(email, is_primary=1)
            dirty = True
        if cleaned_phone and not any(normalize_phone(p.phone) == normalized_phone for p in matching_contact.phone_nos):
            matching_contact.add_phone(cleaned_phone, is_primary_mobile_no=1, is_primary_phone=1)
            dirty = True
        if dirty:
            matching_contact.flags.ignore_mandatory = True
            matching_contact.save()
    else:
        # Create new contact
        contact = frappe.new_doc("Contact")
        contact.first_name = first_name
        contact.last_name = last_name
        contact.is_primary_contact = 1
        contact.is_billing_contact = 1
        if cleaned_phone:
            contact.add_phone(cleaned_phone, is_primary_mobile_no=1, is_primary_phone=1)
        if email:
            contact.add_email(email, is_primary=1)
        contact.append("links", {"link_doctype": "Customer", "link_name": customer.name})
        contact.flags.ignore_mandatory = True
        contact.save()
        matching_contact = contact

    return matching_contact


def add_tax_details(sales_order, price, desc, tax_account_head):
    sales_order.append(
        "taxes",
        {
            "charge_type": "Actual",
            "account_head": tax_account_head,
            "tax_amount": price,
            "description": desc,
        },
    )


def get_tax_inc_price_for_woocommerce_line_item(line_item: Dict):
    """
    WooCommerce's Line Item "price" field will always show the tax excluding amount.
    This function calculates the tax inclusive rate for an item
    """
    return (float(line_item.get("subtotal")) + float(line_item.get("subtotal_tax"))) / float(
        line_item.get("quantity")
    )


def create_placeholder_item(sales_order: SalesOrder):
    """
    Create a placeholder Item for deleted WooCommerce Products
    """
    wc_server = frappe.get_cached_doc("WooCommerce Server", sales_order.woocommerce_server)
    if not frappe.db.exists("Item", "DELETED_WOOCOMMERCE_PRODUCT"):
        item = frappe.new_doc("Item")
        item.item_code = "DELETED_WOOCOMMERCE_PRODUCT"
        item.item_name = "Deletet WooCommerce Product"
        item.description = "Deletet WooCommerce Product"
        item.item_group = "All Item Groups"
        item.stock_uom = wc_server.uom
        item.is_stock_item = 0
        item.is_fixed_asset = 0
        item.opening_stock = 0
        item.flags.created_by_sync = True
        item.save()
    else:
        item = frappe.get_doc("Item", "DELETED_WOOCOMMERCE_PRODUCT")
    return item


def get_addresses_linking_to(doctype, docname, fields=None):
    """Return a list of Addresses containing a link to the given document."""
    return frappe.get_all(
        "Address",
        fields=fields,
        filters=[
            ["Dynamic Link", "link_doctype", "=", doctype],
            ["Dynamic Link", "link_name", "=", docname],
        ],
    )

def get_contacts_linking_to(doctype, docname, fields=None):
    """Return a list of Contacts containing a link to the given document."""
    return frappe.get_all(
        "Contact",
        fields=fields or ["name"],
        filters=[
            ["Dynamic Link", "link_doctype", "=", doctype],
            ["Dynamic Link", "link_name", "=", docname],
            ["Dynamic Link", "parenttype", "=", "Contact"],
        ],
    )

