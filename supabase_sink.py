#!/usr/bin/env python3
"""
GRN -> Supabase sink.

Same extraction pipeline as app.py (Drive PDFs -> LlamaExtract), but the rows land
in Supabase instead of Google Sheets. app.py is never modified: this file finds the
automation class in app.py by duck typing and reuses its Drive/LlamaExtract methods.

This file is IDENTICAL across every scheduler repo. What differs is `.env`, which
names the source via GRN_SOURCE; the per-source table and column types live in the
SOURCES registry below. To sync a fix, copy this file over the others verbatim.

Local testing (run these in order before touching GitHub Actions):

    python supabase_sink.py --list-sources        # sources and their tables
    python supabase_sink.py --print-schema        # paste the SQL into Supabase SQL editor
    python supabase_sink.py --check               # config + connectivity + tables exist
    python supabase_sink.py --self-test           # insert/read/delete a synthetic row
    python supabase_sink.py --run --dry-run --limit 2 --dump-json rows.json
    python supabase_sink.py --from-json rows.json # insert those rows for real
    python supabase_sink.py --run --limit 2       # full path on 2 real PDFs

Configuration comes from the environment (a .env file next to this script is
loaded automatically and never overrides real environment variables):

    GRN_SOURCE=instamart                       # which scheduler this repo runs
    SUPABASE_URL=https://xxxxxxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...      # service role: bypasses RLS for writes
    LLAMA_CLOUD_API_KEY=llx-...                # optional, falls back to app.py CONFIG
    SUPABASE_GRN_TABLE=...                     # optional, overrides the source's table
    SUPABASE_LOG_TABLE=workflow_logs           # optional

Adding a column: extend the SOURCES entry, then run --print-schema and apply the
`alter table` it emits. Until then the field is still queryable as raw_data->>'key'.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime, date, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:  # pragma: no cover - dependency check surfaced by --check
    SUPABASE_AVAILABLE = False
    Client = Any  # type: ignore

logger = logging.getLogger("supabase_sink")

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')

DEFAULT_LOG_TABLE = 'workflow_logs'


class SourceSpec:
    """Per-scheduler table definition.

    Columns listed here get real Postgres types; every other key the extractor
    emits still reaches the database inside `raw_data` (jsonb), so a new or
    renamed field never fails a run and nothing is silently dropped. Query an
    untyped field with  select raw_data->>'some_key' ...  and, once it proves
    stable, promote it to a typed column here plus an `alter table ... add column`.
    """

    def __init__(self, table: str, text: Sequence[str] = (), date: Sequence[str] = (),
                 numeric: Sequence[str] = (), drop: Sequence[str] = (),
                 rename: Optional[Dict[str, str]] = None, note: str = '',
                 kind: str = 'pdf', dedupe: Sequence[str] = ()):
        #: 'pdf'  -> Drive PDFs through LlamaExtract (process_extracted_data)
        #: 'excel'-> Drive spreadsheets through pandas (_read_excel_file*)
        self.kind = kind
        #: Natural key the original script deduped on, as normalized column names.
        #: Excel scripts dedupe the whole sheet on these and keep the first row;
        #: run_excel_pipeline reproduces that instead of relying on row_hash alone.
        self.dedupe = tuple(dedupe)
        self.table = table
        self.text = list(text)
        self.date = list(date)
        self.numeric = list(numeric)
        # Alias keys that exist only to match a Google Sheet header
        # ('ord.qty', 'tax amount', ...). They duplicate a canonical column and
        # would need quoting in SQL, so they are dropped before the row is built.
        self.drop = set(drop)
        self.rename = dict(rename or {})
        self.note = note

    @property
    def columns(self) -> List[str]:
        return self.text + self.date + self.numeric

    def normalize_key(self, key: str) -> str:
        return self.rename.get(key, key)


# Item-level fields produced by the shared 'More retail Agent', used by both the
# aws (mraws) and doc (mrgrn) schedulers.
_MORE_RETAIL_ITEM_TEXT = ['item_description', 'uom', 'sku', 'variant_ean', 'hsn_code']
_MORE_RETAIL_ITEM_NUMERIC = ['rcv_qty', 'ord_qty', 'unit_cost', 'tax_amount',
                             'tax_percentage', 'mrp', 'net_value']
# The dotted/spaced duplicates exist only as sheet headers.
_MORE_RETAIL_DROP = ['grndate', 'ord.qty', 'variant.ean', 'tax amount']

_COMMON_TEXT = ['source_file', 'drive_file_id']

SOURCES: Dict[str, SourceSpec] = {
    'instamart': SourceSpec(
        table='instamart_grn',
        text=['vendor_name', 'po_number', 'grn_no', 'invoice_no', 'sku_code',
              'sku_description', 'vendor_sku', 'sku_bin', 'lot_no'] + _COMMON_TEXT,
        date=['po_date', 'grn_date', 'invoice_date'],
        numeric=['lot_mrp', 'exp_qty', 'recv_qty', 'unit_price',
                 'taxable_value', 'add_cess', 'total_inr'],
    ),
    'mraws': SourceSpec(
        table='mraws_grn',
        text=['supplier', 'po_number', 'vendor_invoice_number',
              'shipping_address'] + _MORE_RETAIL_ITEM_TEXT + _COMMON_TEXT,
        date=['grn_date'],
        numeric=list(_MORE_RETAIL_ITEM_NUMERIC),
        drop=_MORE_RETAIL_DROP,
        note='aws-grn-scheduler. Item keys pass through verbatim in app.py, so the '
             'item columns here are inferred from the shared More retail Agent; '
             'anything unexpected lands in raw_data.',
    ),
    'mrgrn': SourceSpec(
        table='mrgrn_grn',
        text=['supplier', 'po_number', 'vendor_invoice_number',
              'shipping_address'] + _MORE_RETAIL_ITEM_TEXT + _COMMON_TEXT,
        date=['grn_date'],
        numeric=list(_MORE_RETAIL_ITEM_NUMERIC),
        drop=_MORE_RETAIL_DROP,
        note='doc_grn_scheduler. Same More retail Agent as mraws, so same columns.',
    ),
    'hyperpure': SourceSpec(
        table='hyperpure_grn',
        text=['supplier', 'po_number', 'vendor_invoice_number', 'shipping_address',
              'product_no', 'product_name', 'hsn', 'uom', 'gst_rate'] + _COMMON_TEXT,
        date=['grn_date'],
        numeric=['qty_ordered', 'qty_delivered', 'grn_qty', 'damaged_qty',
                 'price_per_unit', 'total_tax_amount', 'amount'],
        rename={'Total Tax Amount': 'total_tax_amount'},
        note='hyperpure_grn_scheduler. Item columns promoted from --run --dry-run '
             '--dump-json over real GRNs. gst_rate stays text because the agent emits '
             "it as '0%'. The Hyperpure Agent emits no supplier key, so that column "
             'stays empty until the agent schema grows one.',
    ),
    'reliance': SourceSpec(
        table='reliance_grn',
        text=['supplier', 'po_number', 'vendor_invoice_number', 'shipping_address',
              'grn_number'] + _COMMON_TEXT,
        date=['grn_date'],
        note='reliance. app.py stamps the header keys typed here onto every line '
             'item and then passes the Reliance Agent item keys through verbatim, '
             'so the item columns are untyped for now. Promote them with '
             "--run --dry-run --dump-json rows.json over a few real GRNs; until "
             "then each one is queryable as raw_data->>'key'.",
    ),
    'nbgrn': SourceSpec(
        table='nb_grn',
        text=['document_type', 'grn_no', 'invoice_no', 'po_no', 'p_slip_no',
              'lr_rr_no', 'gin_no', 'waybill', 'receiver_company_name',
              'receiving_location_name', 'receiving_location_address',
              'receiving_location_gst', 'vendor_code', 'vendor_name',
              'vendor_address', 'line_item_s_no', 'article_code', 'ean_code',
              'description', 'hsn_code', 'free_item', 'uom',
              'line_item_remarks'] + _COMMON_TEXT,
        date=['grn_date', 'invoice_date', 'po_date', 'gin_date'],
        numeric=['invoice_value', 'invoice_tax_value', 'gst_percentage',
                 'gst_vat_value', 'received_quantity', 'accepted_quantity',
                 'rejected_quantity', 'mrp', 'total_cost_value',
                 'summary_total_gst_vat_value', 'summary_total_received_quantity',
                 'summary_total_accepted_quantity', 'summary_total_rejected_quantity',
                 'summary_total_cost_value', 'summary_gross_value'],
        note="nbgrn (Spencer's GRN). app_nb.py builds every row key by hand in "
             '_flatten_json, so these columns are the complete set; only '
             'source_file/drive_file_id are added by the caller. line_item_s_no '
             'and free_item stay text because the agent emits them as labels.',
    ),
    'nbprn': SourceSpec(
        table='nb_prn',
        text=list(_COMMON_TEXT),
        note="nbprn (Spencer's NRGP/RTV). app_nbprn.py flattens whatever the NB PRN "
             'agent returns (header keys verbatim, line-item keys prefixed item_), '
             'so no column list can be read off the code. Every key still lands in '
             "raw_data and is queryable as raw_data->>'key'; run "
             '--run --dry-run --dump-json rows.json over a few real PRNs and promote '
             'the stable keys here plus an `alter table ... add column`.',
    ),
    'milkbasket': SourceSpec(
        table='milkbasket_grn',
        text=['vendor_name', 'supplier', 'po_number', 'grn_number',
              'vendor_invoice_number', 'article', 'shipping_addr', 'sku_code',
              'sku_description', 'item_description', 'vendor_sku', 'sku_bin',
              'lot_no', 'uom'] + _COMMON_TEXT,
        date=['po_date', 'grn_date', 'invoice_date'],
        numeric=['received_qty', 'challan_qty', 'lot_mrp', 'exp_qty', 'recv_qty',
                 'accepted_qty', 'unit_price', 'taxable_value', 'add_cess',
                 'total_inr'],
        note='milkbasket_grn_scheduler (Milkbasket/RIL). Columns are the target '
             'names of the field_mappings/item_mappings tables in app.py '
             'process_extracted_data, so they are the complete mapped set. That '
             'method also copies through any unmapped item key verbatim; those '
             "still land in raw_data and are queryable as raw_data->>'key'.",
    ),
    # ---- flipkart_grn -----------------------------------------------------
    # The one repo holding four schedulers side by side, one per Flipkart
    # vendor. Each job passes both --app and --source, since neither the app
    # file nor the source can be inferred there.
    'flipkart_cb': SourceSpec(
        table='flipkart_cb_grn',
        text=['document_type', 'po_number', 'payment_terms',
              'billing_company_name', 'billing_street_address', 'billing_city',
              'billing_state', 'billing_postal_code', 'billing_gstin', 'billing_pan',
              'shipping_company_name', 'shipping_street_address', 'shipping_city',
              'shipping_state', 'shipping_postal_code',
              'item_description', 'uom'] + _COMMON_TEXT,
        date=['po_date', 'release_date', 'expected_delivery_date', 'po_expiry_date'],
        numeric=['po_quantity', 'grn_quantity', 'final_quantity', 'rate',
                 'taxable_amount', 'gst_percentage', 'igst_amount', 'total_amount',
                 'total_po_amount', 'total_gst_amount', 'grand_total_amount'],
        note='flipkart_grn / app_cp_grn.py, agent "Flipkart Crop Basket GRN", '
             'sheet tab cbgrn. Columns are app_cp_grn.py STATIC_POS_HEADERS, which '
             'that script documents as fixed regardless of what the agent returns. '
             'Postal codes stay text so a leading zero survives.',
    ),
    'flipkart_ppr': SourceSpec(
        table='flipkart_ppr_grn',
        text=['invoice_number', 'supplier_invoice_number', 'other_references',
              'customer_name', 'customer_address', 'customer_gstin_uin',
              'customer_state_name', 'customer_state_code',
              'supplier_name', 'supplier_address', 'supplier_gstin_uin',
              'supplier_state_name', 'supplier_state_code',
              'sl_no', 'description_of_goods', 'unit',
              'amount_chargeable_in_words', 'deduction_description',
              'company_gstin_uin_at_footer',
              'company_for_authorized_signatory'] + _COMMON_TEXT,
        date=['invoice_date', 'supplier_invoice_date'],
        numeric=['quantity', 'rate', 'amount', 'sub_total_amount',
                 'total_quantity_of_items', 'total_amount_payable',
                 'deduction_percentage', 'deduction_amount'],
        note='flipkart_grn / app_fp_grn.py, agent "Flipkart PRR GRN", sheet tab '
             'pprgrn. Columns are app_fp_grn.py STATIC_POS_HEADERS. These are tax '
             'invoices rather than GRNs, so the header fields are invoice-shaped. '
             'sl_no and the state codes stay text because they are labels.',
    ),
    'fatema': SourceSpec(
        table='fatema_grn',
        text=['bill_no', 'place_of_supply', 'po_number',
              'bill_from_name', 'bill_from_address', 'bill_from_contact_no',
              'bill_from_gstin', 'bill_from_state',
              'ship_from_name', 'ship_from_address', 'ship_from_pin',
              'ship_from_state', 'ship_from_gstin', 'ship_from_pan',
              'item_name', 'item_code', 'hsn_sac', 'fsn_no',
              'bill_description'] + _COMMON_TEXT,
        date=['bill_date', 'po_date'],
        numeric=['quantity', 'price_per_unit', 'taxable_amount',
                 'cgst_amount', 'cgst_percentage', 'sgst_amount', 'sgst_percentage',
                 'amount', 'total_quantity', 'total_taxable_amount',
                 'total_cgst', 'total_sgst', 'sub_total', 'grand_total'],
        note='flipkart_grn / app_fat_grn.py, agent "Fatema GRN", sheet tab '
             'fatemagrn. Columns are app_fat_grn.py PREFERRED_HEADERS; that script '
             'appends any unexpected extractor key after them, and those still '
             'reach raw_data here. ship_from_pin stays text (leading zeros).',
    ),
    'slveggies': SourceSpec(
        table='slveggies_grn',
        text=['bill_number', 'invoice_number', 'po_number',
              'buyer_name', 'buyer_address', 'buyer_gstin', 'buyer_state',
              'serial_number', 'item_name', 'fsn_number'] + _COMMON_TEXT,
        date=['bill_date', 'po_date'],
        numeric=['indent_quantity', 'received_quantity', 'return_quantity',
                 'grn_quantity', 'price_per_unit', 'tax_rate_percentage', 'amount'],
        note='flipkart_grn / app_slv_grn.py, agent "SL Veggies GRN", sheet tab '
             'slveggiesgrn. Columns are app_slv_grn.py PREFERRED_HEADERS. '
             'serial_number stays text because it is a line label, not a measure.',
    ),
}

# ---- Excel schedulers ------------------------------------------------------
# These read vendor spreadsheets whose headers are not visible from the code, so
# only the columns the scripts actually name (the dedupe keys) are typed up front.
# Everything else lands in raw_data and stays queryable as raw_data->>'key'.
# Run --discover-columns against real files to generate the full typed schema.

_EXCEL_NOTE = ('Columns discovered from real vendor files via --discover-columns. Any '
               'header not listed here still reaches the database in raw_data, so a '
               'new vendor column never fails a run.')

SOURCES.update({
    'hot': SourceSpec(
        kind='excel',
        table='hot_grn',
        text=['item_code', 'po_number', 'product_upc', 'product_description',
              'landing_rate_grn', 'fill_rate'] + _COMMON_TEXT,
        numeric=['mrp', 'tax_amount', 'landing_rate_po', 'quantity_po',
                 'quantity_grn', 'total_grn_amount', 'gmv_loss'],
        dedupe=('po_number', 'item_code'),
        note='HOT-Automation (Blinkit). Sheet dedupe: po_number + Item Code. '
             + _EXCEL_NOTE,
    ),
    'bbalert': SourceSpec(
        kind='excel',
        table='bb_alert_grn',
        text=['po_no', 'grn_no', 'supplier_id', 'supplier_name', 'ship_to_name',
              'ship_to_address', 'invoice_no', 'sku_code', 'sku_description',
              'hsn_code', 'rejection_id', 'rejected_reason'] + _COMMON_TEXT,
        date=['grn_timestamp'],
        numeric=['mrp', 'cost_price_incl_tax', 'sgst', 'cgst', 'igst', 'cess',
                 'cess_per_unit', 'accepted_quantity', 'accepted_sgst_value',
                 'accepted_cgst_value', 'accepted_igst_value', 'accepted_cess_value',
                 'accepted_cess_per_unit_value', 'accepted_tax_value',
                 'accepted_value_incl_tax', 'rejected_quantity', 'rejected_sgst_value',
                 'rejected_cgst_value', 'rejected_igst_value', 'rejected_cess_value',
                 'rejected_cess_per_unit_value', 'rejected_tax_value',
                 'rejected_value_incl_tax'],
        dedupe=('po_no', 'sku_code'),
        note='bb_alert_scheduler. Sheet dedupe: PO No + Sku Code. ' + _EXCEL_NOTE,
    ),
    'bbnet': SourceSpec(
        kind='excel',
        table='bb_net_grn',
        text=['pono', 'grnno', 'suppliercode', 'suppliername', 'shiptocode',
              'shiptoname', 'shiptoaddress', 'invoiceno', 'hsn_code', 'skucode',
              'skudesc'] + _COMMON_TEXT,
        date=['podate', 'invoicedate'],
        numeric=['mrp', 'sgst', 'sgst_value', 'cgst', 'cgst_value', 'igst',
                 'igst_value', 'gst', 'gst_amount', 'cess', 'cess_value',
                 'landingcost', 'quantity', 'totalvalue'],
        dedupe=('pono', 'skucode'),
        note='bb_net_scheduler. Sheet dedupe: PoNo + Skucode. NOTE: the Drive folder '
             'has had no new file since 2026-01-15. ' + _EXCEL_NOTE,
    ),
    'bbprn': SourceSpec(
        kind='excel',
        table='bb_net_prn',
        text=['pono', 'gonno', 'suppliercode', 'suppliername', 'shiptocode',
              'shiptoname', 'shiptoaddress', 'invoiceno', 'hsn_code', 'skucode',
              'skudesc'] + _COMMON_TEXT,
        date=['invoicedate'],
        numeric=['mrp', 'sgst', 'sgst_value', 'cgst', 'cgst_value', 'igst',
                 'igst_value', 'gst', 'gst_amount', 'cess', 'cess_value',
                 'additional_cess_per_piece', 'total_additional_cess', 'landingcost',
                 'quantity', 'totalvalue'],
        dedupe=('gonno', 'skucode'),
        note='bb_net_scheduler_prn. app.py deduped on PoNo + Skucode, but real PRN '
             'files carry gonno and no pono, so that key never fired there (its guard '
             'checks the column exists) and every re-read leaned on row_hash alone. '
             'Deduped on GonNo + Skucode here so a PRN line is stored once. '
             + _EXCEL_NOTE,
    ),
})


def normalize_column(name: Any) -> str:
    """Turn a spreadsheet header into a safe Postgres identifier.

    'Item Code' -> item_code, 'PO No' -> po_no, 'Qty.' -> qty, '' -> column.
    Deliberately no camelCase splitting: 'PoNo' -> pono, predictably.
    """
    text = str(name).strip().lower() if name is not None else ''
    text = re.sub(r'[^a-z0-9]+', '_', text).strip('_')
    text = re.sub(r'_{2,}', '_', text)
    if not text:
        text = 'column'
    if text[0].isdigit():
        text = f'col_{text}'
    return text[:63]


def normalize_columns(names: Sequence[Any]) -> List[str]:
    """Normalize headers, suffixing collisions so every column stays distinct."""
    seen: Dict[str, int] = {}
    out: List[str] = []
    for name in names:
        base = normalize_column(name)
        count = seen.get(base, 0)
        seen[base] = count + 1
        out.append(base if count == 0 else f'{base}_{count + 1}')
    return out


def get_spec(name: Optional[str] = None) -> SourceSpec:
    key = (name or os.environ.get('GRN_SOURCE', 'instamart')).strip().lower()
    if key not in SOURCES:
        raise SystemExit(
            f"Unknown GRN_SOURCE '{key}'. Known sources: {', '.join(sorted(SOURCES))}.\n"
            'Set GRN_SOURCE in .env to the scheduler this repo runs.')
    return SOURCES[key]

# Date formats seen in Indian GRN/invoice PDFs. Tried in order; first hit wins.
DATE_FORMATS = [
    '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y', '%Y/%m/%d',
    '%d-%b-%Y', '%d %b %Y', '%d-%B-%Y', '%d %B %Y',
    '%d-%b-%y', '%d-%m-%y', '%d/%m/%y',
]

def build_schema_sql(spec: "SourceSpec", log_table: str = DEFAULT_LOG_TABLE) -> str:
    """Generate the DDL for one source. Idempotent: safe to re-run on a live table."""
    t = spec.table
    width = max([len(c) for c in spec.columns] + [16]) + 2

    def col(name: str, sql_type: str) -> str:
        return f"    {name:<{width}}{sql_type},"

    lines = [f"-- {t}: schema for GRN_SOURCE={_source_name(spec)}.",
             "-- Run in the Supabase SQL editor. Safe to re-run.", ""]
    if spec.note:
        for chunk in _wrap(spec.note, 76):
            lines.append(f"-- {chunk}")
        lines.append("")

    lines.append(f"create table if not exists public.{t} (")
    lines.append(col('id', 'bigint generated always as identity primary key'))
    lines.append(col('row_hash', 'text        not null unique'))
    for name in spec.text:
        lines.append(col(name, 'text        not null' if name == 'source_file' else 'text'))
    for name in spec.date:
        lines.append(col(name, 'date'))
    for name in spec.numeric:
        lines.append(col(name, 'numeric'))
    lines.append(col('processed_at', 'timestamptz not null default now()'))
    lines.append(col('raw_data', 'jsonb'))
    lines.append(f"    {'created_at':<{width}}timestamptz not null default now()")
    lines.append(");")
    lines.append("")
    lines.append("-- Case-insensitive dedupe key. PostgREST's in_() filter is case-sensitive,")
    lines.append("-- so lookups run against this generated column.")
    lines.append(f"alter table public.{t}")
    lines.append("    add column if not exists source_file_lower text")
    lines.append("    generated always as (lower(source_file)) stored;")
    lines.append("")
    lines.append(f"comment on column public.{t}.row_hash is")
    lines.append("    'sha256 of the extracted line item; makes re-runs idempotent via upsert';")
    lines.append(f"comment on column public.{t}.raw_data is")
    lines.append("    'full extractor output, including keys with no typed column and values")
    lines.append("     that failed date/number parsing';")
    lines.append("")
    lines.append(f"create index if not exists {t}_source_file_lower_idx on public.{t} (source_file_lower);")
    for name in spec.date:
        lines.append(f"create index if not exists {t}_{name}_idx on public.{t} ({name});")
    lines.append(f"create index if not exists {t}_raw_data_idx on public.{t} using gin (raw_data);")
    lines.append("")
    lines.append(f"create table if not exists public.{log_table} (")
    lines.append("    id               bigint generated always as identity primary key,")
    lines.append("    source           text,")
    lines.append("    workflow         text        not null,")
    lines.append("    started_at       timestamptz not null,")
    lines.append("    ended_at         timestamptz not null,")
    lines.append("    duration_seconds numeric,")
    lines.append("    duration_text    text,")
    lines.append("    processed        integer     default 0,")
    lines.append("    total_items      integer     default 0,")
    lines.append("    failed           integer     default 0,")
    lines.append("    skipped          integer     default 0,")
    lines.append("    status           text,")
    lines.append("    details          jsonb,")
    lines.append("    created_at       timestamptz not null default now()")
    lines.append(");")
    lines.append("")
    lines.append(f"alter table public.{log_table} add column if not exists source text;")
    lines.append(f"create index if not exists {log_table}_started_at_idx on public.{log_table} (started_at desc);")
    lines.append("")
    lines.append("-- Writes use the service role key, which bypasses RLS. Keeping RLS on means")
    lines.append("-- the anon/public key cannot read or write these tables.")
    lines.append(f"alter table public.{t} enable row level security;")
    lines.append(f"alter table public.{log_table} enable row level security;")
    return "\n".join(lines) + "\n"


def _source_name(spec: "SourceSpec") -> str:
    for name, candidate in SOURCES.items():
        if candidate is spec:
            return name
    return spec.table


def _wrap(text: str, width: int) -> List[str]:
    words, out, line = text.split(), [], ''
    for word in words:
        if line and len(line) + 1 + len(word) > width:
            out.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        out.append(line)
    return out



# --------------------------------------------------------------------------- #
# environment
# --------------------------------------------------------------------------- #

def load_dotenv(path: str = ENV_FILE) -> None:
    """Minimal .env loader. Real environment variables always win."""
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def normalize_supabase_url(url: str) -> str:
    """Reduce a pasted URL to the project root the client expects.

    The Supabase dashboard shows the REST endpoint as
    https://<ref>.supabase.co/rest/v1/, but create_client appends /rest/v1
    itself -- passing the full endpoint yields /rest/v1/rest/v1 and PGRST125.
    """
    cleaned = (url or '').strip().rstrip('/')
    for suffix in ('/rest/v1', '/rest'):
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].rstrip('/')
    return cleaned


def mask(secret: Optional[str]) -> str:
    if not secret:
        return '<not set>'
    if len(secret) <= 12:
        return '*' * len(secret)
    return f"{secret[:6]}...{secret[-4:]} (len {len(secret)})"


# --------------------------------------------------------------------------- #
# value coercion
# --------------------------------------------------------------------------- #

# '155159.0' -> '155159'. Only a pure integer with a zero fraction matches, so a
# real code that happens to contain a dot is left alone.
_TRAILING_ZERO_DECIMAL = re.compile(r'^(-?\d+)\.0+$')


def to_text(value: Any) -> Optional[str]:
    """Stringify a value for a text column.

    Excel reads codes like PoNo, GonNo and SupplierCode as floats, so a plain
    str() would store '155159.0' and break every join on that code. An integral
    float is written as the integer it is. Money is unaffected: those are numeric
    columns and go through to_number().
    """
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e15:
        text = str(int(value))
    else:
        text = _TRAILING_ZERO_DECIMAL.sub(r'\1', str(value).strip())
    if text == '' or text.lower() in {'n/a', 'na', 'null', 'none', '-'}:
        return None
    return text


def to_number(value: Any) -> Optional[float]:
    """Parse '1,234.50', '₹1,234', '(45)' -> float. Unparseable -> None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    negative = text.startswith('(') and text.endswith(')')

    # Match the first numeric token rather than stripping characters, so a
    # prefix like 'Rs. 1,000' does not leave a stray '.' and parse as 0.1.
    # The lookbehind keeps 'GRN-123' from reading as -123.
    match = re.search(r'(?<![\d\w])-?\d+(?:\.\d+)?', text.replace(',', ''))
    if not match:
        return None
    try:
        number = float(match.group())
    except ValueError:
        return None
    return -abs(number) if negative else number


def to_date(value: Any) -> Optional[str]:
    """Parse a GRN/invoice date into an ISO date string. Unparseable -> None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    # Drop a trailing time component: '2025-08-12T09:30:00' / '12/08/2025 09:30'
    text = re.split(r'[T ]', text)[0].strip() if re.search(r'\d[T ]\d{1,2}:', text) else text

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    # Only now, having failed every format above, try the wordier shapes some
    # agents emit -- e.g. flipkart_cb/flipkart_ppr return
    # 'Fri, 7th Aug 2026 9:59 AM'. Handled as a second pass, never a first one,
    # so any string that already parsed keeps its exact previous result and no
    # existing row's hash moves.
    loose = str(value).strip()
    loose = re.sub(r'^(mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)[a-z]*\.?,?\s*',
                   '', loose, flags=re.I)                       # 'Fri, ' prefix
    loose = re.sub(r'\s*\d{1,2}:\d{2}(:\d{2})?\s*([ap]\.?m\.?)?\s*$',
                   '', loose, flags=re.I)                       # trailing clock time
    loose = re.sub(r'(?<=\d)(st|nd|rd|th)\b', '', loose, flags=re.I)  # '7th' -> '7'
    loose = re.sub(r'[\s,]+', ' ', loose).strip()
    if loose and loose != text:
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(loose, fmt).date().isoformat()
            except ValueError:
                continue
    return None


def row_hash(payload: Dict[str, Any], occurrence: int) -> str:
    """Deterministic id for a line item so re-runs upsert instead of duplicating.

    `occurrence` distinguishes genuinely repeated identical lines in one PDF.
    """
    canonical = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(f"{canonical}#{occurrence}".encode('utf-8')).hexdigest()


def to_supabase_row(extracted_row: Dict[str, Any], occurrence: int,
                    spec: SourceSpec) -> Dict[str, Any]:
    """Map one row from a scheduler's process_extracted_data to a table row.

    Keys with no typed column in `spec` are not lost: the full extractor output
    is stored in raw_data.
    """
    # Apply the spec's alias renames and drop sheet-header duplicates first.
    normalized = {}
    for key, value in extracted_row.items():
        if key in spec.drop:
            continue
        normalized[spec.normalize_key(key)] = value

    row: Dict[str, Any] = {}
    for column in spec.text:
        row[column] = to_text(normalized.get(column))
    for column in spec.date:
        row[column] = to_date(normalized.get(column))
    for column in spec.numeric:
        row[column] = to_number(normalized.get(column))

    row['processed_at'] = datetime.now(timezone.utc).isoformat()
    row['raw_data'] = extracted_row

    # Hash the typed content only -- processed_at changes every run and must not
    # participate, or every run would insert fresh duplicates. raw_data is
    # excluded too: a cosmetic change there should not fork the row identity.
    hashable = {k: v for k, v in row.items() if k not in {'processed_at', 'raw_data'}}
    row['row_hash'] = row_hash(hashable, occurrence)
    return row


def build_rows(extracted_rows: Sequence[Dict[str, Any]],
               spec: Optional[SourceSpec] = None) -> List[Dict[str, Any]]:
    """Convert extracted rows, numbering duplicates so their hashes stay distinct."""
    spec = spec or get_spec()
    seen_counts: Dict[str, int] = {}
    rows: List[Dict[str, Any]] = []
    for extracted_row in extracted_rows:
        probe = to_supabase_row(extracted_row, 0, spec)
        key = probe['row_hash']
        occurrence = seen_counts.get(key, 0)
        seen_counts[key] = occurrence + 1
        rows.append(probe if occurrence == 0
                    else to_supabase_row(extracted_row, occurrence, spec))
    return rows


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


# --------------------------------------------------------------------------- #
# Supabase client
# --------------------------------------------------------------------------- #

class SupabaseSink:
    """Everything that touches Supabase. No Google APIs in here."""

    def __init__(self, url: Optional[str] = None, key: Optional[str] = None,
                 grn_table: Optional[str] = None, log_table: Optional[str] = None,
                 spec: Optional[SourceSpec] = None):
        self.spec = spec or get_spec()
        self.url = normalize_supabase_url(url or os.environ.get('SUPABASE_URL', ''))
        self.key = (key
                    or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
                    or os.environ.get('SUPABASE_KEY', '').strip())
        self.grn_table = (grn_table or os.environ.get('SUPABASE_GRN_TABLE')
                          or self.spec.table)
        self.log_table = log_table or os.environ.get('SUPABASE_LOG_TABLE', DEFAULT_LOG_TABLE)
        self._client: Optional[Client] = None

    # -- setup ------------------------------------------------------------- #

    def missing_config(self) -> List[str]:
        missing = []
        if not SUPABASE_AVAILABLE:
            missing.append('supabase package (pip install supabase)')
        if not self.url:
            missing.append('SUPABASE_URL')
        if not self.key:
            missing.append('SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)')
        return missing

    @property
    def client(self) -> Client:
        if self._client is None:
            missing = self.missing_config()
            if missing:
                raise RuntimeError('Supabase not configured: ' + ', '.join(missing))
            self._client = create_client(self.url, self.key)
        return self._client

    def check(self) -> bool:
        """Verify credentials and that both tables are reachable."""
        ok = True
        for table in (self.grn_table, self.log_table):
            try:
                self.client.table(table).select('*', count='exact').limit(1).execute()
                logger.info("[CHECK] Table '%s' reachable", table)
            except Exception as exc:
                logger.error("[CHECK] Table '%s' NOT reachable: %s", table, exc)
                logger.error("[CHECK] Run --print-schema and apply the SQL, "
                             "and confirm you are using the service role key.")
                ok = False
        return ok

    def count_rows(self, table: str) -> Optional[int]:
        try:
            result = self.client.table(table).select('id', count='exact').limit(1).execute()
            return result.count
        except Exception as exc:
            logger.error('[COUNT] Failed to count %s: %s', table, exc)
            return None

    # -- reads -------------------------------------------------------------- #

    def get_existing_source_files(self, candidates: Sequence[str]) -> set:
        """Which of `candidates` already have rows? Compared case-insensitively.

        Queries only the candidate names rather than scanning the table, so this
        stays cheap as the table grows.
        """
        if not candidates:
            return set()

        wanted = sorted({name.lower().strip() for name in candidates if name})
        existing = set()

        # Match on the generated lowercase column: in_() is case-sensitive, so
        # filtering on source_file itself would miss 'FILE.PDF' vs 'file.pdf'.
        column = 'source_file_lower'
        try:
            for batch in chunked(wanted, 100):
                result = (self.client.table(self.grn_table)
                          .select(column)
                          .in_(column, list(batch))
                          .execute())
                for record in result.data or []:
                    value = (record.get(column) or '').strip()
                    if value:
                        existing.add(value)
        except Exception as exc:
            if 'source_file_lower' in str(exc):
                logger.warning('[SUPABASE] Column source_file_lower is missing -- falling '
                               'back to case-sensitive matching. Re-run the SQL from '
                               '--print-schema to add it.')
                return self._existing_source_files_exact(candidates)
            logger.error('[SUPABASE] Failed to read existing source files: %s', exc)
            logger.error('[SUPABASE] Treating all files as new -- duplicates are still '
                         'blocked by the row_hash unique constraint.')
            return set()

        logger.info('[SUPABASE] %d/%d candidate files already present',
                    len(existing), len(wanted))
        return existing

    def get_existing_dedupe_keys(self, keys: Sequence[tuple],
                                 raw_first_values: Sequence[Any] = ()) -> set:
        """Which natural keys are already in the table?

        Queries the first key column for the candidate values only, then filters
        the full tuple client-side. That keeps one round trip per batch without
        needing a composite index or a stored procedure.

        PostgREST's in_() filter is case-sensitive and compares the stored text
        verbatim, but dedupe_key() lowercases and drops the trailing '.0' Excel
        leaves on numeric codes. Looking up only the normalized value therefore
        misses 'IRA43141122' and '155159.0'. Both spellings go into the filter and
        dedupe_key() still decides the match client-side.
        """
        spec = self.spec
        if not spec.dedupe or not keys:
            return set()

        first_column = spec.dedupe[0]
        wanted = {k for k in keys if k}
        if not wanted:
            return set()
        candidates = {k[0] for k in wanted}
        for value in raw_first_values:
            if value not in (None, ''):
                candidates.add(str(value).strip())
        first_values = sorted(candidates)

        found = set()
        try:
            for batch in chunked(first_values, 100):
                result = (self.client.table(self.grn_table)
                          .select(','.join(spec.dedupe))
                          .in_(first_column, list(batch))
                          .execute())
                for record in result.data or []:
                    candidate = dedupe_key(record, spec)
                    if candidate in wanted:
                        found.add(candidate)
        except Exception as exc:
            logger.error('[SUPABASE] Dedupe-key lookup failed: %s', exc)
            logger.error('[SUPABASE] Continuing without it -- exact repeats are still '
                         'blocked by the row_hash unique constraint.')
            return set()

        if found:
            logger.info('[SUPABASE] %d/%d natural keys already present',
                        len(found), len(wanted))
        return found

    def _existing_source_files_exact(self, candidates: Sequence[str]) -> set:
        """Pre-migration fallback: exact-case match on source_file."""
        existing = set()
        try:
            for batch in chunked(list(candidates), 100):
                result = (self.client.table(self.grn_table)
                          .select('source_file')
                          .in_('source_file', list(batch))
                          .execute())
                for record in result.data or []:
                    value = (record.get('source_file') or '').lower().strip()
                    if value:
                        existing.add(value)
        except Exception as exc:
            logger.error('[SUPABASE] Fallback lookup also failed: %s', exc)
            return set()
        return existing

    # -- writes ------------------------------------------------------------- #

    def insert_rows(self, rows: List[Dict[str, Any]], batch_size: int = 200) -> int:
        """Upsert rows on row_hash. Returns the number of rows written."""
        if not rows:
            return 0

        written = 0
        for batch in chunked(rows, batch_size):
            for attempt in range(1, 4):
                try:
                    result = (self.client.table(self.grn_table)
                              .upsert(list(batch), on_conflict='row_hash')
                              .execute())
                    written += len(result.data or batch)
                    break
                except Exception as exc:
                    if attempt == 3:
                        logger.error('[SUPABASE] Batch of %d rows failed after 3 attempts: %s',
                                     len(batch), exc)
                    else:
                        logger.warning('[SUPABASE] Batch insert attempt %d failed: %s', attempt, exc)
                        time.sleep(2 * attempt)
        logger.info('[SUPABASE] Wrote %d/%d rows to %s', written, len(rows), self.grn_table)
        return written

    def log_workflow_run(self, workflow: str, started_at: datetime, ended_at: datetime,
                         stats: Dict[str, Any]) -> bool:
        duration = (ended_at - started_at).total_seconds()
        record = {
            # All schedulers share one workflow_logs table, so stamp the source.
            'source': _source_name(self.spec),
            'workflow': workflow,
            'started_at': started_at.isoformat(),
            'ended_at': ended_at.isoformat(),
            'duration_seconds': round(duration, 2),
            'duration_text': format_duration(duration),
            'processed': int(stats.get('processed_pdfs', stats.get('processed', 0)) or 0),
            'total_items': int(stats.get('rows_added', stats.get('total_attachments', 0)) or 0),
            'failed': int(stats.get('failed_pdfs', stats.get('failed', 0)) or 0),
            'skipped': int(stats.get('skipped_pdfs', stats.get('skipped', 0)) or 0),
            'status': stats.get('status') or ('Success' if stats.get('success') else 'Failed'),
            'details': stats,
        }
        try:
            self.client.table(self.log_table).insert(record).execute()
            logger.info('[SUPABASE] Logged run of %s', workflow)
            return True
        except Exception as exc:
            # workflow_logs gained a `source` column when the table became shared
            # across schedulers. Log without it rather than losing the record.
            if 'source' in str(exc):
                logger.warning('[SUPABASE] workflow_logs has no `source` column -- logging '
                               'without it. Re-run the SQL from --print-schema to add it.')
                try:
                    self.client.table(self.log_table).insert(
                        {k: v for k, v in record.items() if k != 'source'}).execute()
                    return True
                except Exception as retry_exc:
                    exc = retry_exc
            logger.error('[SUPABASE] Failed to log workflow run: %s', exc)
            return False

    def delete_by_source_file(self, source_file: str) -> None:
        self.client.table(self.grn_table).delete().eq('source_file', source_file).execute()


def format_duration(seconds: float) -> str:
    if seconds >= 60:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    return f"{seconds:.2f}s"


# --------------------------------------------------------------------------- #
# pipeline: Drive PDFs -> LlamaExtract -> Supabase
# --------------------------------------------------------------------------- #

def _import_app_under_alias(directory: str, app_file: Optional[str] = None):
    """Import one app*.py from `directory` as the module named 'app'.

    Most repos call the scheduler app.py; reliance does too, but nbgrn ships
    app_nb.py and nbprn ships app_nbprn.py. Renaming their scripts would break
    however the owner runs them, so the odd one out is loaded under the name
    'app' instead -- which also keeps every `from app import ...` below working.

    flipkart_grn is the one repo holding several schedulers side by side
    (app_cp_grn.py, app_fp_grn.py, app_fat_grn.py, app_slv_grn.py), so the
    single-candidate rule cannot pick for it. `app_file` -- from --app or
    GRN_APP -- names the one to load. It stays optional so every other repo
    behaves exactly as before.
    """
    import glob
    import importlib.util

    myself = os.path.basename(os.path.abspath(__file__))
    if app_file:
        path = app_file if os.path.isabs(app_file) else os.path.join(directory, app_file)
        if not os.path.isfile(path):
            available = ', '.join(sorted(
                os.path.basename(p) for p in glob.glob(os.path.join(directory, 'app*.py'))
                if os.path.basename(p) != myself)) or 'none'
            raise ModuleNotFoundError(
                f"--app/GRN_APP names {app_file!r}, which is not a file next to "
                f"supabase_sink.py (available: {available}).", name='app')
        candidates = [path]
    else:
        candidates = sorted(p for p in glob.glob(os.path.join(directory, 'app*.py'))
                            if os.path.basename(p) != myself)
    if len(candidates) != 1:
        names = ', '.join(os.path.basename(p) for p in candidates) or 'none'
        raise ModuleNotFoundError(
            'No app.py next to supabase_sink.py, and the app*.py files here do '
            f'not name one unambiguously (found: {names}). Pass --app NAME.py '
            '(or set GRN_APP) to choose.', name='app')

    path = candidates[0]
    spec = importlib.util.spec_from_file_location('app', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules['app'] = module          # so `from app import CONFIG` resolves
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop('app', None)
        raise
    logger.info('[PIPELINE] Imported %s as app', os.path.basename(path))
    return module


def import_app():
    """Import the repo's app.py, with a readable error on the pydantic/3.13+ trap.

    Imported lazily so --check / --self-test / --from-json / --print-schema keep
    working without Google credentials or a compatible Python.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    app_file = os.environ.get('GRN_APP', '').strip()
    try:
        if 'app' in sys.modules:
            return sys.modules['app']
        # An explicit --app/GRN_APP always wins: a repo holding several
        # schedulers may also have a plain app.py, and `import app` would
        # silently load that one instead of the requested file.
        if app_file:
            return _import_app_under_alias(here, app_file)
        try:
            import app as app_module
            return app_module
        except ModuleNotFoundError as exc:
            if exc.name != 'app':
                raise
            return _import_app_under_alias(here)
    except Exception as exc:
        raise RuntimeError(
            f"Could not import app.py ({type(exc).__name__}: {exc}).\n"
            f"You are on Python {sys.version_info.major}.{sys.version_info.minor}. "
            'llama-cloud-services pulls in pydantic v1 compatibility shims that fail on '
            'Python 3.13+. Run this command from a 3.12-or-older venv (GitHub Actions '
            'uses 3.11):\n'
            '    py -3.12 -m venv .venv\n'
            '    .venv\\Scripts\\pip install -r requirements.txt\n'
            '    .venv\\Scripts\\python supabase_sink.py --run --dry-run --limit 1\n'
            'The --check / --self-test / --from-json commands do not need this import.'
        ) from exc


def ensure_google_access(automation: Any, need_gmail: bool = False) -> str:
    """Give `automation` a Drive client, without demanding scopes we do not use.

    Writing GRN rows to Supabase only needs Drive (list + download). The repos'
    own authenticate() asks for drive + spreadsheets + gmail.readonly + gmail.send
    and refreshing a token that was granted fewer scopes fails with
    'invalid_scope', which would block the Supabase path for a reason that has
    nothing to do with it. So: build credentials from token.json using the scopes
    the token actually carries, and fall back to the repo's own authenticate()
    when that is not possible or when Gmail is genuinely needed (--with-mail,
    --email).

    Returns a short description of which path was taken.
    """
    if need_gmail:
        if not automation.authenticate():
            raise RuntimeError('Google authentication failed')
        return 'full authenticate() (Gmail required)'

    token_path = getattr(automation, 'token_file', None) or 'token.json'
    try:
        from app import CONFIG
        token_path = CONFIG.get('token_path', token_path)
    except Exception:
        pass

    if os.path.exists(token_path):
        try:
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build

            raw = json.load(open(token_path, encoding='utf-8'))
            scopes = raw.get('scopes') or []
            if any('drive' in s for s in scopes):
                creds = Credentials.from_authorized_user_info(raw, scopes)
                if not creds.valid:
                    creds.refresh(Request())          # refresh with granted scopes only
                automation.drive_service = build('drive', 'v3', credentials=creds)
                if any('spreadsheets' in s for s in scopes):
                    automation.sheets_service = build('sheets', 'v4', credentials=creds)
                granted = ','.join(s.rsplit('/', 1)[-1] for s in scopes)
                logger.info('[AUTH] Drive-only access using granted scopes: %s', granted)
                return f'drive-only ({granted})'
        except Exception as exc:
            logger.warning('[AUTH] Drive-only auth failed (%s); falling back to '
                           "the repo's authenticate()", exc)

    if not automation.authenticate():
        raise RuntimeError('Google authentication failed')
    return 'full authenticate()'


#: What a PDF scheduler's automation class must expose for run_pipeline. Each
#: tuple is one requirement, satisfied by any name in it -- the repos disagree on
#: what the row mapper and the Drive listing are called.
#: safe_extract is deliberately absent: reliance calls agent.extract() inline
#: rather than through a retry wrapper, and extractor() covers that.
REQUIRED_METHODS = (
    ('authenticate',),
    ('download_from_drive',),
    ('process_extracted_data', 'flatten_json'),
    ('list_drive_files', 'list_drive_pdfs'),
)
#: ...and for an Excel scheduler.
REQUIRED_EXCEL_METHODS = ('authenticate',)


def method(obj: Any, name: str):
    """Get a method whether the repo made it public or private.

    instamart/doc/aws/hyperpure expose download_from_drive; reliance, nbgrn and
    nbprn keep the same method as _download_from_drive. Returns None if neither
    spelling exists.
    """
    for candidate in (name, f'_{name}'):
        found = getattr(obj, candidate, None)
        if callable(found):
            return found
    return None


def find_automation_class(module: Any) -> Any:
    """Locate the automation class in a scheduler's app.py.

    Each repo names it differently (InstamartAutomation, DocAutomation,
    AWSMoreRetailAutomation, HyperpureAutomation), so it is found by the methods
    it exposes rather than by name. That keeps this file identical across repos.
    """
    import inspect

    def is_pdf_automation(obj) -> bool:
        return all(any(method(obj, name) for name in group)
                   for group in REQUIRED_METHODS)

    def is_excel_automation(obj) -> bool:
        return (all(callable(getattr(obj, m, None)) for m in REQUIRED_EXCEL_METHODS)
                and any(callable(getattr(obj, m, None))
                        for m in ('_get_excel_files_filtered', '_get_excel_files_with_grn'))
                and any(callable(getattr(obj, m, None))
                        for m in ('_read_excel_file_robust', '_read_excel_file')))

    candidates = [
        obj for obj in vars(module).values()
        if inspect.isclass(obj)
        and obj.__module__ == module.__name__
        and (is_pdf_automation(obj) or is_excel_automation(obj))
    ]
    if not candidates:
        wanted = ', '.join('/'.join(group) for group in REQUIRED_METHODS)
        raise RuntimeError(
            'No automation class found in app.py. Expected a class exposing either '
            f'{wanted} (a leading underscore on any of them is fine), '
            'or the Excel equivalents (_get_excel_files_*, _read_excel_file*).')
    if len(candidates) > 1:
        names = ', '.join(c.__name__ for c in candidates)
        raise RuntimeError(f'Ambiguous automation classes in app.py: {names}')
    logger.info('[PIPELINE] Using %s from app.py', candidates[0].__name__)
    return candidates[0]


def find_method(obj: Any, *names: str):
    """Return the first method that exists, so naming drift between repos is fine."""
    for name in names:
        found = method(obj, name)
        if found is not None:
            return found
    raise RuntimeError(f"app.py exposes none of: {', '.join(names)}")


def config_section(config: Dict[str, Any], *names: str) -> Dict[str, Any]:
    """Return a copy of the first CONFIG section that exists.

    instamart/doc/aws/hyperpure call them CONFIG['sheet'] and CONFIG['mail'];
    reliance, nbgrn and nbprn call the same two sections CONFIG['pdf'] and
    CONFIG['gmail'].
    """
    for name in names:
        section = config.get(name)
        if isinstance(section, dict):
            return dict(section)
    raise RuntimeError(f"app.py CONFIG has no section named: {', '.join(names)}")


def apply_module_defaults(section: Dict[str, Any], app_module: Any,
                          **key_to_constant: str) -> Dict[str, Any]:
    """Fill section keys the repo keeps as module constants instead of in CONFIG.

    nbgrn/nbprn hardcode DEFAULT_DAYS_BACK / DEFAULT_MAX_FILES at module level and
    merge them into the config dict inside run_combined_workflow, so reading
    CONFIG alone would silently use different limits than a normal run.
    """
    for key, constant in key_to_constant.items():
        if key not in section and hasattr(app_module, constant):
            section[key] = getattr(app_module, constant)
    return section


def extractor(automation: Any):
    """Return `(agent, file_path) -> extraction`, however app.py drives LlamaExtract.

    Most repos wrap it in safe_extract(agent, path), which retries on its own.
    reliance calls agent.extract(path) inline instead, so fall back to that --
    through the class's own retry_wrapper when it has one, to keep the retry
    behaviour the same as a normal run.
    """
    found = method(automation, 'safe_extract')
    if found is not None:
        return found

    retry = method(automation, 'retry_wrapper')
    if retry is not None:
        return lambda agent, file_path: retry(agent.extract, file_path)
    return lambda agent, file_path: agent.extract(file_path)


def row_mapper(automation: Any):
    """Return `(extracted_data, file_info) -> rows`, whatever app.py calls it.

    Most repos expose process_extracted_data(extracted_data, file_info), which
    stamps source_file/drive_file_id itself. nbgrn/nbprn expose _flatten_json(),
    which takes the extraction alone because their PDF workflow stamps those two
    keys onto every row afterwards -- so do the same here.
    """
    mapper = method(automation, 'process_extracted_data')
    if mapper is not None:
        return mapper

    flatten = find_method(automation, 'flatten_json')

    def map_and_stamp(extracted_data: Dict[str, Any],
                      file_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = flatten(extracted_data) or []
        for row in rows:
            row.setdefault('source_file', file_info['name'])
            row.setdefault('drive_file_id', file_info['id'])
        return rows

    return map_and_stamp


def read_excel_config(automation: Any) -> Dict[str, Any]:
    """Get the Excel settings, however this repo happens to store them.

    Most schedulers use `self.excel_config`; bb_alert nests everything under
    `self.config['excel']` so it can be overridden by env vars.
    """
    config = getattr(automation, 'excel_config', None)
    if isinstance(config, dict):
        return config
    nested = getattr(automation, 'config', None)
    if isinstance(nested, dict) and isinstance(nested.get('excel'), dict):
        return nested['excel']
    raise RuntimeError('Could not find excel_config or config["excel"] on the '
                       'automation class in app.py')


def dataframe_to_dicts(df: Any, file_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a cleaned DataFrame into plain dicts with normalized column names.

    NaN/NaT become None and numpy scalars become Python types, so the rows are
    JSON-serializable for the Supabase client.
    """
    import pandas as pd

    if df is None or df.empty:
        return []

    columns = normalize_columns(list(df.columns))
    rows: List[Dict[str, Any]] = []
    for record in df.to_dict(orient='records'):
        row: Dict[str, Any] = {}
        for column, (_, value) in zip(columns, record.items()):
            if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
                row[column] = None
            elif hasattr(value, 'item'):        # numpy scalar -> Python scalar
                row[column] = value.item()
            elif isinstance(value, (pd.Timestamp, datetime, date)):
                row[column] = str(value)
            else:
                row[column] = value
        # The scripts add source_file_name; standardise on source_file so every
        # table in the project shares one dedupe column.
        row['source_file'] = file_info['name']
        row['drive_file_id'] = file_info.get('id')
        rows.append(row)
    return rows


def dedupe_key(row: Dict[str, Any], spec: SourceSpec) -> Optional[tuple]:
    """The natural key the original sheet deduped on, or None if incomplete.

    Values are compared as trimmed strings with a trailing '.0' removed, matching
    the sheet-side cleanup where Excel turned codes into floats.
    """
    if not spec.dedupe:
        return None
    parts = []
    for column in spec.dedupe:
        value = row.get(column)
        if value in (None, ''):
            return None
        text = str(value).strip()
        text = re.sub(r'\.0$', '', text)
        parts.append(text.lower())
    return tuple(parts)


def infer_sql_type(values: Sequence[Any], column: str, spec: SourceSpec) -> str:
    """Guess a column type from sample values, biased towards not losing data."""
    samples = [v for v in values if v not in (None, '')]
    if not samples:
        return 'text'
    # Never type a dedupe key as numeric: these are codes, and '007' must not
    # become 7. The original scripts force them to strings for the same reason.
    if column in spec.dedupe:
        return 'text'
    # Identifiers that happen to be all digits (UPC/EAN barcodes, HSN, GSTIN,
    # invoice/GRN numbers, supplier codes). Storing them as numbers invites lost
    # leading zeros and float rounding on long values, and no one sums a barcode.
    # Underscores are stripped first so 'invoiceno' and 'invoice_no' behave alike.
    squashed = column.replace('_', '')
    # Matched on the suffix only: 'invoiceno' is an identifier, but 'invoicedate'
    # is a real date and must not be caught by a blanket 'invoice' match.
    if (squashed.endswith(('no', 'num', 'code', 'id', 'upc', 'ean', 'gtin', 'hsn',
                           'sac', 'gstin', 'pan', 'pincode', 'zip', 'barcode', 'asin'))
            or 'barcode' in squashed):
        return 'text'
    # Descriptive fields: a column of all-zero reason codes should not become
    # numeric and then swallow the first real free-text value that shows up.
    if re.search(r'(reason|description|desc|name|address|status|remark|comment|'
                 r'notes?)', column):
        return 'text'
    texts = [str(v).strip() for v in samples]
    if any(re.fullmatch(r'0\d+', t) for t in texts):
        return 'text'                       # leading zeros => identifier, not number

    # Dates first: to_number() pulls the leading number out of anything, so
    # '2026-08-12' would otherwise look numeric and be stored as the year 2026.
    if all(to_date(t) is not None for t in texts):
        return 'date'
    # Strict full-match, not to_number(), for the same reason: only type a column
    # numeric when every value is *entirely* a number.
    if all(re.fullmatch(r'-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d*\.?\d+', t)
           for t in texts):
        return 'numeric'
    return 'text'


def normalize_extraction(extraction_result: Any) -> List[Any]:
    """Flatten LlamaExtract's several return shapes into a list of dicts.

    Mirrors the handling in app.py.process_drive_to_sheet_workflow.
    """
    results = extraction_result if isinstance(extraction_result, list) else [extraction_result]
    return [item.data if hasattr(item, 'data') else item for item in results]


def run_pipeline(sink: SupabaseSink, days_back: Optional[int] = None,
                 limit: Optional[int] = None, skip_existing: bool = True,
                 dry_run: bool = False, dump_json: Optional[str] = None,
                 with_mail: bool = False, send_email: bool = False) -> Dict[str, Any]:
    """Reuse app.py's Drive + LlamaExtract logic, write the rows to Supabase."""
    # Imported lazily so --check / --self-test / --from-json / --print-schema keep
    # working without Google credentials, and on Python versions where the
    # llama-cloud-services import blows up (see the hint below).
    app_module = import_app()
    from app import CONFIG
    # Deliberately not `from app import LLAMA_AVAILABLE`. Each app.py sets that
    # flag from whichever SDK *it* uses, and flipkart_grn's app_cp_grn.py /
    # app_fp_grn.py drive extraction through the newer `llama_cloud.LlamaCloud`
    # instead. In the environment this sink runs in (llama-cloud-services, which
    # pins llama-cloud<0.2) their import fails and the flag is False -- even
    # though everything this sink needs is present. The import below is the
    # honest check: it raises ImportError with a clear message if the package
    # really is missing.
    from llama_cloud_services import LlamaExtract

    sheet_config = config_section(CONFIG, 'sheet', 'pdf')
    apply_module_defaults(sheet_config, app_module,
                          days_back='DEFAULT_DAYS_BACK',
                          max_files='DEFAULT_MAX_FILES')
    if days_back is not None:
        sheet_config['days_back'] = days_back

    stats = {
        'files_found': 0, 'skipped_pdfs': 0, 'processed_pdfs': 0,
        'failed_pdfs': 0, 'rows_added': 0,
    }
    started_at = datetime.now(timezone.utc)

    automation = find_automation_class(app_module)()
    ensure_google_access(automation, need_gmail=with_mail or send_email)

    mail_stats: Dict[str, Any] = {}
    if with_mail:
        logger.info('[PIPELINE] Running Mail -> Drive first')
        mail_started = datetime.now(timezone.utc)
        mail_config = config_section(CONFIG, 'mail', 'gmail')
        apply_module_defaults(mail_config, app_module,
                              days_back='DEFAULT_DAYS_BACK',
                              max_results='DEFAULT_MAX_RESULTS')
        run_mail = find_method(automation, 'process_mail_to_drive_workflow',
                               'process_gmail_workflow')
        mail_stats = run_mail(mail_config) or {}
        if not dry_run:
            sink.log_workflow_run('Mail to Drive', mail_started,
                                  datetime.now(timezone.utc), mail_stats)

    # LlamaExtract: prefer the env var, fall back to the key hardcoded in app.py.
    api_key = os.environ.get('LLAMA_CLOUD_API_KEY') or sheet_config['llama_api_key']
    os.environ['LLAMA_CLOUD_API_KEY'] = api_key
    agent = LlamaExtract().get_agent(name=sheet_config['llama_agent'])
    if agent is None:
        raise RuntimeError(f"LlamaExtract agent '{sheet_config['llama_agent']}' not found")
    logger.info('[PIPELINE] LlamaExtract agent ready')

    list_pdfs = find_method(automation, 'list_drive_files', 'list_drive_pdfs')
    download = find_method(automation, 'download_from_drive')
    extract = extractor(automation)
    map_rows = row_mapper(automation)
    pdf_files = list_pdfs(sheet_config['drive_folder_id'],
                          sheet_config.get('days_back', 3))
    stats['files_found'] = len(pdf_files)

    if skip_existing and pdf_files:
        existing = sink.get_existing_source_files([f['name'] for f in pdf_files])
        new_files = [f for f in pdf_files if f['name'].lower().strip() not in existing]
        stats['skipped_pdfs'] = len(pdf_files) - len(new_files)
        pdf_files = new_files

    max_files = limit if limit is not None else sheet_config.get('max_files')
    if max_files is not None:
        pdf_files = pdf_files[:max_files]

    logger.info('[PIPELINE] %d PDF(s) to process (%d skipped as already loaded)',
                len(pdf_files), stats['skipped_pdfs'])

    all_rows: List[Dict[str, Any]] = []

    for pdf_file in pdf_files:
        tmp_path = None
        try:
            logger.info('[PIPELINE] Processing %s', pdf_file['name'])
            file_data = download(pdf_file['id'], pdf_file['name'])
            if not file_data:
                stats['failed_pdfs'] += 1
                continue

            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                tmp_file.write(file_data)
                tmp_path = tmp_file.name

            extracted_rows: List[Dict[str, Any]] = []
            for chunk in normalize_extraction(extract(agent, tmp_path)):
                if isinstance(chunk, dict):
                    extracted_rows.extend(map_rows(chunk, pdf_file))
                else:
                    logger.warning('[PIPELINE] Skipping non-dict extraction chunk: %s', type(chunk))

            if not extracted_rows:
                logger.warning('[PIPELINE] No line items found in %s', pdf_file['name'])
                stats['failed_pdfs'] += 1
                continue

            rows = build_rows(extracted_rows, sink.spec)
            all_rows.extend(rows)

            if dry_run:
                stats['processed_pdfs'] += 1
                stats['rows_added'] += len(rows)
                logger.info('[DRY RUN] %s -> %d row(s)', pdf_file['name'], len(rows))
                continue

            written = sink.insert_rows(rows)
            if written:
                stats['processed_pdfs'] += 1
                stats['rows_added'] += written
            else:
                stats['failed_pdfs'] += 1

        except Exception as exc:
            logger.error('[PIPELINE] Failed on %s: %s', pdf_file.get('name', 'unknown'), exc)
            stats['failed_pdfs'] += 1
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    if dump_json and all_rows:
        with open(dump_json, 'w', encoding='utf-8') as handle:
            json.dump(all_rows, handle, indent=2, default=str)
        logger.info('[PIPELINE] Wrote %d row(s) to %s', len(all_rows), dump_json)

    ended_at = datetime.now(timezone.utc)
    stats['success'] = stats['failed_pdfs'] == 0
    stats['status'] = ('Success' if stats['failed_pdfs'] == 0
                       else 'Partial' if stats['processed_pdfs'] else 'Failed')
    stats['dry_run'] = dry_run

    if not dry_run:
        sink.log_workflow_run('Drive to Supabase', started_at, ended_at, stats)

    if send_email and not dry_run:
        notify = find_method(automation, 'send_email_notification')
        notify({
            'mail_emails_checked': mail_stats.get('emails_checked', 0),
            'mail_attachments_found': mail_stats.get('attachments_found', 0),
            'mail_attachments_skipped': mail_stats.get('attachments_skipped', 0),
            'mail_attachments_uploaded': mail_stats.get('attachments_uploaded', 0),
            'mail_upload_failed': mail_stats.get('upload_failed', 0),
            'drive_files_found': stats['files_found'],
            'drive_files_skipped': stats['skipped_pdfs'],
            'drive_files_processed': stats['processed_pdfs'],
            'drive_files_failed': stats['failed_pdfs'],
            'drive_rows_added': stats['rows_added'],
            'total_duration': format_duration((ended_at - started_at).total_seconds()),
            'overall_success': stats['success'],
            'any_success': stats['processed_pdfs'] > 0,
        })

    logger.info('[PIPELINE] Done in %s | found=%d skipped=%d processed=%d failed=%d rows=%d',
                format_duration((ended_at - started_at).total_seconds()),
                stats['files_found'], stats['skipped_pdfs'],
                stats['processed_pdfs'], stats['failed_pdfs'], stats['rows_added'])
    return stats


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #

def _excel_setup(sink: SupabaseSink, days_back: Optional[int], need_gmail: bool = False):
    """Shared bootstrap for the Excel pipeline and column discovery."""
    app_module = import_app()
    automation = find_automation_class(app_module)()
    ensure_google_access(automation, need_gmail=need_gmail)

    config = read_excel_config(automation)
    list_files = find_method(automation, '_get_excel_files_filtered',
                             '_get_excel_files_with_grn')
    read_excel = find_method(automation, '_read_excel_file_robust', '_read_excel_file')
    clean = getattr(automation, '_clean_dataframe', None)

    lookback = days_back if days_back is not None else config.get('days_back', 3)
    max_files = config.get('max_files', config.get('max_results', 1000))
    files = list_files(config['excel_folder_id'], lookback, max_files)
    return automation, config, read_excel, clean, files


def _load_excel_rows(read_excel, clean, config, excel_file) -> List[Dict[str, Any]]:
    df = read_excel(excel_file['id'], excel_file['name'], config.get('header_row', 0))
    if df is None or df.empty:
        return []
    if callable(clean):
        df = clean(df)
    return dataframe_to_dicts(df, excel_file)


def run_excel_pipeline(sink: SupabaseSink, days_back: Optional[int] = None,
                       limit: Optional[int] = None, skip_existing: bool = True,
                       dry_run: bool = False, dump_json: Optional[str] = None,
                       with_mail: bool = False) -> Dict[str, Any]:
    """Drive spreadsheets -> pandas -> Supabase, reusing the repo's own readers."""
    spec = sink.spec
    stats = {'files_found': 0, 'skipped_pdfs': 0, 'processed_pdfs': 0,
             'failed_pdfs': 0, 'rows_added': 0, 'duplicates_skipped': 0}
    started_at = datetime.now(timezone.utc)

    automation, config, read_excel, clean, excel_files = _excel_setup(
        sink, days_back, need_gmail=with_mail)

    mail_stats: Dict[str, Any] = {}
    if with_mail:
        gmail_workflow = getattr(automation, 'process_gmail_workflow', None)
        if callable(gmail_workflow):
            logger.info('[PIPELINE] Running Mail -> Drive first')
            mail_started = datetime.now(timezone.utc)
            result = gmail_workflow()
            mail_stats = result if isinstance(result, dict) else {'success': bool(result)}
            if not dry_run:
                sink.log_workflow_run('Mail to Drive', mail_started,
                                      datetime.now(timezone.utc), mail_stats)
        else:
            logger.warning('[PIPELINE] --with-mail ignored: no process_gmail_workflow')

    stats['files_found'] = len(excel_files)

    if skip_existing and excel_files:
        existing = sink.get_existing_source_files([f['name'] for f in excel_files])
        new_files = [f for f in excel_files if f['name'].lower().strip() not in existing]
        stats['skipped_pdfs'] = len(excel_files) - len(new_files)
        excel_files = new_files

    if limit is not None:
        excel_files = excel_files[:limit]

    logger.info('[PIPELINE] %d spreadsheet(s) to process (%d already loaded)',
                len(excel_files), stats['skipped_pdfs'])

    all_rows: List[Dict[str, Any]] = []
    # Reproduces the scripts' whole-sheet "drop_duplicates(keep='first')": the
    # first row for a natural key wins, both within this run and against rows
    # already in the table.
    seen_keys: set = set()

    for excel_file in excel_files:
        try:
            logger.info('[PIPELINE] Processing %s', excel_file['name'])
            extracted_rows = _load_excel_rows(read_excel, clean, config, excel_file)
            if not extracted_rows:
                logger.warning('[PIPELINE] No rows read from %s', excel_file['name'])
                stats['failed_pdfs'] += 1
                continue

            if spec.dedupe:
                fresh = []
                keys = [dedupe_key(r, spec) for r in extracted_rows]
                known = sink.get_existing_dedupe_keys(
                    [k for k in keys if k],
                    [r.get(spec.dedupe[0]) for r in extracted_rows])
                for row, key in zip(extracted_rows, keys):
                    if key and (key in seen_keys or key in known):
                        stats['duplicates_skipped'] += 1
                        continue
                    if key:
                        seen_keys.add(key)
                    fresh.append(row)
                extracted_rows = fresh

            if not extracted_rows:
                logger.info('[PIPELINE] %s: every row already present, nothing to add',
                            excel_file['name'])
                stats['processed_pdfs'] += 1
                continue

            rows = build_rows(extracted_rows, spec)
            all_rows.extend(rows)

            if dry_run:
                stats['processed_pdfs'] += 1
                stats['rows_added'] += len(rows)
                logger.info('[DRY RUN] %s -> %d row(s)', excel_file['name'], len(rows))
                continue

            written = sink.insert_rows(rows)
            if written:
                stats['processed_pdfs'] += 1
                stats['rows_added'] += written
            else:
                stats['failed_pdfs'] += 1

        except Exception as exc:
            logger.error('[PIPELINE] Failed on %s: %s',
                         excel_file.get('name', 'unknown'), exc)
            stats['failed_pdfs'] += 1

    if dump_json and all_rows:
        with open(dump_json, 'w', encoding='utf-8') as handle:
            json.dump(all_rows, handle, indent=2, default=str)
        logger.info('[PIPELINE] Wrote %d row(s) to %s', len(all_rows), dump_json)

    ended_at = datetime.now(timezone.utc)
    stats['success'] = stats['failed_pdfs'] == 0
    stats['status'] = ('Success' if stats['failed_pdfs'] == 0
                       else 'Partial' if stats['processed_pdfs'] else 'Failed')
    stats['dry_run'] = dry_run
    if not dry_run:
        sink.log_workflow_run('Drive to Supabase (Excel)', started_at, ended_at, stats)

    logger.info('[PIPELINE] Done in %s | found=%d skipped=%d processed=%d failed=%d '
                'rows=%d duplicate-rows-skipped=%d',
                format_duration((ended_at - started_at).total_seconds()),
                stats['files_found'], stats['skipped_pdfs'], stats['processed_pdfs'],
                stats['failed_pdfs'], stats['rows_added'], stats['duplicates_skipped'])
    return stats


def cmd_discover_columns(sink: SupabaseSink, files: int = 3,
                         days_back: Optional[int] = None) -> int:
    """Read real spreadsheets and emit a typed schema for what is actually there.

    The vendor headers are not knowable from the code, so this is how an Excel
    source graduates from 'everything in raw_data' to real typed columns.
    """
    spec = sink.spec
    if spec.kind != 'excel':
        print(f"--discover-columns is for Excel sources; {_source_name(spec)} is "
              f"'{spec.kind}'. Use --run --dry-run --dump-json instead.")
        return 1

    _, config, read_excel, clean, excel_files = _excel_setup(sink, days_back)
    if not excel_files:
        print('No spreadsheets found in the Drive folder for that lookback window. '
              'Try a larger --days-back.')
        return 1

    samples: Dict[str, List[Any]] = {}
    order: List[str] = []
    inspected = 0
    for excel_file in excel_files[:files]:
        try:
            rows = _load_excel_rows(read_excel, clean, config, excel_file)
        except Exception as exc:
            print(f"  ! {excel_file['name']}: {exc}")
            continue
        if not rows:
            continue
        inspected += 1
        print(f"  read {excel_file['name']}: {len(rows)} rows, {len(rows[0])} columns")
        for row in rows:
            for column, value in row.items():
                if column not in samples:
                    samples[column] = []
                    order.append(column)
                if value not in (None, '') and len(samples[column]) < 50:
                    samples[column].append(value)

    if not inspected:
        print('Could not read any spreadsheet.')
        return 1

    inferred = {c: infer_sql_type(samples.get(c, []), c, spec) for c in order}
    buckets = {'text': [], 'date': [], 'numeric': []}
    for column, sql_type in inferred.items():
        buckets[sql_type].append(column)

    print(f"\nInspected {inspected} file(s); {len(order)} distinct columns.\n")
    print('Column                          type      sample')
    print('-' * 74)
    for column in order:
        sample = samples.get(column) or []
        preview = str(sample[0])[:28] if sample else '(always empty)'
        print(f"{column:<32}{inferred[column]:<10}{preview}")

    print('\n--- paste into the SOURCES entry in supabase_sink.py ---\n')
    for bucket in ('text', 'date', 'numeric'):
        names = [c for c in buckets[bucket] if c not in _COMMON_TEXT]
        rendered = ', '.join(f"'{c}'" for c in names)
        suffix = ' + _COMMON_TEXT' if bucket == 'text' else ''
        print(f"    {bucket}=[{rendered}]{suffix},")

    print('\n--- then apply this DDL ---\n')
    updated = SourceSpec(table=spec.table, text=buckets['text'], date=buckets['date'],
                         numeric=buckets['numeric'], kind='excel', dedupe=spec.dedupe,
                         note=spec.note)
    # build_schema_sql labels the DDL by looking the spec up in SOURCES; this
    # freshly built one is not registered, so register it under the real name.
    SOURCES[_source_name(spec)] = updated
    print(build_schema_sql(updated, sink.log_table))
    return 0


def cmd_check(sink: SupabaseSink) -> int:
    print('Configuration')
    print(f"  SUPABASE_URL              : {sink.url or '<not set>'}")
    print(f"  SUPABASE_SERVICE_ROLE_KEY : {mask(sink.key)}")
    print(f"  LLAMA_CLOUD_API_KEY       : {mask(os.environ.get('LLAMA_CLOUD_API_KEY'))}")
    print(f"  GRN table                 : {sink.grn_table}")
    print(f"  log table                 : {sink.log_table}")
    print(f"  supabase package          : {'installed' if SUPABASE_AVAILABLE else 'MISSING'}")
    print()

    missing = sink.missing_config()
    if missing:
        print('FAIL: missing configuration -> ' + ', '.join(missing))
        return 1

    if not sink.check():
        return 1

    for table in (sink.grn_table, sink.log_table):
        count = sink.count_rows(table)
        print(f"  {table}: {count if count is not None else '?'} row(s)")
    print('\nOK: Supabase is reachable and both tables exist.')
    return 0


def _self_test_sample(spec: "SourceSpec", marker: str) -> Dict[str, Any]:
    """Synthetic row shaped like `spec`, not like any one scheduler.

    Values exercise the coercion paths every source hits: the Indian date
    formats in DATE_FORMATS, thousands separators and parenthesised negatives.
    """
    dates = ['12-08-2025', '14/08/2025', '2025-08-16', '18-Aug-2025']
    numbers = ['1,234.50', '(45.00)', '12', '7.5']

    sample: Dict[str, Any] = {}
    for index, name in enumerate(spec.text):
        sample[name] = f"SELFTEST-{index + 1}"
    for index, name in enumerate(spec.date):
        sample[name] = dates[index % len(dates)]
    for index, name in enumerate(spec.numeric):
        sample[name] = numbers[index % len(numbers)]

    sample['source_file'] = marker
    if 'drive_file_id' in spec.text:
        sample['drive_file_id'] = 'selftest-file-id'
    return sample


def cmd_self_test(sink: SupabaseSink) -> int:
    """Insert, read back, upsert again and delete a synthetic row."""
    marker = f"__selftest__{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.pdf"
    spec = sink.spec
    sample = _self_test_sample(spec, marker)

    rows = build_rows([sample, sample], spec)  # duplicate line -> distinct hashes
    print(f"Prepared {len(rows)} row(s) with distinct hashes: "
          f"{len({r['row_hash'] for r in rows})} unique")

    print('Coerced values:')
    for key in spec.date[:3] + spec.numeric[:3]:
        print(f"  {key:24s} {sample[key]!r:14s} -> {rows[0][key]!r}")
    # No typed column can hold an unparseable date, so show that path directly.
    print(f"  {'(unparseable date)':24s} {'not a date'!r:14s} -> {to_date('not a date')!r}")

    unparsed = [k for k in spec.date + spec.numeric if rows[0][k] is None]
    if unparsed:
        print('FAIL: these typed columns did not coerce -> ' + ', '.join(unparsed))
        return 1
    if to_date('not a date') is not None:
        print('FAIL: an unparseable date should coerce to None')
        return 1

    try:
        written = sink.insert_rows(rows)
        if written != len(rows):
            print(f"FAIL: wrote {written} of {len(rows)} rows")
            return 1

        found = sink.get_existing_source_files([marker])
        if marker.lower() not in found:
            print('FAIL: inserted row not found by get_existing_source_files')
            return 1
        print('OK: rows inserted and read back')

        sink.insert_rows(rows)                  # same hashes -> must not duplicate
        result = (sink.client.table(sink.grn_table)
                  .select('id', count='exact').eq('source_file', marker).execute())
        if result.count != len(rows):
            print(f"FAIL: re-insert duplicated rows ({result.count} present, expected {len(rows)})")
            return 1
        print('OK: re-running is idempotent (upsert on row_hash)')

        now = datetime.now(timezone.utc)
        if not sink.log_workflow_run('Self Test', now, now,
                                     {'processed_pdfs': 1, 'rows_added': len(rows),
                                      'failed_pdfs': 0, 'skipped_pdfs': 0, 'success': True}):
            print('FAIL: could not write to the log table')
            return 1
        print('OK: workflow log written')
        return 0
    finally:
        try:
            sink.delete_by_source_file(marker)
            sink.client.table(sink.log_table).delete().eq('workflow', 'Self Test').execute()
            print('Cleaned up self-test rows')
        except Exception as exc:
            print(f"WARNING: cleanup failed, remove source_file={marker} manually: {exc}")


def cmd_from_json(sink: SupabaseSink, path: str, dry_run: bool) -> int:
    with open(path, 'r', encoding='utf-8') as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        print('FAIL: expected a JSON array of rows')
        return 1

    # Accept both a --dump-json file (already coerced) and raw extractor output.
    rows = (payload if payload and 'row_hash' in payload[0]
            else build_rows(payload, sink.spec))

    if dry_run:
        print(json.dumps(rows[:3], indent=2, default=str))
        print(f"... {len(rows)} row(s) total (dry run, nothing written)")
        return 0

    written = sink.insert_rows(rows)
    print(f"Wrote {written}/{len(rows)} row(s)")
    return 0 if written == len(rows) else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description='Write Instamart GRN extractions to Supabase.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    parser.add_argument('--print-schema', action='store_true',
                        help='print the SQL to create the tables, then exit')
    parser.add_argument('--check', action='store_true',
                        help='verify config, connectivity and that the tables exist')
    parser.add_argument('--self-test', action='store_true',
                        help='insert/read/delete a synthetic row end to end')
    parser.add_argument('--run', action='store_true',
                        help='run Drive -> LlamaExtract -> Supabase')
    parser.add_argument('--with-mail', action='store_true',
                        help='with --run, also run the Mail -> Drive step first')
    parser.add_argument('--from-json', metavar='PATH',
                        help='insert rows from a JSON file instead of running extraction')
    parser.add_argument('--dry-run', action='store_true',
                        help='extract but do not write to Supabase')
    parser.add_argument('--dump-json', metavar='PATH',
                        help='with --run, save the prepared rows to this file')
    parser.add_argument('--limit', type=int, help='process at most N PDFs')
    parser.add_argument('--days-back', type=int, help='override the Drive lookback window')
    parser.add_argument('--no-skip-existing', action='store_true',
                        help='reprocess files already present in Supabase')
    parser.add_argument('--email', action='store_true',
                        help='send the summary email after a real run')
    parser.add_argument('--discover-columns', action='store_true',
                        help='Excel sources: read real spreadsheets and print a typed '
                             'schema for the columns actually present')
    parser.add_argument('--discover-files', type=int, default=3, metavar='N',
                        help='how many spreadsheets --discover-columns samples (default 3)')
    parser.add_argument('--source', metavar='NAME',
                        help='which scheduler this run is for; defaults to GRN_SOURCE '
                             'in .env. See --list-sources.')
    parser.add_argument('--app', metavar='FILE.py',
                        help='which app*.py to import, for a repo holding several '
                             'schedulers (flipkart_grn). Defaults to GRN_APP in .env, '
                             'and to the single app*.py present everywhere else.')
    parser.add_argument('--list-sources', action='store_true',
                        help='list the known sources and their tables, then exit')
    parser.add_argument('-v', '--verbose', action='store_true', help='debug logging')
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')

    load_dotenv()

    # Set before any import_app() call so --app beats GRN_APP from .env.
    if args.app:
        os.environ['GRN_APP'] = args.app

    if args.list_sources:
        for name, spec in sorted(SOURCES.items()):
            marker = '*' if name == (args.source or os.environ.get('GRN_SOURCE', 'instamart')) else ' '
            print(f"{marker} {name:<12} -> table {spec.table:<16} "
                  f"({len(spec.columns)} typed columns)")
            if spec.note:
                for chunk in _wrap(spec.note, 68):
                    print(f"      {chunk}")
        return 0

    spec = get_spec(args.source)

    if args.print_schema:
        print(build_schema_sql(spec))
        return 0

    sink = SupabaseSink(spec=spec)

    if args.check:
        return cmd_check(sink)

    if args.self_test:
        missing = sink.missing_config()
        if missing:
            print('FAIL: missing configuration -> ' + ', '.join(missing))
            return 1
        return cmd_self_test(sink)

    if args.from_json:
        return cmd_from_json(sink, args.from_json, args.dry_run)

    if args.discover_columns:
        try:
            return cmd_discover_columns(sink, args.discover_files, args.days_back)
        except Exception as exc:
            logger.error('Column discovery failed: %s', exc)
            return 1

    if args.run:
        if not args.dry_run:
            missing = sink.missing_config()
            if missing:
                print('FAIL: missing configuration -> ' + ', '.join(missing))
                return 1
        try:
            runner = run_excel_pipeline if spec.kind == 'excel' else run_pipeline
            stats = runner(
                sink,
                days_back=args.days_back,
                limit=args.limit,
                skip_existing=not args.no_skip_existing,
                dry_run=args.dry_run,
                dump_json=args.dump_json,
                with_mail=args.with_mail,
                **({} if spec.kind == 'excel' else {'send_email': args.email}),
            )
        except Exception as exc:
            logger.error('Pipeline failed: %s', exc)
            return 1
        return 0 if stats['status'] != 'Failed' else 1

    parser.print_help()
    return 0


if __name__ == '__main__':
    sys.exit(main())
