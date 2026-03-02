# -*- coding: utf-8 -*-
"""
BSale → MySQL (tbl_documento) — Extracción por día (TODO, todas las sucursales)
SIN ALTER, SIN TRUNCATE, SIN COLUMNAS NUEVAS
CORREGIDO: details COMPLETOS por /documents/{id}/details.json (sin recorte 25)

PUNTOS CLAVE:
- Se extrae por día usando emissiondaterange en UTC (00:00–23:59 UTC).
- La fecha guardada ("fecha") se calcula con emissionDate interpretado en UTC (DATE_UTC).
- NO filtra por office ni por document_type: trae TODO.
- Insert IGNORE evita duplicar.
"""

import os, sys, logging, argparse
from datetime import datetime, date, timedelta, timezone
from functools import lru_cache

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.mysql import insert as mysql_insert

# =========================
# CONFIG
# =========================
BASE_URL = "https://api.bsale.com.pe/v1"
BSALE_TOKEN = os.getenv("BSALE_TOKEN")
if not BSALE_TOKEN:
    raise RuntimeError("Falta BSALE_TOKEN en variables de entorno.")

MYSQL_URL = os.getenv("MYSQL_URL")
if not MYSQL_URL:
    raise RuntimeError("MYSQL_URL no configurado en variables de entorno.")

MYSQL_TABLE = "tbl_documento"

REQUEST_TIMEOUT    = 30
MAX_PAGES_PER_CALL = 800

#FECHA_INICIO_RAPIDA = "2020-01-01"
#FECHA_FIN_RAPIDA    = date.today().strftime("%Y-%m-%d")


FECHA_INICIO_RAPIDA = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
FECHA_FIN_RAPIDA    = date.today().strftime("%Y-%m-%d")

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("bsale_etl")

# =========================
# HTTP SESSION
# =========================
session = requests.Session()
session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=6,
            backoff_factor=0.7,
            allowed_methods=frozenset(["GET"]),
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
    )
)
session.headers.update({"access_token": BSALE_TOKEN, "Content-Type": "application/json"})

def _get(url, params=None):
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

# =========================
# HELPERS
# =========================
def iso_date(d): 
    return d.strftime("%Y-%m-%d")

def parse_date(s):
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    raise SystemExit(f"Fecha inválida: {s}")

def one_line(s):
    if s is None:
        return ""
    return " ".join(str(s).split())

def epoch_range_utc_day(d: date) -> str:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end   = start + timedelta(days=1) - timedelta(seconds=1)
    return f"[{int(start.timestamp())},{int(end.timestamp())}]"

def fecha_ui(doc):
    v = doc.get("emissionDate")
    if v is None:
        return None

    if isinstance(v, (int, float)):
        ts = v / 1000 if v > 1e12 else v
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()

    if isinstance(v, str):
        s = v.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date()

    return None

# =========================
# PAGINACIÓN
# =========================
def _page_through(url, params_base):
    limit = int(params_base.get("limit", 50))
    params = {k: v for k, v in params_base.items() if k != "offset"}
    offset = 0
    pages = 0

    while True:
        if pages >= MAX_PAGES_PER_CALL:
            break

        batch_params = {**params, "limit": limit, "offset": offset}
        data = _get(url, batch_params)

        items = (data or {}).get("items", []) if isinstance(data, dict) else []
        if not items:
            break

        yield items

        if len(items) < limit:
            break

        offset += limit
        pages += 1

def _page_items(url, params_base):
    limit = int(params_base.get("limit", 50))
    params = {k: v for k, v in params_base.items() if k != "offset"}
    offset = 0
    pages = 0

    while True:
        if pages >= MAX_PAGES_PER_CALL:
            break

        batch_params = {**params, "limit": limit, "offset": offset}
        data = _get(url, batch_params)

        items = (data or {}).get("items", []) if isinstance(data, dict) else []
        if not items:
            break

        for it in items:
            yield it

        if len(items) < limit:
            break

        offset += limit
        pages += 1

# =========================
# DETAILS COMPLETOS POR DOC_ID (NO RECORTE)
# =========================
@lru_cache(maxsize=200000)
def get_document_details(doc_id: int):
    url = f"{BASE_URL}/documents/{doc_id}/details.json"
    return list(_page_items(url, {"limit": 50}))

# =========================
# TIPO DOCUMENTO
# =========================
def mapear_tipo_documento(doc):
    import re
    serie = (doc.get("serialNumber") or "").strip().upper()
    if re.match(r"^BC\d{1,4}-\d+$", serie): return "NOTA DE CREDITO"
    if re.match(r"^TI\d{1,4}-\d+$", serie): return "TRASLADO INTERNO"
    if re.match(r"^B\d{1,4}-\d+$",  serie): return "BOLETA DE VENTA ELECTRÓNICA"
    if re.match(r"^F\d{1,4}-\d+$",  serie): return "FACTURA ELECTRÓNICA"
    if re.match(r"^T\d{1,4}-\d+$",  serie): return "GUÍA DE REMISIÓN ELECTRÓNICA"
    return "Desconocido"

# =========================
# VARIANT/PRODUCT (descripción)
# =========================
@lru_cache(maxsize=10000)
def get_variant(variant_id):
    try:
        return _get(f"{BASE_URL}/variants/{variant_id}.json") or {}
    except Exception:
        return {}

@lru_cache(maxsize=10000)
def get_product(product_id):
    try:
        return _get(f"{BASE_URL}/products/{product_id}.json") or {}
    except Exception:
        return {}

# =========================
# FETCH POR DÍA (TODO)
# =========================
def fetch_documents_for_day(d: date):
    url = f"{BASE_URL}/documents.json"

    # IMPORTANTE: ya no pedimos details en expand
    expand = "[client,user,document_taxes,payments,document_type,office]"

    docs = []
    api_count = 0

    params = {
        "expand": expand,
        "limit": 50,
        "emissiondaterange": epoch_range_utc_day(d),
    }

    for items in _page_through(url, params):
        for doc in items:
            api_count += 1
            f = fecha_ui(doc)
            if f == d:
                docs.append(doc)

    log.info("DÍA %s | API=%d | FINAL=%d (sin filtro sucursal/tipo)", iso_date(d), api_count, len(docs))
    return docs

# =========================
# BUILD ROW (con details completos)
# =========================
def construir_fila(doc):
    fdate = fecha_ui(doc)
    if not fdate:
        return None
    fecha = iso_date(fdate)

    cli = doc.get("client") or {}
    cliente = cli.get("company") or one_line(f"{cli.get('firstName','')} {cli.get('lastName','')}")
    ruc = cli.get("code", "NO REGISTRADO")
    direccion = cli.get("address", "NO REGISTRADO")

    user = doc.get("user") or {}
    vendedor = one_line(f"{user.get('firstName','')} {user.get('lastName','')}")

    # ===== detalles COMPLETOS por doc_id (sin recorte) =====
    doc_id = doc.get("id")
    detalles = []
    if doc_id:
        try:
            detalles = get_document_details(int(doc_id))
        except Exception as e:
            log.warning("No se pudo traer details doc_id=%s: %s", doc_id, e)
            detalles = []

    # fallback (si fallara)
    if not detalles:
        if isinstance(doc.get("details"), dict) and "items" in doc["details"]:
            detalles = doc["details"]["items"]
        elif isinstance(doc.get("details"), list):
            detalles = doc["details"]

    cantidades, descripciones, valores_u, descuentos, totales = [], [], [], [], []

    def first_non_empty(*vals):
        for v in vals:
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    for it in detalles:
        cantidades.append(str(it.get("quantity", "")))

        unit_value = it.get("totalUnitValue")
        if unit_value is None:
            unit_value = it.get("unitValue") or it.get("unitPrice") or it.get("price") or ""


        valores_u.append(str(unit_value))

        descuentos.append(f"{it.get('discountRate', 0)} %")

        line_total = it.get("totalAmount")
        if line_total is None:
            line_total = it.get("amount") or it.get("total") or ""
        totales.append(str(line_total))

        desc_det = one_line(it.get("description") or "")

        var = it.get("variant") or {}
        if not isinstance(var, dict):
            var = {}

        variant_id = it.get("variantId") or it.get("variant_id") or var.get("id") or var.get("Id")

        product_name = ""
        variant_name = ""

        if variant_id:
            v = get_variant(int(variant_id)) or {}
            variant_name = one_line(first_non_empty(
                v.get("description"), v.get("Description"),
                v.get("name"), v.get("Name"),
                desc_det
            ))

            prod = v.get("product") or {}
            pid = prod.get("id") or prod.get("Id")

            if pid:
                p = get_product(int(pid)) or {}
                product_name = one_line(first_non_empty(
                    p.get("description"), p.get("Description"),
                    p.get("name"), p.get("Name")
                ))

        if not variant_name:
            variant_name = desc_det

        if not product_name:
            product_name = first_non_empty(
                it.get("productName"), it.get("product_name"),
                it.get("name"), it.get("Name")
            )

        parts = [x for x in (product_name, variant_name) if x]
        if not parts:
            parts = [desc_det or "SIN DESCRIPCION"]

        descripciones.append(" ".join(parts).strip())

    # impuestos
    taxes = []
    if isinstance(doc.get("document_taxes"), dict) and "items" in doc["document_taxes"]:
        taxes = doc["document_taxes"]["items"]
    elif isinstance(doc.get("document_taxes"), list):
        taxes = doc["document_taxes"]

    igv = isc = icbper = 0.0
    op_gravada = float(doc.get("netAmount", 0) or 0)
    op_exonerada = float(doc.get("exemptAmount", 0) or 0)

    for t in taxes:
        tid = str((t.get("tax") or {}).get("id"))
        monto = float(t.get("totalAmount", 0) or 0)
        if tid == "1":
            igv = monto
        elif tid == "3":
            isc = monto
        elif tid == "5":
            icbper = monto

    pagos = doc.get("payments") or []
    forma_pago = " | ".join([p.get("name", "") for p in pagos if p.get("name")]) or "NO REGISTRADO"
    total_recibido = sum(float(p.get("amount", 0) or 0) for p in pagos)
    vuelto = next((float(p.get("change", 0) or 0) for p in pagos if "change" in p), 0.0)

    tipo = mapear_tipo_documento(doc)

    office = doc.get("office") or {}
    tienda = one_line(office.get("address", ""))

    return {
        "empresa": "LA PERICA S.A.C.",
        "ruc_empresa": "20563066688",
        "cliente": cliente,
        "ruc_o_dni_cliente": ruc,
        "direccion_cliente": direccion,
        "numero_serie": doc.get("serialNumber", ""),
        "fecha": fecha,
        "tipo": tipo,
        "cantidad": " | ".join(cantidades),
        "descripcion": " | ".join(descripciones),
        "valor_u": " | ".join(valores_u),
        "descuento": " | ".join(descuentos),
        "total": " | ".join(totales),
        "op_gravada": op_gravada,
        "igv": igv,
        "icbper": icbper,
        "isc": isc,
        "op_exonerada": op_exonerada,
        "total_a_pagar": float(doc.get("totalAmount", 0) or 0),
        "total_recibido": total_recibido,
        "vuelto": vuelto,
        "forma_de_pago": forma_pago,
        "vendedor": vendedor,
        "tienda": tienda,
    }

# =========================
# MYSQL
# =========================
def get_engine():
    eng = create_engine(
        MYSQL_URL,
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True
    )
    with eng.connect() as c:
        c.execute(text("SELECT 1"))
    log.info("MySQL conectado (Railway)")
    return eng

ENGINE = get_engine()

def insert_dataframe(df):
    if df.empty:
        return {"inserted": 0, "error": None}

    meta = MetaData()
    tbl = Table(MYSQL_TABLE, meta, autoload_with=ENGINE)
    rows = df.to_dict(orient="records")

    inserted = 0
    try:
        with ENGINE.begin() as conn:
            for i in range(0, len(rows), 1000):
                batch = rows[i:i+1000]
                stmt = mysql_insert(tbl).values(batch).prefix_with("IGNORE")
                r = conn.execute(stmt)
                inserted += (r.rowcount or 0)

        return {"inserted": inserted, "error": None}
    except Exception as e:
        return {"inserted": inserted, "error": str(e)}

# =========================
# PIPELINE
# =========================
def cargar_rango_por_dia(inicio, fin):
    total = 0
    d = inicio

    while d <= fin:
        log.info("=== Día %s (UI=emissionDate UTC) ===", iso_date(d))

        docs = fetch_documents_for_day(d)
        rows = []

        for doc in docs:
            r = construir_fila(doc)
            if r:
                # DEBUG opcional para ver recortes (debería ser >25 cuando aplique)
                n_qty = len([x for x in (r["cantidad"] or "").split("|") if x.strip()])
                if n_qty == 25:
                    log.warning("POSIBLE RECORTE? serie=%s doc_id=%s items=%d", r["numero_serie"], doc.get("id"), n_qty)
                rows.append(r)

        if not rows:
            log.info("Sin documentos")
            d += timedelta(days=1)
            continue

        df = pd.DataFrame(rows)
        ins = insert_dataframe(df)

        if ins["error"]:
            log.error("Error insertando día %s: %s", iso_date(d), ins["error"])
        else:
            log.info("INSERTADOS: %d", ins["inserted"])
            total += ins["inserted"]

        d += timedelta(days=1)

    log.info("TOTAL INSERTADO = %d", total)
    return total

# =========================
# CLI
# =========================
def build_cli():
    p = argparse.ArgumentParser()
    p.add_argument("--rango", help="YYYY-MM-DD:YYYY-MM-DD")
    p.add_argument("--inicio", help="YYYY-MM-DD")
    p.add_argument("--fin", help="YYYY-MM-DD")
    return p

def main():
    args, _ = build_cli().parse_known_args()

    if args.rango:
        a, b = args.rango.split(":")
        return cargar_rango_por_dia(parse_date(a), parse_date(b))

    if args.inicio:
        ini = parse_date(args.inicio)
        fin = parse_date(args.fin) if args.fin else date.today()
        return cargar_rango_por_dia(ini, fin)

    ini = parse_date(FECHA_INICIO_RAPIDA)
    fin = parse_date(FECHA_FIN_RAPIDA)
    return cargar_rango_por_dia(ini, fin)

if __name__ == "__main__":
    total_insertados = main()

    print("RESUMEN_PROCESO")
    print(f"INSERTADOS={total_insertados or 0}")
    print("ACTUALIZADOS=0")
    print("RECHAZADOS=0")
    print("FIN_RESUMEN")

