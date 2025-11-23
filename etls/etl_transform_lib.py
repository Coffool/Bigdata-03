"""
Librería de transformaciones para ETLs analíticos (Data Lake / Glue Catalog)

Estas funciones usan únicamente DataFrames pandas (simulando datos ya extraídos
vía Glue Catalog o archivos en S3) para permitir pruebas unitarias sin depender
de conexión JDBC ni del runtime de AWS Glue.
"""

from datetime import datetime
import pandas as pd

__all__ = [
    "transform_sales_per_day",
    "compute_monthly_artist_metrics",
    "select_top_artist_per_month",
    "transform_sales_weekday",
    "transform_monthly_sales"
]


def transform_sales_per_day(df_invoice: pd.DataFrame, df_invoice_line: pd.DataFrame) -> pd.DataFrame:
    """Agrega ventas por día usando Invoice e InvoiceLine.

    Requisitos mínimos de columnas:
    - df_invoice: InvoiceId, InvoiceDate, CustomerId
    - df_invoice_line: InvoiceId, TrackId, UnitPrice, Quantity
    """
    if df_invoice.empty or df_invoice_line.empty:
        return pd.DataFrame()

    # Join
    merged = df_invoice_line.merge(
        df_invoice[["InvoiceId", "InvoiceDate", "CustomerId"]],
        on="InvoiceId", how="inner"
    )
    merged["InvoiceDate"] = pd.to_datetime(merged["InvoiceDate"], errors="coerce")
    merged = merged.dropna(subset=["InvoiceDate"])  # eliminar fechas inválidas

    # Monto por línea
    merged["monto_linea"] = merged["UnitPrice"] * merged["Quantity"]
    merged["fecha"] = merged["InvoiceDate"].dt.date

    grouped = merged.groupby("fecha").agg(
        total_canciones_vendidas=("TrackId", "count"),
        cantidad_total=("Quantity", "sum"),
        numero_facturas=("InvoiceId", "nunique"),
        clientes_unicos=("CustomerId", "nunique"),
        monto_total=("monto_linea", "sum")
    ).reset_index()

    grouped["fecha"] = pd.to_datetime(grouped["fecha"])  # para añadir metadata temporal
    grouped["dia_semana"] = grouped["fecha"].dt.day_name()
    grouped["mes"] = grouped["fecha"].dt.month
    grouped["año"] = grouped["fecha"].dt.year
    grouped["trimestre"] = grouped["fecha"].dt.quarter

    grouped["avg_canciones_por_factura"] = (
        grouped["total_canciones_vendidas"] / grouped["numero_facturas"]
    ).round(2)
    grouped["ticket_promedio"] = (
        grouped["monto_total"] / grouped["numero_facturas"]
    ).round(2)

    return grouped.sort_values("fecha", ascending=False)


def compute_monthly_artist_metrics(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Agrega métricas mensuales por artista.
    Requiere columnas: InvoiceDate, TrackId, Quantity, UnitPrice, ArtistId / ArtistName opcionales.
    """
    if df_sales.empty:
        return pd.DataFrame()

    df = df_sales.copy()
    possible_date_cols = ["InvoiceDate", "invoice_date", "Date"]
    date_col = next((c for c in possible_date_cols if c in df.columns), None)
    if date_col is None:
        raise ValueError("No se encontró columna de fecha en datos de ventas")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])

    artist_id_col = next((c for c in ["ArtistId", "artist_id"] if c in df.columns), None)
    artist_name_col = next((c for c in ["ArtistName", "nombre_artista", "Name"] if c in df.columns), None)
    qty_col = next((c for c in ["Quantity", "quantity"] if c in df.columns), None)
    price_col = next((c for c in ["UnitPrice", "unitprice"] if c in df.columns), None)
    track_col = next((c for c in ["TrackId", "trackid"] if c in df.columns), None)

    df["mes"] = df[date_col].dt.to_period("M").dt.to_timestamp()
    if qty_col and price_col:
        df["line_amount"] = pd.to_numeric(df[price_col]) * pd.to_numeric(df[qty_col])
    else:
        df["line_amount"] = 0.0

    df["track_count"] = 1 if track_col else 1

    group_cols = ["mes"]
    if artist_id_col:
        group_cols.append(artist_id_col)
    if artist_name_col and artist_name_col != artist_id_col:
        group_cols.append(artist_name_col)

    agg = df.groupby(group_cols).agg(
        total_canciones_vendidas=("track_count", "sum"),
        cantidad_total=(qty_col if qty_col else "track_count", "sum"),
        monto_total=("line_amount", "sum"),
        numero_facturas=("InvoiceId" if "InvoiceId" in df.columns else track_col, "nunique"),
        clientes_unicos=("CustomerId" if "CustomerId" in df.columns else track_col, "nunique")
    ).reset_index()

    if artist_id_col and artist_id_col in agg.columns:
        agg = agg.rename(columns={artist_id_col: "ArtistId"})
    if artist_name_col and artist_name_col in agg.columns:
        agg = agg.rename(columns={artist_name_col: "nombre_artista"})

    agg["nombre_mes"] = agg["mes"].dt.month_name()
    agg["año"] = agg["mes"].dt.year
    agg["numero_mes"] = agg["mes"].dt.month

    return agg


def select_top_artist_per_month(df_month_artist: pd.DataFrame) -> pd.DataFrame:
    if df_month_artist.empty:
        return df_month_artist
    idx = df_month_artist.groupby("mes")["total_canciones_vendidas"].idxmax()
    top = df_month_artist.loc[idx].reset_index(drop=True)

    totals = df_month_artist.groupby("mes").agg({
        "total_canciones_vendidas": "sum",
        "monto_total": "sum"
    }).rename(columns={"total_canciones_vendidas": "total_mes_canciones", "monto_total": "total_mes_monto"})

    top = top.merge(totals, on="mes", how="left")
    top["porcentaje_canciones"] = (
        top["total_canciones_vendidas"] / top["total_mes_canciones"] * 100
    ).round(2)
    top["porcentaje_monto"] = (
        top["monto_total"] / top["total_mes_monto"] * 100
    ).round(2)
    top["precio_promedio_cancion"] = (
        top["monto_total"] / top["total_canciones_vendidas"]
    ).round(2)
    return top.sort_values("mes", ascending=False)


def transform_sales_weekday(df_invoice: pd.DataFrame, df_invoice_line: pd.DataFrame) -> pd.DataFrame:
    if df_invoice.empty or df_invoice_line.empty:
        return pd.DataFrame()
    merged = df_invoice_line.merge(df_invoice[["InvoiceId", "InvoiceDate", "CustomerId"]], on="InvoiceId", how="inner")
    merged["InvoiceDate"] = pd.to_datetime(merged["InvoiceDate"], errors="coerce")
    merged = merged.dropna(subset=["InvoiceDate"])

    merged["dia_semana"] = merged["InvoiceDate"].dt.day_name()
    merged["monto_linea"] = merged["UnitPrice"] * merged["Quantity"]

    grouped = merged.groupby("dia_semana").agg(
        total_canciones_vendidas=("TrackId", "count"),
        cantidad_total=("Quantity", "sum"),
        numero_facturas=("InvoiceId", "nunique"),
        clientes_unicos=("CustomerId", "nunique"),
        monto_total=("monto_linea", "sum")
    ).reset_index()

    grouped["avg_canciones_por_factura"] = (
        grouped["total_canciones_vendidas"] / grouped["numero_facturas"]
    ).round(2)
    grouped["ticket_promedio"] = (
        grouped["monto_total"] / grouped["numero_facturas"]
    ).round(2)

    return grouped


def transform_monthly_sales(df_invoice: pd.DataFrame, df_invoice_line: pd.DataFrame) -> pd.DataFrame:
    if df_invoice.empty or df_invoice_line.empty:
        return pd.DataFrame()
    merged = df_invoice_line.merge(df_invoice[["InvoiceId", "InvoiceDate", "CustomerId"]], on="InvoiceId", how="inner")
    merged["InvoiceDate"] = pd.to_datetime(merged["InvoiceDate"], errors="coerce")
    merged = merged.dropna(subset=["InvoiceDate"])

    merged["monto_linea"] = merged["UnitPrice"] * merged["Quantity"]
    merged["year_month"] = merged["InvoiceDate"].dt.to_period("M").astype(str)

    grouped = merged.groupby("year_month").agg(
        total_canciones_vendidas=("TrackId", "count"),
        cantidad_total=("Quantity", "sum"),
        numero_facturas=("InvoiceId", "nunique"),
        clientes_unicos=("CustomerId", "nunique"),
        monto_total=("monto_linea", "sum")
    ).reset_index()

    grouped["avg_canciones_por_factura"] = (
        grouped["total_canciones_vendidas"] / grouped["numero_facturas"]
    ).round(2)
    grouped["ticket_promedio"] = (
        grouped["monto_total"] / grouped["numero_facturas"]
    ).round(2)

    return grouped
