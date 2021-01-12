#!/usr/bin/python3

"""Clean the csv file downloaded from fareharbor and create the SQL query
for the dashboard."""

import sys
import argparse
import pandas as pd


def main():
    """Entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_csv", help="The raw csv to be cleaned", type=str)
    parser.add_argument("origin", choices=["b", "s"], type=str,
                        help="Determine the source of the data")
    args = parser.parse_args()

    if args.origin == "b":
        df = clean_bookings(args.raw_csv)
        create_upload(df, "bookings")
    else:
        df = clean_sales(args.raw_csv)
        create_upload(df, "sales")


def clean_bookings(source: str) -> pd.DataFrame:
    """Clean the bookings data.

    Arguments:
    source, a path like string that points to a csv file

    Returns:
    A pandas DataFrame object with the data ready to be uploaded.
    """


    col_map = {
        'ID de reserva': 'id',
        '¿Cancelado?': 'cancelled',
        'Creado en hora': 'create_time',
        'Creado en fecha': 'create_date',
        'Reservado por': 'booked_by',
        'ID de artículo': 'article_id',
        'Artículo': 'article',
        'Hora de inicio': 'start_hour',
        'Fecha de inicio': 'start_date',
        'Día de disponibilidad': 'start_day',
        'Encabezado': 'public_header',
        'Encabezado privado': 'private_header',
        'Contacto': 'contact',
        'Teléfono': 'phone',
        'Idioma de contacto': 'language',
        '¿Suscrito a mensajes de texto?': 'opt_in_txt',
        'E-mail': 'email',
        '¿Está suscrito a e-mail?': 'opt_in_email',
        'Notas de reserva': 'notes',
        'N.º de pasajeros': 'pax',
        'Referencia de reserva online': 'online_ref',
        'Hoja de total': 'price_sheet',
        'Subtotal': 'subtotal',
        'Impuesto (21 %)': 'tax_21',
        'Impuesto total': 'tax_total',
        'Total': 'total',
        'Subtotal pagado': 'subtotal_paid',
        'Impuesto (21 %) pagado': 'tax_21_paid',
        'Impuestos totales pagados': 'tax_total_paid',
        'Total pagado': 'total_paid',
        'Subtotal pagado a Afiliado': 'subtotal_paid_affiliate',
        'Impuestos pagados a Afiliado': 'tax_paid_affiliate',
        'Total pagado a Afiliado': 'total_paid_affiliate',
        'Ingresos netos Cobrados': 'net_profit',
        'Gastos de gestión': 'tpv_charge',
        'Total pagado tras los gastos de gestión': 'total_paid_after_tpv',
        'Gastos de gestión Cobrado al Afiliado': 'tpv_charged_to_affiliate',
        'Total pagado al Afiliado tras los gastos de gestión':
            'total_paid_affiliate_after_tpv',
        'Cantidad debida': 'debt_amount',
        'Estado de pago': 'payment_status',
        'Afiliado': 'affiliate',
        'Cupón': 'voucher',
        'Descripción de hoja de factura': 'sheet_description',
        'Hoja de factura': 'invoice_sheet',
        'Factura Subtotal': 'invoice_subtotal',
        'Impuesto de factura': 'invoice_tax',
        'Total de factura': 'invoice_total',
        '¿Facturado?': 'invoiced',
        'A pagar al afiliado': 'affiliate_to_be_paid',
        'Pagado a Afiliado': 'affiliate_paid',
        'Por cobrar al afiliado': 'affiliate_pending',
        'Recibido de Afiliado': 'affiliate_received'
    }

    # If any of the columns in the col_map is missing, it will raise value
    # error, whereas if there are more on the csv file they won't be used. This
    # way we ensure that only the columns in the map are used, no more no less
    bookings_raw = pd.read_csv(
        source, header=1, engine='python', usecols=col_map.keys())

    bookings = bookings_raw.rename(columns=col_map)

    if bookings.pax.dtype != 'int64':
        raise ValueError("Pax should be an int")

    # Last row is likely to have totals and therefore won't have booking id so
    # get rid of it
    last_row = bookings.iloc[-1:].index
    f = bookings.index.isin(last_row) & bookings.id.isna()
    if f.sum():
        bookings.drop(index=bookings[f].index, inplace=True)

    # Ensure there are no more nan ids across the dataframe
    if not bookings[bookings.id.isna()].empty:
        raise ValueError("There are rows with no id.")

    # Transform float columns
    float_cols = [
        'subtotal',
        'tax_21',
        'tax_total',
        'total',
        'subtotal_paid',
        'tax_21_paid',
        'tax_total_paid',
        'total_paid',
        'subtotal_paid_affiliate',
        'tax_paid_affiliate',
        'total_paid_affiliate',
        'net_profit',
        'tpv_charge',
        'total_paid_after_tpv',
        'tpv_charged_to_affiliate',
        'total_paid_affiliate_after_tpv',
        'debt_amount',
        'invoice_subtotal',
        'invoice_tax',
        'invoice_total',
        'affiliate_to_be_paid',
        'affiliate_paid',
        'affiliate_pending',
        'affiliate_received', ]

    for col in float_cols:
        bookings.loc[:, col] = bookings[col].str.replace('.', '')
        bookings.loc[:, col] = bookings[col].str.replace(',', '.')
        bookings.loc[:, col] = bookings[col].str.replace('€', '').str.strip()
        bookings.loc[:, col] = bookings[col].astype(float)

    # Convert ids to ints
    bookings.id = bookings.id.str.replace('#', '').astype(int)

    return bookings


def clean_sales(source: str) -> pd.DataFrame:
    """Clean the sales data.

    Arguments:
    source, a path like string that points to a csv file

    Returns:
    A pandas DataFrame object with the data ready to be uploaded.
    """
    col_map = {
        'Creado en hora': 'create_time',
        'Creado en fecha': 'create_date',
        'ID de pago o reembolso': 'id',
        'Creado por': 'created_by',
        'Tipo de pago': 'pay_method',
        'Tipo de tarjeta de crédito': 'card_type',
        'Bruto': 'gross',
        'Gasto de gestión': 'tpv_charge',
        'Neto': 'net',
        'Pago bruto': 'gross_paid',
        'Gasto de gestión de pago': 'tpv_charge_paid',
        'Pago neto': 'net_paid',
        'Reembolso bruto': 'gross_refund',
        'Gasto de gestión de reembolso': 'gross_refund_tpv_charge',
        'Reembolso neto': 'net_refund',
        'Subtotal pagado': 'subtotal_paid',
        'Impuesto pagado': 'tax_paid',
        'ID de reserva': 'booking_id', }

    # As with clean_bookings, if any of the columns in the col_map is missing,
    # it will raise value error, whereas if there are more on the csv file they
    # won't be used. This way we ensure that only the columns in the map are
    # used, no more no less
    sales_raw = pd.read_csv(
        source, header=1, engine='python', usecols=col_map.keys())

    sales = sales_raw.rename(columns=col_map)

    # Last row is likely to have totals and therefore won't have booking id so
    # get rid of it
    last_row = sales.iloc[-1:].index
    f = sales.index.isin(last_row) & sales.id.isna()
    if f.sum():
        sales.drop(index=sales[f].index, inplace=True)

    # Ensure there are no more nan ids across the dataframe
    if not sales[sales.id.isna()].empty:
        raise ValueError("There are rows with no id.")

    # Transform float columns
    float_cols = [
        'gross',
        'tpv_charge',
        'net',
        'gross_paid',
        'tpv_charge_paid',
        'net_paid',
        'gross_refund',
        'gross_refund_tpv_charge',
        'net_refund',
        'subtotal_paid',
        'tax_paid', ]

    for col in float_cols:
        sales.loc[:, col] = sales[col].str.replace('.', '')
        sales.loc[:, col] = sales[col].str.replace(',', '.')
        sales.loc[:, col] = sales[col].str.replace('€', '').str.strip()
        sales.loc[:, col] = sales[col].astype(float)

    # Convert ids to ints
    sales.id = sales.id.str.replace('#', '').astype(int)
    sales.booking_id = sales.booking_id.str.replace('#', '').astype(int)

    return sales

def create_upload(df: pd.DataFrame, filename: str) -> str:
    """Create an upload file out of a cleaned dataframe."""

    if filename not in ("bookings", "sales"):
        raise ValueError("Filename should be either bookings or sales.")

    df.to_csv('{}.csv'.format(filename), index=False)

    # create a SQL upload query
    sql_query = (
        """COPY {}({})
FROM '/home/redash/{}.csv'
DELIMITER ','
CSV HEADER;""").format(filename, ','.join(df.columns.tolist()), filename)
    sql_file = "{}.sql".format(filename)
    with open(sql_file, 'w') as f:
        f.write(sql_query)


if __name__ == "__main__":
    main()
