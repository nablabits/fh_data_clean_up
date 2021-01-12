import unittest
import os
import csv
import pandas as pd

from clean_csv import clean_bookings, clean_sales, create_upload


class TestCleanBookings(unittest.TestCase):
    """Test the cleaning of the files."""

    def test_less_columns_than_expected_raises_error(self):
        source = "./sample_test_files/less_columns_bookings.csv"
        with open(source, "r") as f:
            msg = "Usecols do not match columns"
            with self.assertRaisesRegex(ValueError, msg):
                clean_bookings(f)

    def test_not_int_pax_raises_error(self):
        source = "./sample_test_files/pax_not_int.csv"
        with self.assertRaisesRegex(ValueError, "Pax should be an int"):
            clean_bookings(source)

    def test_nan_ids_are_only_allowed_in_the_last_row(self):
        source = "./sample_test_files/nan_ids_between_bookings.csv"
        msg = "There are rows with no id."
        with self.assertRaisesRegex(ValueError, msg):
            clean_bookings(source)

    def test_valid_csv_returns_a_pandas_dataframe(self):
        source = "./sample_test_files/valid_bookings.csv"
        df = clean_bookings(source)
        self.assertIsInstance(df, pd.DataFrame)


class TestCleanSales(unittest.TestCase):

    def test_less_columns_than_expected_raises_error(self):
        source = "./sample_test_files/less_columns_sales.csv"
        with open(source, "r") as f:
            msg = "Usecols do not match columns"
            with self.assertRaisesRegex(ValueError, msg):
                clean_sales(f)

    def test_nan_ids_are_only_allowed_in_the_last_row(self):
        source = "./sample_test_files/nan_ids_between_sales.csv"
        msg = "There are rows with no id."
        with self.assertRaisesRegex(ValueError, msg):
            clean_sales(source)

    def test_valid_csv_returns_a_pandas_dataframe(self):
        source = "./sample_test_files/valid_sales.csv"
        df = clean_sales(source)
        self.assertIsInstance(df, pd.DataFrame)


class TestCreateUpload(unittest.TestCase):

    def test_create_upload_filename_should_be_bookings_or_sales(self):
        msg = "Filename should be either bookings or sales."
        with self.assertRaisesRegex(ValueError, msg):
            create_upload("foo", "bar")

    def test_create_upload_creates_a_valid_csv_file_for_bookings(self):
        source = "./sample_test_files/valid_bookings.csv"
        df = clean_bookings(source)
        create_upload(df, "bookings")
        self.assertTrue(os.path.exists("bookings.csv"))
        with open("bookings.csv", "r") as f:
            reader = csv.reader(f, delimiter=",")
            _, values = [row for row in reader]

            expected = [
                '36816723', 'No', '16:43', '2019-05-27', 'Aila a', '#153',
                'bao', '10:30', '2019-05-28', 'Mtes', 'sh', '', 'Hrtz',
                '+49 1648', 'Esl', 'N', 'om', '', '', '2', '', 'Direct',
                '52.9', '0.0', '11.1', '64.0', '52.9', '0.0', '11.1', '64.0',
                '52.9', '11.1', '64.0', '44.8', '0.0', '64.0', '0.0', '64.0',
                '0.0', 'paid', '', '7', '70% rate, taxed', 'Billing - 30%',
                '37.04', '7.76', '44.8', 'No', '0.0', '0.0', '44.8', '0.0']

            for v, e in zip(values, expected):
                self.assertEqual(v, e)

    def test_create_upload_creates_a_valid_csv_file_for_sales(self):
        source = "./sample_test_files/valid_sales.csv"
        df = clean_sales(source)
        create_upload(df, "sales")
        self.assertTrue(os.path.exists("sales.csv"))
        with open("sales.csv", "r") as f:
            reader = csv.reader(f, delimiter=",")
            _, values = [row for row in reader]

            expected = [
                '14:08', '2019-05-26', '23839', 'Online', 'tarito', 'Visa',
                '9.0', '-2.04', '766.0', '79.0', '-2.04', '76.96', '0.0',
                '0.0', '0.0', '65.3', '13.7', '362', ]
            for v, e in zip(values, expected):
                self.assertEqual(v, e)

    def test_create_upload_creates_a_sql_query_for_bookings(self):
        source = "./sample_test_files/valid_bookings.csv"
        df = clean_bookings(source)
        create_upload(df, "bookings")
        self.assertTrue(os.path.exists("bookings.sql"))
        with open("bookings.sql", "r") as f:
            lines = f.readlines()
            db_fields = [
                'id', 'cancelled', 'create_time', 'create_date', 'booked_by',
                'article_id', 'article', 'start_hour', 'start_date',
                'start_day', 'public_header', 'private_header', 'contact',
                'phone', 'language', 'opt_in_txt', 'email', 'opt_in_email',
                'notes', 'pax', 'online_ref', 'price_sheet', 'subtotal',
                'tax_21', 'tax_total', 'total', 'subtotal_paid', 'tax_21_paid',
                'tax_total_paid', 'total_paid', 'subtotal_paid_affiliate',
                'tax_paid_affiliate', 'total_paid_affiliate', 'net_profit',
                'tpv_charge', 'total_paid_after_tpv',
                'tpv_charged_to_affiliate', 'total_paid_affiliate_after_tpv',
                'debt_amount', 'payment_status', 'affiliate', 'voucher',
                'sheet_description', 'invoice_sheet', 'invoice_subtotal',
                'invoice_tax', 'invoice_total', 'invoiced',
                'affiliate_to_be_paid', 'affiliate_paid', 'affiliate_pending',
                'affiliate_received', ]
            self.assertEqual(
                lines[0], "COPY bookings({})\n".format(','.join(db_fields)))
            self.assertEqual(lines[1], "FROM '/home/redash/bookings.csv'\n")
            self.assertEqual(lines[2], "DELIMITER ','\n")
            self.assertEqual(lines[3], "CSV HEADER;")

    def test_create_upload_creates_a_sql_query_for_sales(self):
        source = "./sample_test_files/valid_sales.csv"
        df = clean_sales(source)
        create_upload(df, "sales")
        self.assertTrue(os.path.exists("sales.sql"))
        with open("sales.sql", "r") as f:
            lines = f.readlines()
            db_fields = [
                'create_time', 'create_date', 'id', 'created_by', 'pay_method',
                'card_type', 'gross', 'tpv_charge', 'net', 'gross_paid',
                'tpv_charge_paid', 'net_paid', 'gross_refund',
                'gross_refund_tpv_charge', 'net_refund', 'subtotal_paid',
                'tax_paid', 'booking_id', ]
            self.assertEqual(
                lines[0], "COPY sales({})\n".format(','.join(db_fields)))
            self.assertEqual(lines[1], "FROM '/home/redash/sales.csv'\n")
            self.assertEqual(lines[2], "DELIMITER ','\n")
            self.assertEqual(lines[3], "CSV HEADER;")

if __name__ == "__main__":
    unittest.main()
