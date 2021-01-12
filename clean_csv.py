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
        return clean_bookings(args.raw_csv)
    else:
        return clean_sales(args.raw_csv)


def clean_bookings(source):
    """Clean the bookings data."""
    print("cleaning bookings source {}.".format(source))


def clean_sales(source):
    """Clean the sales data."""
    print("Cleaning sales source {}.".format(source))


if __name__ == "__main__":
    main()
