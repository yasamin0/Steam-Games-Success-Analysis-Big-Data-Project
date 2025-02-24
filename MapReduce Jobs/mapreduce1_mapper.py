#!/usr/bin/env python3
import sys
import csv

def main():
    reader = csv.reader(sys.stdin)
    header = next(reader, None)  # Read the header row to get column names

    if not header:
        return  # Exit if the header is not available

    # Dynamically find the indices for 'price_tier' and 'success_metric'
    try:
        price_tier_idx = header.index("price_tier")  # Replace with the exact column name in your CSV
        success_metric_idx = header.index("success_metric")  # Replace with the exact column name
    except ValueError:
        print("Error: Required columns not found in the header.", file=sys.stderr)
        return

    # Process the data rows
    for row in reader:
        try:
            price_tier = row[price_tier_idx].strip()
            success_metric = float(row[success_metric_idx])

            # Skip rows where price_tier is empty, invalid, or explicitly '0'
            if not price_tier or price_tier == '0':
                continue

            print(f"{price_tier}\t{success_metric}")
        except (ValueError, IndexError):
            continue  # Skip invalid rows

if __name__ == "__main__":
    main()

