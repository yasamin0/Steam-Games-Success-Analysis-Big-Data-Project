#!/usr/bin/env python3
import sys

# Reducer for calculating average success metric by pricing tier
def main():
    current_tier = None
    current_sum = 0.0
    current_count = 0

    for line in sys.stdin:
        # Strip and parse the line
        line = line.strip()
        try:
            price_tier, success_metric = line.split("\t")
            success_metric = float(success_metric)

            # Skip invalid or infinite values
            if success_metric == float('inf') or success_metric < 0:
                continue
        except ValueError:
            # Skip invalid or malformed lines
            continue

        # Group by price tier and aggregate the success metric
        if current_tier == price_tier:
            current_sum += success_metric
            current_count += 1
        else:
            if current_tier is not None and current_count > 0:
                # Emit the result for the previous tier
                avg_success_metric = current_sum / current_count
                print(f"{current_tier}\t{avg_success_metric:.2f}")
            # Reset for the new price tier
            current_tier = price_tier
            current_sum = success_metric
            current_count = 1

    # Emit the result for the last price tier
    if current_tier is not None and current_count > 0:
        avg_success_metric = current_sum / current_count
        print(f"{current_tier}\t{avg_success_metric:.2f}")

if __name__ == "__main__":
    main()

