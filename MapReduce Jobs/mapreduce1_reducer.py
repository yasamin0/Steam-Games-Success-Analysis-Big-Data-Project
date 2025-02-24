#!/usr/bin/env python3
import sys

# Reducer for calculating average success metric by pricing tier
def main():
    current_tier = None # Tracks the current price tier being processed
    current_sum = 0.0 #Stores the cumulative success metric for the price tier.
    current_count = 0 #Counts the number of games in each price tier.

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

        # Group by price tier and aggregate the success metric .If the same price tier is repeated, it:Adds success_metric to current_sum.Increments the count (current_count).

        if current_tier == price_tier:
            current_sum += success_metric
            current_count += 1
        else:
            if current_tier is not None and current_count > 0: #If a new price tier appears:
                # Emit the result for the previous tier
                avg_success_metric = current_sum / current_count #Computes the average success metric for the previous tier.
                print(f"{current_tier}\t{avg_success_metric:.2f}")  #Prints the result in tab-separated format (price_tier -> avg_success_metric).
            # Reset for the new price tier
            current_tier = price_tier
            current_sum = success_metric
            current_count = 1

    # Emit the result for the last price tier .The loop doesn’t automatically print the last price tier.This ensures the last group’s result is printed.
    if current_tier is not None and current_count > 0:
        avg_success_metric = current_sum / current_count
        print(f"{current_tier}\t{avg_success_metric:.2f}")

if __name__ == "__main__":
    main()

