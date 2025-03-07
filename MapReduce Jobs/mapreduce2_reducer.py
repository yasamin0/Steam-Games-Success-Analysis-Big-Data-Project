#!/usr/bin/env python3
import sys

def main():
    current_genre = None # Tracks the current genre being processed.
    total_success_metric = 0.0 #Stores the cumulative success metric for a genre
    total_playtime = 0.0 #Stores the cumulative playtime for a genre.
    total_ratings = 0.0 #Stores the cumulative positive ratings for a genre.
    total_sales = 0.0 #Stores the cumulative sales (price values) for a genre.
    count = 0

    for line in sys.stdin:
        line = line.strip()
        try:
            # Parse the input line
            genre, metrics = line.split("\t") #Splits the line into:genre and metrices
            success_metric, average_playtime, ratings, sales = map(float, metrics.split(",")) #Converts all metrics to floating-point numbers.
        except ValueError:
            # Skip lines with invalid format
            continue

        if current_genre == genre: 
            # Adds success_metric, average_playtime, ratings, and sales to their respective totals.
            total_success_metric += success_metric
            total_playtime += average_playtime
            total_ratings += ratings
            total_sales += sales
            count += 1
        else:
            if current_genre is not None: #When a new genre appears
                # Output results for the previous genre
                avg_success_metric = total_success_metric / count if count > 0 else 0
                avg_playtime = total_playtime / count if count > 0 else 0
                avg_ratings = total_ratings / count if count > 0 else 0
                avg_sales = total_sales / count if count > 0 else 0
                print(f"{current_genre}\t{avg_success_metric:.2f},{avg_playtime:.2f},{avg_ratings:.2f},{avg_sales:.2f}")

            # Reset for the new genre
            current_genre = genre
            total_success_metric = success_metric
            total_playtime = average_playtime
            total_ratings = ratings
            total_sales = sales
            count = 1

    # Output results for the last genre
    if current_genre:
        avg_success_metric = total_success_metric / count if count > 0 else 0
        avg_playtime = total_playtime / count if count > 0 else 0
        avg_ratings = total_ratings / count if count > 0 else 0
        avg_sales = total_sales / count if count > 0 else 0
        print(f"{current_genre}\t{avg_success_metric:.2f},{avg_playtime:.2f},{avg_ratings:.2f},{avg_sales:.2f}")

if __name__ == "__main__":
    main()
