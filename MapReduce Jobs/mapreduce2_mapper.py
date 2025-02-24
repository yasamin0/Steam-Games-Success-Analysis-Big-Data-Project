#!/usr/bin/env python3
import sys
import csv

# Refined list of valid genres
VALID_GENRES = {
    "Action", "Adventure", "Casual", "Indie", "Massively Multiplayer",
    "Racing", "RPG", "Simulation", "Sports", "Strategy", "Early Access",
    "Violent", "Gore"
}

def main():
    reader = csv.reader(sys.stdin)
    header = next(reader, None)  # Read the header row to get column names

    if not header:
        return  # Exit if the header is not available

    # Dynamically find indices for relevant columns
    try:
        genres_idx = header.index("genres")
        success_metric_idx = header.index("success_metric")
        average_playtime_idx = header.index("average_playtime")
        ratings_idx = header.index("positive_ratings")  # Assuming positive ratings
        sales_idx = header.index("price")  # Assuming price contributes to sales
    except ValueError:
        print("Error: Required columns not found in the header.", file=sys.stderr)
        return

    # Process the data rows
    for row in reader:
        try:
            genres = row[genres_idx].split(";")  # Genres are separated by ";"
            success_metric = float(row[success_metric_idx])
            average_playtime = float(row[average_playtime_idx])
            ratings = float(row[ratings_idx])
            sales = float(row[sales_idx])

            # Emit genre, success_metric, average_playtime, ratings, and sales
            for genre in genres:
                genre = genre.strip()
                if genre in VALID_GENRES:  # Only process valid genres
                    print(f"{genre}\t{success_metric},{average_playtime},{ratings},{sales}")
        except (ValueError, IndexError):
            continue  # Skip invalid rows

if __name__ == "__main__":
    main()
