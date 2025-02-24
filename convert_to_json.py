import json   
#This script reads the output files from three different MapReduce jobs and converts them into structured JSON files for further processing
# Open the MapReduce output file
with open("mapreduce1_output.txt", "r") as file:
    data = []

    for line in file:
        try:
            # Strip whitespace and split the line into key-value pairs
            key, value = line.strip().split("\t")
            value = float(value)  # Convert the value to a float
            
            # Append as JSON
            data.append({
                "key": key,
                "value": value
            })
        except ValueError:
            print(f"Skipping malformed line: {line.strip()}")

# Write the JSON output to a file
with open("mapreduce1_output.json", "w") as json_file:
    json.dump(data, json_file, indent=4)  #indent=4 → Formats the JSON output with 4 spaces per level for readability.The json.dump() function in Python is used to write JSON data to a file.

print("JSON file for MapReduce1 successfully created.")

import json

# Open the MapReduce2 output file
with open("mapreduce2_output.txt", "r") as file:
    data = []

    for line in file:
        try:
            # Strip whitespace and split the line
            genre, metrics = line.strip().split("\t")
            success_metric, average_playtime, positive_ratings, negative_ratings = map(float, metrics.split(","))
            
            # Append as JSON
            data.append({
                "genre": genre,
                "success_metric": success_metric,
                "average_playtime": average_playtime,
                "positive_ratings": positive_ratings,
                "negative_ratings": negative_ratings
            })
        except ValueError:
            print(f"Skipping malformed line (incorrect number of metrics): {line.strip()}")

# Write the JSON output to a file
with open("mapreduce2_output.json", "w") as json_file:
    json.dump(data, json_file, indent=4)

print("JSON file for MapReduce2 successfully created.")

# Open the MapReduce3 output file
with open("mapreduce3_output.txt", "r") as file:
    data = {}

    for line in file:
        try:
            # Parse the line to extract the metric and its value
            parts = line.strip().split(":")
            metric_description = parts[0].strip()  # Extract the metric description
            correlation_value = float(parts[1].strip())  # Convert the value to float
            
            # Store as key-value pair
            data[metric_description] = correlation_value
        except (ValueError, IndexError):
            print(f"Skipping malformed line: {line.strip()}")

# Write the JSON output to a file
with open("mapreduce3_output.json", "w") as json_file:
    json.dump(data, json_file, indent=4)

print("JSON file for MapReduce3 successfully created.")
