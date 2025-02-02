import pandas as pd

# Path to the input Parquet file
parquet_file = "input_file.parquet"

# Path to save the output CSV file
csv_file = "output_file.csv"

# Read the Parquet file
df = pd.read_parquet(parquet_file)

# Save the data to a CSV file
df.to_csv(csv_file, index=False)

print(f"Converted {parquet_file} to {csv_file}")
