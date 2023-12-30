import pandas as pd

parquet_file_path = 'Flight_Delay.parquet'

# Read the Parquet file into a DataFrame
df = pd.read_parquet(parquet_file_path)

# Now you can work with the DataFrame 'df'
print(df.columns)