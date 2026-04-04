"""Extractor module for the World Population ETL pipeline."""

import pandas as pd


class Extractor:
    """Handles extracting data from the CSV source file."""

    def __init__(self, file_path):
        """Initialize the Extractor with the path to the CSV file."""
        self.file_path = file_path

    def extract(self):
        """Read the CSV file and return a pandas DataFrame."""
        print(f"Extracting data from {self.file_path}...")
        df = pd.read_csv(self.file_path)
        print(f"Successfully extracted {len(df)} rows.")
        return df