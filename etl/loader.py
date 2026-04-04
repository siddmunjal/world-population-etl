"""Loader module for the World Population ETL pipeline."""

import os
import zipfile
import pandas as pd


class Loader:
    """Handles saving the cleaned data into a ZIP file."""

    def __init__(self, df, output_folder):
        """Initialize the Loader with the DataFrame and output folder."""
        self.df = df
        self.output_folder = output_folder

    def load(self):
        """Save the cleaned DataFrame as a CSV inside a ZIP file."""
        print("Loading data into output ZIP file...")

        os.makedirs(self.output_folder, exist_ok=True)

        csv_path = os.path.join(
            self.output_folder, "world_population_cleaned.csv"
        )
        zip_path = os.path.join(
            self.output_folder, "world_population_cleaned.zip"
        )

        self.df.to_csv(csv_path, index=False)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, "world_population_cleaned.csv")

        os.remove(csv_path)

        print(f"Success! ZIP file saved to: {zip_path}")