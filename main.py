"""Main script to run the World Population ETL pipeline."""

import os
from etl.extractor import Extractor
from etl.transformer import Transformer
from etl.loader import Loader


def run_pipeline():
    """Run the full ETL pipeline: Extract, Transform, Load."""

    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_dir, "data", "world_population.csv")
    output_folder = os.path.join(base_dir, "output")

    print("=" * 40)
    print("  WORLD POPULATION ETL PIPELINE")
    print("=" * 40)

    extractor = Extractor(input_path)
    raw_df = extractor.extract()

    transformer = Transformer(raw_df)
    cleaned_df = transformer.transform()

    loader = Loader(cleaned_df, output_folder)
    loader.load()

    print("=" * 40)
    print("  PIPELINE COMPLETE!")
    print("=" * 40)


if __name__ == "__main__":
    run_pipeline()
