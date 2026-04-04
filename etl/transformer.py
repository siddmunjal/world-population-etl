"""Transformer module for the World Population ETL pipeline."""


class Transformer:
    """Handles all data cleaning and transformation."""

    def __init__(self, df):
        """Initialize the Transformer with a pandas DataFrame."""
        self.df = df.copy()

    def normalize_continent(self):
        """Capitalize continent names consistently."""
        self.df["Continent"] = self.df["Continent"].str.strip().str.title()
        return self

    def normalize_country_names(self):
        """Convert country and capital names to title case."""
        self.df["Country/Territory"] = (
            self.df["Country/Territory"].str.strip().str.title()
        )
        self.df["Capital"] = self.df["Capital"].str.strip().str.title()
        return self

    def normalize_country_codes(self):
        """Convert country codes to uppercase."""
        self.df["CCA3"] = self.df["CCA3"].str.upper()
        return self

    def convert_growth_rate(self):
        """Convert growth rate from decimal to percentage string."""
        self.df["Growth Rate"] = (
            (self.df["Growth Rate"] - 1) * 100
        ).round(2).astype(str) + "%"
        return self

    def format_population_columns(self):
        """Format population numbers with comma separators."""
        pop_columns = [
            "2022 Population", "2020 Population", "2015 Population",
            "2010 Population", "2000 Population", "1990 Population",
            "1980 Population", "1970 Population", "Area (km²)"
        ]
        for col in pop_columns:
            self.df[col] = self.df[col].apply(lambda x: f"{x:,}")
        return self

    def round_density(self):
        """Round population density to 2 decimal places."""
        self.df["Density (per km²)"] = (
            self.df["Density (per km²)"].round(2)
        )
        return self

    def add_population_change(self):
        """Add a new column showing population change from 1970 to 2022."""
        self.df["Population Change (1970-2022)"] = (
            self.df["2022 Population"].str.replace(",", "").astype(int)
            - self.df["1970 Population"].str.replace(",", "").astype(int)
        ).apply(lambda x: f"{x:,}")
        return self

    def transform(self):
        """Run all transformations in order and return cleaned DataFrame."""
        print("Transforming data...")
        (
            self.normalize_continent()
                .normalize_country_names()
                .normalize_country_codes()
                .convert_growth_rate()
                .format_population_columns()
                .round_density()
                .add_population_change()
        )
        print("Transformation complete!")
        return self.df