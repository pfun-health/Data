import logging
from typing import Optional
from pathlib import Path
import concurrent.futures
import zipfile
import pandas as pd


def extractFromZip(zip_path: Path | str, output_path: Path | str):
    """Extract all files from a zipfile."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_path)
    except Exception as e:
        logging.error('Error extracting ZIP file : %s', str(e))


def _convert_single_csv_to_parquet(csv_file: Path, parquet_path: Path) -> None:
    """Helper function to convert a single CSV file to a Parquet file."""
    parquet_file = parquet_path.joinpath(csv_file.stem + '.parquet')
    df = pd.read_csv(csv_file)
    df.to_parquet(parquet_file)

def convertCsvToParquet(csv_path: Path | str, parquet_path: Path | str):
    """Convert CSV files to Parquet files.

    Arguments:
    ----------

    csv_path : input path to directory containing CSV files.

    parquet_path : output path to directory containing .parquet files.

    """
    csv_path = Path(csv_path)
    parquet_path = Path(parquet_path)
    parquet_path.mkdir(parents=False, exist_ok=True)

    # Find all CSV files to convert
    csv_files = [
        root.joinpath(file)
        for root, _, files in csv_path.walk()
        for file in files if file.endswith('.csv')
    ]

    # Use a process pool to convert files in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(_convert_single_csv_to_parquet, csv_file, parquet_path) for csv_file in csv_files]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logging.error(f'Error converting a CSV file to Parquet: {future.exception()}')


class Csv2ParquetPipeline:
    def __init__(self, zip_path: str | Path, csv_path: str | Path, parquet_path: str | Path):
        self.zip_path = zip_path
        self.csv_path = csv_path
        self.parquet_path = parquet_path

    def __call__(self, parquet_path: Optional[str | Path] = None):
        """Execute the csv2parquet pipeline in order.
        """

        if parquet_path is not None:
            self.parquet_path = parquet_path

        # Extract ZIP -> CSVs
        self.extract_csvs_from_zip()

        # Convert CSVs to Parquet
        self.convert_csvs_to_parquet()

        return self

    def extract_csvs_from_zip(self):
        return extractFromZip(self.zip_path, self.csv_path)

    def convert_csvs_to_parquet(self):
        return convertCsvToParquet(self.csv_path, self.parquet_path)
