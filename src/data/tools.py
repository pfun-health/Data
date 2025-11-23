import logging
import logging.config

logging.config.dictConfig(
    {
        "version": 1,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"}
        },
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "standard",
            }
        },
        "loggers": {"": {"handlers": ["default"], "level": "INFO", "propagate": True}},
    }
)
from typing import Optional
import argparse
from pathlib import Path
import concurrent.futures
import zipfile
import pandas as pd


def _extract_single_zipitem(zip_path, info_item, output_path):
    logging.info("Extracting '%s'...", info_item.filename)
    with zipfile.ZipFile(zip_path, "r") as zipref:
        zipref.extract(info_item, path=output_path)


def extractFromZip(zip_path: Path | str, output_path: Path | str):
    """Extract all files from a zipfile."""
    infolist = []
    with zipfile.ZipFile(zip_path, "r") as zipref:
        infolist = [zitem for zitem in zipref.infolist()]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(
                _extract_single_zipitem, zip_path, info_item, output_path
            )
            for info_item in infolist
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logging.error(
                    f"Error extracting zipfile: {future.exception()}"
                )
    logging.info("...done extracting zip.")


def _convert_single_csv_to_parquet(csv_file: Path, parquet_path: Path) -> None:
    """Helper function to convert a single CSV file to a Parquet file."""
    parquet_file = parquet_path.joinpath(csv_file.stem + ".parquet")
    df = pd.read_csv(csv_file)
    df.to_parquet(parquet_file)


def convertCsvToParquet(csv_path: Path | str, parquet_path: Path | str):
    """Convert CSV files to Parquet files.

    Arguments:
    ----------

    csv_path : input path to directory containing CSV files.

    parquet_path : output path to directory containing .parquet files.

    """
    logging.info("Converting CSVs to Parquet...")
    csv_path = Path(csv_path)
    parquet_path = Path(parquet_path)
    parquet_path.mkdir(parents=False, exist_ok=True)

    # Find all CSV files to convert
    csv_files = [
        root.joinpath(file)
        for root, _, files in csv_path.walk()
        for file in files
        if file.endswith(".csv")
    ]

    # Use a process pool to convert files in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(_convert_single_csv_to_parquet, csv_file, parquet_path)
            for csv_file in csv_files
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logging.error(
                    f"Error converting a CSV file to Parquet: {future.exception()}"
                )


class Csv2ParquetPipeline:
    def __init__(
        self, zip_path: str | Path, csv_path: str | Path, parquet_path: str | Path
    ):
        self.zip_path = zip_path
        self.csv_path = csv_path
        self.parquet_path = parquet_path

    def __call__(self, parquet_path: Optional[str | Path] = None):
        """Execute the csv2parquet pipeline in order."""

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


def unzipCsv2Parquet(zip_path, csv_path, parquet_path):
    pipeline = Csv2ParquetPipeline(zip_path, csv_path, parquet_path)
    pipeline()


def main():
    parser = argparse.ArgumentParser(
        description="Extract CSVs from a ZIP file and convert them to Parquet format."
    )
    parser.add_argument("--zip-path", required=True, help="Path to the input ZIP file.")
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to the directory to extract CSV files into.",
    )
    parser.add_argument(
        "--parquet-path",
        required=True,
        help="Path to the directory to save Parquet files.",
    )

    parsed_args = parser.parse_args()
    unzipCsv2Parquet(
        parsed_args.zip_path, parsed_args.csv_path, parsed_args.parquet_path
    )


if __name__ == "__main__":
    main()
