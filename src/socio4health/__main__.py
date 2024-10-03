import argparse
from socio4health.harmonizer import Harmonizer


def main():
    parser = argparse.ArgumentParser(description="A CLI for extracting and transforming data in socio4health.")

    # Flags for different commands
    parser.add_argument('--extract', action='store_true', help="Extract data from the specified source.")
    parser.add_argument('--transform', action='store_true', help="Transform data into Parquet files.")

    # Options for extraction
    parser.add_argument('--url', type=str, help="URL to download data from.")
    parser.add_argument('--depth', type=int, help="Depth of web scraping for data extraction.", default=0)
    parser.add_argument('--download_dir', type=str, help="Directory to download the data files.", default='data/input')
    parser.add_argument('--extensions', type=str, nargs='+', help="Allowed file extensions.",
                        default=['.csv', '.xls', '.xlsx', ".txt", ".sav", ".zip"])

    # Options for transformation
    parser.add_argument('--delete_files', action='store_true', help="Delete original files after transforming.")

    # Option to specify the output directory for transformed files
    parser.add_argument('--output_dir', type=str, help="Directory to save transformed Parquet files.",
                        default='data/output')

    args = parser.parse_args()

    harmonizer = Harmonizer()

    # Extraction process
    if args.extract:
        harmonizer.extract(url=args.url, depth=args.depth, download_dir=args.download_dir, down_ext=args.extensions)

    # Transformation process
    if args.transform:
        harmonizer.transform(delete_files=args.delete_files)


if __name__ == "__main__":
    main()
