import mmappet
import pandas as pd
import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Display the dataframe (schema and first few rows) stored in an mmappet dataset directory."
    )
    parser.add_argument(
        "dataset_path", type=Path, help="Path to the mmappet dataset directory."
    )
    args = parser.parse_args()

    print(mmappet.open_dataset(args.dataset_path, read_write=False))


if __name__ == "__main__":
    main()
