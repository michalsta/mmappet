import argparse
from pathlib import Path

import mmappet


def main():
    parser = argparse.ArgumentParser(
        description="Display the schema and first rows of an mmappet dataset."
    )
    parser.add_argument(
        "dataset_path", type=Path, help="Path to the mmappet dataset directory."
    )
    args = parser.parse_args()

    arrays = mmappet.open_dataset_dct(args.dataset_path, read_write=False)
    for name, array in arrays.items():
        print(f"{name} ({array.dtype}, {len(array)} rows): {array[:10]!r}")


if __name__ == "__main__":
    main()
