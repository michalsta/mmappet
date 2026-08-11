import numpy as np

from .mmappet import _read_schema_tbl
from .mmappet import open_dataset_dct
from .mmappet import open_new_dataset_dct
from pathlib import Path


def append_empty_columns(folder: str | Path, **name2dtype):
    """
    Empty additional empty column to an existing columnwise-non-empty dataset.

    Example:
        `append_empty_columns(dataset_path, mz=np.float32, bones=np.bool)`
    """
    folder = Path(folder)
    dataset = open_dataset_dct(folder)
    assert len(dataset), "Appending only if non-empty dataset."
    nrows = len(dataset[tuple(dataset)[0]])
    column_number = len(dataset)
    for name in name2dtype:
        assert name not in dataset
    schema_path = folder / "schema.txt"
    # DatasetWriter/write_schema never terminate schema.txt with a trailing
    # newline (schema_to_str is "\n".join(...)), so a plain append-mode write
    # here would concatenate the new column onto the previous line instead of
    # starting a fresh one.
    existing = schema_path.read_text()
    if existing and not existing.endswith("\n"):
        existing += "\n"
    with open(schema_path, "w") as schema:
        schema.write(existing)
        for name, dtype in name2dtype.items():
            dtype = np.dtype(dtype)
            column_size = dtype.itemsize
            with open(folder / f"{column_number}.bin", "xb") as file:
                file.truncate(nrows * column_size)
            column_number += 1
            schema.write(f"{dtype} {name}")


def crop_dataset(
    input_dataset_path: str | Path, output_dataset_path: str | Path, rows_cnt: int
) -> None:
    in_data = open_dataset_dct(input_dataset_path)
    data_schema = _read_schema_tbl(input_dataset_path)
    out_data = open_new_dataset_dct(output_dataset_path, data_schema, rows_cnt)
    for colname, values in in_data.items():
        out_data[colname][:] = values[:rows_cnt]
