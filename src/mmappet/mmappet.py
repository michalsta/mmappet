from __future__ import annotations

import mmap
import os
from collections.abc import Mapping, Sequence
from os import PathLike
from pathlib import Path
from types import SimpleNamespace
from typing import Any, BinaryIO

import numpy as np
import numpy.typing as npt

Schema = dict[str, np.dtype]
SchemaLike = Mapping[str, Any]
PathType = str | PathLike[str]


def _normalize_schema(schema: SchemaLike) -> Schema:
    if not isinstance(schema, Mapping):
        raise TypeError("schema must be a mapping of column names to NumPy dtypes")
    normalized: Schema = {}
    for name, value in schema.items():
        if not isinstance(name, str) or not name:
            raise TypeError("schema column names must be non-empty strings")
        try:
            normalized[name] = np.dtype(value)
        except TypeError:
            if not hasattr(value, "dtype"):
                raise
            normalized[name] = np.dtype(value.dtype)
    if not normalized:
        raise ValueError("schema must contain at least one column")
    return normalized


def schema_to_str(schema: SchemaLike) -> str:
    """Serialize an ordered mapping of column names to NumPy dtypes."""

    return "\n".join(
        f"{dtype} {name}" for name, dtype in _normalize_schema(schema).items()
    )


def str_to_schema(value: str) -> Schema:
    """Parse a schema string into an ordered dtype mapping."""

    schema: Schema = {}
    for line in value.splitlines():
        dtype_string, column_name = line.split(maxsplit=1)
        schema[column_name] = np.dtype(dtype_string)
    if not schema:
        raise ValueError("schema must contain at least one column")
    return schema


def write_schema(schema: SchemaLike, path: PathType) -> None:
    with open(Path(path) / "schema.txt", "wt") as file:
        file.write(schema_to_str(schema))


def _read_schema(path: PathType) -> Schema:
    with open(Path(path) / "schema.txt", "rt") as file:
        return str_to_schema(file.read())


def get_schema(**columns: npt.DTypeLike) -> Schema:
    """Return an ordered mapping suitable for mmappet dataset creation."""

    return _normalize_schema(columns)


class DatasetWriter:
    def __init__(
        self, path: PathType, append_ok: bool = False, overwrite_dir: bool = False
    ):
        if append_ok and overwrite_dir:
            raise ValueError("Cannot set both append_ok and overwrite_dir to True.")
        self.path = Path(path)
        self.files: list[BinaryIO] = []
        self.colnames: list[str] = []
        self.dtypes: list[np.dtype] = []
        self.schema: Schema = {}
        self._initialized = False
        self.length = 0

        if self.path.exists() and overwrite_dir:
            import shutil

            shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)

        if append_ok and (self.path / "schema.txt").is_file():
            self._reset_schema(_read_schema(self.path))

    @staticmethod
    def preallocate_dataset(
        path: PathType,
        schema: SchemaLike,
        nrows: int,
        overwrite_dir: bool = False,
    ) -> None:
        if nrows < 0:
            raise ValueError("nrows must be non-negative")
        with DatasetWriter(path=path, overwrite_dir=overwrite_dir) as writer:
            writer._reset_schema(schema)
            for file, dtype in zip(writer.files, writer.dtypes):
                file.truncate(nrows * dtype.itemsize)

    def _reset_schema(self, schema: SchemaLike) -> None:
        self.close()
        self.schema = _normalize_schema(schema)
        self.files = []
        self.colnames = list(self.schema)
        self.dtypes = list(self.schema.values())

        lengths = []
        for index, dtype in enumerate(self.dtypes):
            file_path = self.path / f"{index}.bin"
            # These handles intentionally remain open for the writer lifetime.
            self.files.append(open(file_path, "ab", buffering=10240))  # noqa: SIM115
            size = file_path.stat().st_size
            if size % dtype.itemsize:
                raise RuntimeError(
                    f"Corrupted dataset: {file_path} size is not divisible "
                    f"by dtype {dtype}"
                )
            lengths.append(size // dtype.itemsize)

        if lengths and not all(length == lengths[0] for length in lengths):
            raise RuntimeError(
                f"Corrupted dataset: columns of unequal lengths: {self.path}"
            )

        self.length = 0 if not lengths else lengths[0]
        write_schema(self.schema, self.path)
        self._initialized = True

    @classmethod
    def new(
        cls,
        path: PathType,
        append_ok: bool = False,
        overwrite_dir: bool = False,
        **columns: npt.DTypeLike,
    ) -> DatasetWriter:
        if not columns:
            raise ValueError(
                "DatasetWriter.new requires column=dtype arguments, for "
                "example scan=np.uint32"
            )
        writer = cls(path, append_ok, overwrite_dir)
        writer._reset_schema(columns)
        return writer

    def close(self) -> None:
        for file in self.files:
            file.close()
        self.files = []
        self._initialized = False

    def __del__(self):
        self.close()

    def __enter__(self) -> DatasetWriter:  # noqa: PYI034
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def __len__(self) -> int:
        return self.length

    def append(self, **columns: npt.ArrayLike) -> None:
        """Append equally sized one-dimensional columns."""

        if not columns:
            raise ValueError("append requires at least one column")
        if not self._initialized:
            schema = self.schema or {
                name: np.asarray(values).dtype for name, values in columns.items()
            }
            self._reset_schema(schema)
        if list(columns) != self.colnames:
            raise ValueError(
                f"Columns must be {self.colnames} in that order; got {list(columns)}"
            )

        arrays = [
            np.asarray(columns[name], dtype=dtype)
            for name, dtype in zip(self.colnames, self.dtypes)
        ]
        if any(array.ndim != 1 for array in arrays):
            raise ValueError("appended columns must be one-dimensional")
        lengths = [len(array) for array in arrays]
        if not all(length == lengths[0] for length in lengths):
            raise ValueError(f"appended columns have unequal lengths: {lengths}")

        for file, array in zip(self.files, arrays):
            file.write(array.tobytes())
        self.length += lengths[0]

    def append_df(self, dataframe) -> None:
        """Append a pandas DataFrame when the optional extra is installed."""

        try:
            columns = {
                name: dataframe[name].to_numpy(copy=False) for name in dataframe.columns
            }
        except AttributeError as error:
            raise TypeError("append_df expects a pandas DataFrame") from error
        self.append(**columns)

    def append_column(self, colname: str, column: npt.ArrayLike) -> None:
        if not self._initialized:
            raise ValueError("append_column requires an initialized schema")
        array = np.asarray(column)
        if array.ndim != 1:
            raise ValueError("column must be one-dimensional")
        if len(array) != len(self):
            raise ValueError(
                f"Column {colname!r} has {len(array)} rows; expected {len(self)}"
            )
        if colname in self.schema:
            raise ValueError(f"Column already exists: {colname}")

        schema = dict(self.schema)
        schema[colname] = array.dtype
        with open(self.path / f"{len(self.files)}.bin", "xb") as file:
            file.write(array.tobytes())
        self._reset_schema(schema)

    def append_columns(self, **columns: npt.ArrayLike) -> None:
        arrays = {name: np.asarray(values) for name, values in columns.items()}
        for name, array in arrays.items():
            if len(array) != len(self):
                raise ValueError(
                    f"Column {name!r} has {len(array)} rows; expected {len(self)}"
                )
        for name, array in arrays.items():
            self.append_column(name, array)

    def flush(self) -> None:
        for file in self.files:
            file.flush()

    def append_row(self, **values: Any) -> None:
        if not self._initialized:
            self._reset_schema(
                {name: np.asarray(value).dtype for name, value in values.items()}
            )
        self.append(
            **{
                name: np.asarray([values[name]], dtype=dtype)
                for name, dtype in zip(self.colnames, self.dtypes)
            }
        )

    def append_dct(self, values: Mapping[str, npt.ArrayLike]) -> None:
        self.append(**values)

    def append_list(self, columns: Sequence[npt.ArrayLike]) -> None:
        if not self._initialized:
            raise ValueError("append_list requires an initialized schema")
        if len(columns) != len(self.colnames):
            raise ValueError(
                f"Expected {len(self.colnames)} columns; got {len(columns)}"
            )
        self.append(**dict(zip(self.colnames, columns)))


def open_dataset_dct(
    path: PathType, read_write: bool = False, **kwargs
) -> dict[str, np.ndarray]:
    """Open a dataset as an ordered dictionary of mmap-backed NumPy arrays."""

    path = Path(path)
    schema = _read_schema(path)
    arrays = {}
    open_flags = os.O_RDWR if read_write else os.O_RDONLY
    open_flags |= getattr(os, "O_BINARY", 0)

    def do_mmap(fd):
        if os.name == "nt":
            return mmap.mmap(
                fd, 0, access=mmap.ACCESS_WRITE if read_write else mmap.ACCESS_READ
            )
        return mmap.mmap(
            fd,
            0,
            prot=mmap.PROT_READ | mmap.PROT_WRITE if read_write else mmap.PROT_READ,
        )

    for index, (column_name, dtype) in enumerate(schema.items()):
        fd = os.open(path / f"{index}.bin", open_flags)
        if os.fstat(fd).st_size == 0:
            os.close(fd)
            arrays[column_name] = np.empty(0, dtype=dtype)
            continue
        mmap_object = do_mmap(fd)
        os.close(fd)
        arrays[column_name] = np.frombuffer(mmap_object, dtype=dtype)

    return arrays


def open_new_dataset_dct(
    path: PathType, schema: SchemaLike, nrows: int
) -> dict[str, np.ndarray]:
    """Create and open a fixed-size dataset as writable NumPy arrays."""

    DatasetWriter.preallocate_dataset(path, schema, nrows=nrows)
    return open_dataset_dct(path, read_write=True)


def open_dataset_simple_namespace(path: PathType, **kwargs) -> SimpleNamespace:
    return SimpleNamespace(**open_dataset_dct(path, **kwargs))


def open_dataset(path: PathType, **kwargs):
    """Open as a pandas DataFrame using the optional pandas dependency."""

    try:
        import pandas as pd
    except ImportError as error:
        raise ImportError(
            "open_dataset requires pandas; install mmappet[pandas]"
        ) from error
    return pd.DataFrame(open_dataset_dct(path, **kwargs), copy=False)


def np_to_pa(array: np.ndarray):
    """Convert a NumPy array to a zero-copy PyArrow array."""

    import pyarrow as pa  # pyright: ignore[reportMissingImports]

    pyarrow_buffer = pa.py_buffer(array)
    dtype = pa.from_numpy_dtype(array.dtype)
    return pa.Array.from_buffers(
        type=dtype,
        length=len(array),
        buffers=[None, pyarrow_buffer],
        null_count=0,
    )


def open_dataset_pa(path: PathType, **kwargs):
    """Return a dataset as a dictionary of mmap-backed PyArrow arrays."""

    return {
        key: np_to_pa(value) for key, value in open_dataset_dct(path, **kwargs).items()
    }


def open_dataset_pl(path: PathType, **kwargs):
    """Return a dataset as an mmap-backed Polars DataFrame."""

    import polars as pl  # pyright: ignore[reportMissingImports]
    import pyarrow as pa  # pyright: ignore[reportMissingImports]

    return pl.from_arrow(pa.table(open_dataset_pa(path, **kwargs)))
