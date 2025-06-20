import os, mmap
import pandas as pd
import numpy as np
import numpy.typing as npt
from types import SimpleNamespace
from pathlib import Path

def schema_to_str(schema: pd.DataFrame):
    ret = []
    for colname in schema:
        ret.append(f"{schema[colname].values.dtype} {colname}")
    return "\n".join(ret)


def str_to_schema(s: str):
    ret = {}
    for line in s.splitlines():
        dtype_str, colname = line.split(maxsplit=1)
        ret[colname] = np.empty(dtype=np.dtype(dtype_str), shape=0)
    return pd.DataFrame(ret)


def write_schema(schema: pd.DataFrame, path: Path | str):
    with open(Path(path) / "schema.txt", "wt") as f:
        f.write(schema_to_str(schema))


def _read_schema_tbl(path: Path):
    with open(path / "schema.txt", "rt") as f:
        return str_to_schema(f.read())


def get_schema(**kwargs: np.dtype):
    """Turn mapping name -> np.dtype into what mmapped_df expects: a schema."""
    return pd.DataFrame({c: pd.Series(dtype=dt) for c, dt in kwargs.items()})

class DatasetWriter:
    def __init__(self, path: Path | str, append_ok: bool = False, overwrite_dir=False):
        if append_ok and overwrite_dir:
            raise ValueError("Cannot set both append_ok and overwrite_dir to True.")
        if isinstance(path, str):
            path = Path(path)
        if not isinstance(path, Path):
            raise TypeError("path must be a Path or a string representing a path.")
        self.path = path
        self.files = None
        self.colnames = None
        self.dtypes = None
        self.schema = None
        if self.path.exists():
            if append_ok:
                tbl = _read_schema_tbl(self.path)
                self._reset_schema(tbl)
            else:
                if overwrite_dir:
                    import shutil
                    shutil.rmtree(self.path, ignore_errors=True)
        else:
            self.path.mkdir(parents=True, exist_ok=overwrite_dir)

    @staticmethod
    def preallocate_dataset(
        path: Path | str,
        dataframe_scheme: pd.DataFrame,
        nrows: int,
        overwrite_dir=False,
    ) -> None:
        with DatasetWriter(path=path, overwrite_dir=overwrite_dir) as DW:
            DW._reset_schema(like=dataframe_scheme)
            for file, dt in zip(DW.files, DW.dtypes):
                file.truncate(nrows * dt.itemsize)

    def _reset_schema(self, like: pd.DataFrame):
        self.close()
        self.files = []
        self.colnames = []
        self.dtypes = []
        schema_str = schema_to_str(like)
        self.schema = str_to_schema(schema_str)
        lengths = []
        for idx, colname in enumerate(self.schema):
            file_path = self.path / f"{idx}.bin"
            self.files.append(open(file_path, "ab", buffering=10240))
            self.colnames.append(colname)
            self.dtypes.append(self.schema[colname].values.dtype)
            lengths.append(file_path.stat().st_size / self.dtypes[-1].itemsize)

        if not (lengths == [] or all(l == lengths[0] for l in lengths)):
            raise RuntimeError(
                f"Corrupted dataset: columns of unequal lengths: {self.path}"
            )

        self.length = 0 if lengths == [] else int(lengths[0])

        with open(self.path / "schema.txt", "wt") as f:
            f.write(schema_str)

    @classmethod
    def new(
        cls,
        path: Path | str,
        append_ok: bool = False,
        overwrite_dir=False,
        **kwargs,
    ):
        assert (
            len(kwargs) > 0
        ), "Using `.new` requires you to specify the types of columns in advance and pass them in as `column=numpy.type` fashion, e.g. `scan=np.uint32`."
        res = cls(path, append_ok, overwrite_dir)
        res._reset_schema(like=get_scheme(**kwargs))
        return res

    def close(self):
        if self.files is not None:
            for file in self.files:
                file.close()
        self.files = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __len__(self):
        return self.length

    def append_df(self, df):
        if len(df) == 0:
            return
        if self.files is None:
            self._reset_schema(like=df)
        self.length += len(df)
        for idx, colname in enumerate(df):
            column = df[colname]
            assert colname == self.colnames[idx]
            assert (
                column.values.dtype == self.dtypes[idx] or len(df) == 0
            ), f"Types don't match: {column.values.dtype} vs {self.dtypes[idx]}"
            self.files[idx].write(column.values.tobytes())

    def append_column(self, colname: str, column: npt.NDArray | pd.Series):
        if isinstance(column, pd.Series):
            column = column.values
        assert self.length == len(column)
        self.schema[colname] = np.empty_like(column)
        with open(self.path / f"{len(self.files)}.bin", "xb") as f:
            f.write(column.tobytes())
        self._reset_schema(like=self.schema)

    def append_columns(self, **colname_to_values: npt.NDArray | pd.Series):
        for col, vals in colname_to_values.items():
            assert len(vals) == len(
                self
            ), f"Column `{col}` has {len(vals)} elements, and here we store columns with {len(self)} values. Submit values being either an np.array or pd.Series of the same size as the dataset."
        for col, vals in colname_to_values.items():
            self.append_column(colname=col, column=vals)

    def flush(self):
        for file in self.files:
            file.flush()

    def append(self, **kwargs):
        if self.files is None:
            self._reset_schema(pd.DataFrame(kwargs, copy=False))
        length = None
        for file, dtype, colname in zip(self.files, self.dtypes, self.colnames):
            dat = dtype.type(kwargs[colname])
            file.write(dat.tobytes())
            length = len(kwargs[colname])
        self.length += length

    def append_row(self, **kwargs):
        if self.files is None:
            self._reset_schema(pd.DataFrame([kwargs], copy=False))
        for file, dtype, colname in zip(self.files, self.dtypes, self.colnames):
            dat = dtype.type(kwargs[colname])
            file.write(dat.tobytes())
        self.length += 1

    def append_dct(self, D):
        return self.append(**D)

    def append_list(self, L):
        assert len(L) == len(self.files)
        self.length += len(L[0])
        for data, file, dtype in zip(L, self.files, self.dtypes):
            dat = dtype.type(data)
            file.write(dat.tobytes())



def open_dataset_dct(path: Path | str, read_write: bool = False, **kwargs):
    path = Path(path)
    df = _read_schema_tbl(path)
    new_data = {}

    open_flags = os.O_RDWR if read_write else os.O_RDONLY
    try:
        open_flags = open_flags | os.O_BINARY
    except AttributeError:
        # We're not on Windows, thank goodness
        pass

    mmap_flags = mmap.PROT_READ | mmap.PROT_WRITE if read_write else mmap.PROT_READ
    for idx, column_name in enumerate(df):
        col_dtype = df[column_name].values.dtype
        fd = os.open(path / f"{idx}.bin", open_flags)
        mmap_obj = mmap.mmap(fd, 0, prot=mmap_flags)
        new_data[column_name] = np.frombuffer(mmap_obj, dtype=col_dtype)

    return new_data


def open_new_dataset_dct(path: Path | str, scheme: pd.DataFrame | dict, nrows: int):
    """Create a new dataset and return it as dictionary of column names to appropriately sized arrays."""
    DatasetWriter.preallocate_dataset(
        path,
        pd.DataFrame(scheme),
        nrows=nrows,
    )
    return open_dataset_dct(path, read_write=True)


def open_dataset_simple_namespace(path: Path | str, **kwargs) -> SimpleNamespace:
    return SimpleNamespace(**open_dataset_dct(path, **kwargs))


def open_dataset(path: Path | str, **kwargs):
    return pd.DataFrame(open_dataset_dct(path, **kwargs), copy=False)


def np_to_pa(np_arr):
    """Convert Numpy array to Pyarrow one, sharing the same backing buffer"""
    pyarrow_buf = pa.py_buffer(np_arr)
    dtype = pa.from_numpy_dtype(np_arr.dtype)
    return pa.Array.from_buffers(
        type=dtype, length=len(np_arr), buffers=[None, pyarrow_buf], null_count=0
    )


def open_dataset_pa(path: Path | str, **kwargs):
    """Return dataset as dict of colname -> mmapped pyarrow array"""
    return {key: np_to_pa(val) for key, val in open_dataset_dct(path, **kwargs).items()}


def open_dataset_pl(path: Path | str, **kwargs):
    """Return dataset as mmapped Polars dataframe"""
    import polars as pl

    return pl.from_arrow(pa.table(open_dataset_pa(path, **kwargs)))

