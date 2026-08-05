# AI.md — mmappet

## What this repo is

Appendable, mmap-backed columnar storage format for DataFrames on disk. Two
independent implementations of the same on-disk format: a header-only C++20 library
(`src/mmappet/cpp/mmappet/mmappet.h`) and a pure-Python package (`src/mmappet/mmappet.py`)
— no bindings between them, they just agree on the file layout below.

```
src/mmappet/
├── mmappet.py                          # Python API: DatasetWriter, open_dataset*
├── dirty_hacks_experimental_never_use.py  # append_empty_columns, crop_dataset — unstable, avoid
├── __main__.py                         # `python -m mmappet` — version/path/include info only
├── scripts/mmappet_show.py             # `mmappet_show <path>` CLI: print dataset as DataFrame
└── cpp/mmappet/mmappet.h               # header-only C++20 reader/writer, used by pmsms2mzml

tests/Python/test_roundtrip.py
examples/cpp/                           # reader/writer/indexed-{reader,writer} example programs
```

Entry point (`pyproject.toml`): `mmappet_show = mmappet.scripts.mmappet_show:main`.
Optional extras: `polars`, `pyarrow` (only needed for `open_dataset_pl`/`open_dataset_pa`).

## On-disk format

A dataset is a directory: `schema.txt` (one `<numpy dtype> <colname>` line per column,
column order = file index order) + `0.bin`, `1.bin`, ... (one flat contiguous binary
array per column, no header). Column `i` in `schema.txt` corresponds to `i.bin`. This is
the format both the C++ header and `mmappet.py` read/write, and what `pandas_ops.io.read_df`
and this pipeline's `MmappetDataset`/`.mmappet`-suffixed node types (see root `CLAUDE.md`'s
"Precursor table format") assume everywhere.

## Python API

- `DatasetWriter(path, append_ok=, overwrite_dir=)` — append-only writer, context manager.
  `append_df`/`append_row`/`append`/`append_column(s)`/`append_list`; schema is inferred
  from the first write (or fixed via `.new(path, **col=dtype)` / `preallocate_dataset`).
  `append_ok` and `overwrite_dir` are mutually exclusive.
- `open_dataset(path, read_write=False)` → pandas DataFrame, zero-copy via
  `np.frombuffer` over an `mmap` of each `N.bin` file. `open_dataset_dct`/
  `open_dataset_simple_namespace`/`open_dataset_pa`/`open_dataset_pl` are the same mmap
  read, wrapped as dict/`SimpleNamespace`/pyarrow/polars instead.
- `dirty_hacks_experimental_never_use.py` (`append_empty_columns`, `crop_dataset`): as the
  filename says — mutates a dataset's column count or row count outside the normal
  append-only writer path. Don't reach for these; treat as unstable/last-resort.

## Conventions to follow when editing this file

Design rationale and history belongs here, not in code docstrings — keep `mmappet.py`/
`mmappet.h` comments about what the code does now, not why it changed.
