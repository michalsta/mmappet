import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from mmappet import (
    DatasetWriter,
    get_schema,
    open_dataset,
    open_dataset_dct,
    open_new_dataset_dct,
    schema_to_str,
    str_to_schema,
)


def test_numpy_roundtrip_and_append(tmp_path):
    path = tmp_path / "test.mmappet"
    first = {
        "a": np.arange(100, dtype=np.uint32),
        "b": np.linspace(0, 1, 100, dtype=np.float64),
        "c": np.arange(100, dtype=np.int64),
    }
    second = {
        "a": np.arange(100, 200, dtype=np.uint32),
        "b": np.linspace(1, 2, 100, dtype=np.float64),
        "c": np.arange(100, 200, dtype=np.int64),
    }

    with DatasetWriter(path, overwrite_dir=True) as writer:
        writer.append(**first)
    with DatasetWriter(path, append_ok=True) as writer:
        assert len(writer) == 100
        writer.append(**second)
        assert len(writer) == 200

    result = open_dataset_dct(path)
    for name, first_values in first.items():
        expected = np.concatenate([first_values, second[name]])
        np.testing.assert_array_equal(result[name], expected)


def test_append_ok_can_create_a_new_dataset(tmp_path):
    path = tmp_path / "new.mmappet"

    with DatasetWriter(path, append_ok=True) as writer:
        writer.append(values=np.asarray([1, 2, 3], dtype=np.int64))

    np.testing.assert_array_equal(
        open_dataset_dct(path)["values"],
        np.asarray([1, 2, 3], dtype=np.int64),
    )


def test_schema_is_an_ordered_numpy_dtype_mapping():
    schema = get_schema(a=np.uint32, b=np.float64)

    assert schema == {"a": np.dtype(np.uint32), "b": np.dtype(np.float64)}
    assert str_to_schema(schema_to_str(schema)) == schema


def test_zero_row_dataset_roundtrip(tmp_path):
    path = tmp_path / "empty.mmappet"
    schema = get_schema(a=np.uint32, b=np.float64)

    data = open_new_dataset_dct(path, schema=schema, nrows=0)
    assert list(data) == ["a", "b"]
    assert data["a"].dtype == np.dtype(np.uint32)
    assert data["b"].dtype == np.dtype(np.float64)
    assert len(data["a"]) == 0
    assert len(data["b"]) == 0

    reopened = open_dataset_dct(path)
    assert reopened["a"].dtype == np.dtype(np.uint32)
    assert reopened["b"].dtype == np.dtype(np.float64)


def test_append_rejects_unequal_or_misordered_columns(tmp_path):
    path = tmp_path / "invalid.mmappet"
    with DatasetWriter.new(path, a=np.int64, b=np.float32) as writer:
        with pytest.raises(ValueError, match="unequal"):
            writer.append(
                a=np.array([1, 2], dtype=np.int64),
                b=np.array([1], dtype=np.float32),
            )
        with pytest.raises(ValueError, match="Columns must"):
            writer.append(
                b=np.array([1], dtype=np.float32),
                a=np.array([1], dtype=np.int64),
            )


def test_pandas_adapters_are_optional_and_roundtrip(tmp_path):
    pd = pytest.importorskip("pandas")
    path = tmp_path / "pandas.mmappet"
    expected = pd.DataFrame(
        {
            "a": np.arange(3, dtype=np.uint32),
            "b": np.arange(3, dtype=np.float64),
        }
    )

    with DatasetWriter(path) as writer:
        writer.append_df(expected)

    pd.testing.assert_frame_equal(open_dataset(path), expected)


def test_importing_core_does_not_import_pandas():
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.fspath((Path(__file__).parents[2] / "src").resolve())
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, mmappet; assert 'pandas' not in sys.modules",
        ],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
