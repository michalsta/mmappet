from mmappet import DatasetWriter, open_dataset
import numpy as np
import pandas as pd
import tempfile
import os
import shutil


def test_roundtrip():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "test.mmappet")
        print(f"Testing roundtrip in temporary directory: {path}")
        schema = pd.DataFrame(
            {
                "a": pd.Series(dtype=np.uint32),
                "b": pd.Series(dtype=np.float64),
                "c": pd.Series(dtype=np.int64),
            }
        )

        # Test open/close pattern
        writer = DatasetWriter(path, overwrite_dir=True)
        data = pd.DataFrame(
            {
                "a": np.arange(100, dtype=np.uint32),
                "b": np.random.rand(100).astype(np.float64),
                "c": np.array(range(100), dtype=np.int64),
            }
        )
        writer.append_df(data)
        writer.close()

        # Read back the data
        df = open_dataset(path, read_write=False)
        pd.testing.assert_frame_equal(df, data)

        # Test appending more data
        more_data = pd.DataFrame(
            {
                "a": np.arange(100, 200, dtype=np.uint32),
                "b": np.random.rand(100).astype(np.float64),
                "c": np.array(range(100, 200), dtype=np.int64),
            }
        )

        # Test context manager pattern
        with DatasetWriter(path, append_ok=True) as writer:
            writer.append_df(more_data)

        df = open_dataset(path, read_write=False)
        combined_data = pd.concat([data, more_data], ignore_index=True)
        pd.testing.assert_frame_equal(df, combined_data)

        #shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    test_roundtrip()
