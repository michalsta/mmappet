# mmappet

`mmappet` is appendable, mmap-backed NumPy column storage. A dataset is a
directory containing one binary file per column plus an ordered dtype schema.
All columns have the same row count.

## NumPy API

```python
import numpy as np
from mmappet import DatasetWriter, open_dataset_dct

with DatasetWriter.new(
    "example.mmappet",
    scan=np.uint32,
    intensity=np.float32,
) as writer:
    writer.append(
        scan=np.asarray([1, 2], dtype=np.uint32),
        intensity=np.asarray([0.2, 0.8], dtype=np.float32),
    )

arrays = open_dataset_dct("example.mmappet")
arrays["scan"]       # mmap-backed NumPy array
arrays["intensity"]  # mmap-backed NumPy array
```

Opening an existing writer reconstructs its current length from the column
file sizes:

```python
with DatasetWriter("example.mmappet", append_ok=True) as writer:
    start = len(writer)
    writer.append(
        scan=np.asarray([3], dtype=np.uint32),
        intensity=np.asarray([0.5], dtype=np.float32),
    )
```

The core package depends only on NumPy and importing it does not import pandas.

## Optional pandas adapter

Install the extra when DataFrame conversion is needed:

```bash
pip install 'mmappet[pandas]'
```

Then use `DatasetWriter.append_df(dataframe)` or `open_dataset(path)`. These are
adapters around the same NumPy-backed storage; they are not part of the core
schema representation.
