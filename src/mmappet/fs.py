import os
import subprocess
import sys
from os import PathLike
from pathlib import Path


def hardlink_or_copy(src: PathLike, dst: PathLike) -> bool:
    """Hard-link `dst` to `src`; on failure (e.g. cross-device), fall back to
    a reflink/clone copy (`cp -a --reflink=auto`, `-c` on macOS).

    Returns True if the fallback copy path was taken.
    """
    try:
        os.link(src, dst)
        return False
    except OSError:
        command = ["cp", "-a"]
        if sys.platform == "darwin":
            command.append("-c")
        else:
            command.append("--reflink=auto")
            command.append("--")
        command.extend((str(src), str(dst)))
        subprocess.run(command, check=True)
        return True


def copy_dataset(src: PathLike, dst: PathLike) -> bool:
    """Recreate every file under dataset directory `src` (schema.txt, column
    `N.bin` files, and any nested dataset dirs such as `dataindex.mmappet`,
    walked recursively) under `dst`, via `hardlink_or_copy`.

    Returns True if any file fell back to copy instead of hard-linking.
    """
    src = Path(src)
    dst = Path(dst)
    fell_back = False
    for root, _dirs, files in os.walk(src):
        root = Path(root)
        rel_root = root.relative_to(src)
        for name in files:
            file_dst = dst / rel_root / name
            file_dst.parent.mkdir(parents=True, exist_ok=True)
            if hardlink_or_copy(root / name, file_dst):
                fell_back = True
    return fell_back
