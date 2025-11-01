from . import __version__

if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(
        "Appendable, mmappable format for DataFrames on disk"
    )
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"{__version__}",
        help="Show the version of mmappet module",
    )
    parser.add_argument(
        "--path",
        "-p",
        action="store_true",
        help="Print the path to the mmappet module",
    )
    parser.add_argument(
        "--include",
        "-i",
        action="store_true",
        help="Print the path to the mmappet include files",
    )
    args = parser.parse_args()
    if args.path:
        print(Path(__file__).parent.resolve())
    elif args.include:
        include_path = Path(__file__).parent.resolve() / "mmappet_cpp"
        print(include_path)
    else:
        print(
            "mmappet is a module for efficient, appendable, and mmappable DataFrames on disk."
        )
        print(f"Version: {__version__}")
        print("Use --path to get the installation path of the mmappet module.")
        print("Use --version to display the version of the mmappet module.")
        print("Use --scm_version to display the SCM version of the mmappet module.")
        print("For more information, visit the mmappet documentation.")
