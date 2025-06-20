from pathlib import Path
from typing import Any
from pydriosm.reader import PBFReadParse


def pydriosm_get_all_data(pbf_file: Path, **kwargs: Any) -> None:
    PBFReadParse.read_pbf(str(pbf_file), parse_geometry=True)
