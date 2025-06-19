from pathlib import Path
from typing import Any
from quackosm import convert_pbf_to_parquet


def quackosm_get_all_data(pbf_file: Path, **kwargs: Any) -> None:
    convert_pbf_to_parquet(
        pbf_file,
        sort_result=False,
        verbosity_mode="silent",
        ignore_cache=True,
    )
