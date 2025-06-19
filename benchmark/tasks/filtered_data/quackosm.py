from pathlib import Path
from typing import Any

from quackosm import convert_pbf_to_parquet


def quackosm_get_filtered_data(pbf_file: Path, tags_filter: dict[str, Any], **kwargs: Any) -> None:
    convert_pbf_to_parquet(
        pbf_file,
        tags_filter=tags_filter,
        sort_result=False,
        verbosity_mode="silent",
        ignore_cache=True,
    )
