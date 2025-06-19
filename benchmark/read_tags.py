from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd


def get_all_tags_filter(pbf_file_path: Path) -> dict[str, Literal[True]]:
    saved_tags_file = pbf_file_path.with_suffix(".tags.parquet")
    if not saved_tags_file.exists():
        duckdb.install_extension("spatial")
        duckdb.load_extension("spatial")
        duckdb.sql(
            f"SELECT DISTINCT UNNEST(map_keys(tags)) as keys FROM ST_ReadOSM('{pbf_file_path}') ORDER BY 1"
        ).to_parquet(str(saved_tags_file))

    tags_filter = {key: True for key in pd.read_parquet(saved_tags_file)["keys"]}
    return tags_filter
