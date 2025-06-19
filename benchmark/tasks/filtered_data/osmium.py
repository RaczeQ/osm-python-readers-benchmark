from pathlib import Path
from typing import Any
from benchmark.osmium_handler import PbfFileHandler


def osmium_get_filtered_data(pbf_file: Path, tags_filter: dict[str, Any], **kwargs: Any) -> None:
    pbf_handler = PbfFileHandler(tags=tags_filter)
    pbf_handler.get_features_gdf(file_paths=[pbf_file])
