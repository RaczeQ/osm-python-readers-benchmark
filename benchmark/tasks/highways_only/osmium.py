from pathlib import Path
from typing import Any
from benchmark.osmium_handler import PbfFileHandler


def osmium_get_highways(pbf_file: Path, **kwargs: Any) -> None:
    pbf_handler = PbfFileHandler(tags={"highway": True})
    pbf_handler.get_features_gdf(file_paths=[pbf_file])
