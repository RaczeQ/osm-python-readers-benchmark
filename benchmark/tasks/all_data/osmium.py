from pathlib import Path
from typing import Any
from benchmark.osmium_handler import PbfFileHandler


def osmium_get_all_data(pbf_file: Path, **kwargs: Any) -> None:
    pbf_handler = PbfFileHandler(tags=None)
    pbf_handler.iterate_osm_features(file_paths=[pbf_file])
