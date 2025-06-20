from pathlib import Path
from typing import Any
from pyrosm import OSM


def pyrosm_get_highways(pbf_file: Path, **kwargs: Any) -> None:
    osm = OSM(str(pbf_file))
    osm.get_data_by_custom_criteria({"highway": True})
