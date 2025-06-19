from pathlib import Path
from typing import Any

from pyrosm import OSM


def pyrosm_get_filtered_data(
    pbf_file: Path, tags_filter: dict[str, Any], **kwargs: Any
) -> None:
    osm = OSM(str(pbf_file))
    osm.get_data_by_custom_criteria(tags_filter)
