from pathlib import Path
from typing import Any

import esy.osm.shape


def esyosmshape_get_filtered_data(
    pbf_file: Path, tags_filter: dict[str, Any], **kwargs: Any
) -> None:
    shape = esy.osm.shape.Shape(str(pbf_file))
    for shape, _, _ in shape(
        lambda e: any(
            e.tags.get(tag_key) in tag_values
            for tag_key, tag_values in tags_filter.items()
        )
    ):
        pass
