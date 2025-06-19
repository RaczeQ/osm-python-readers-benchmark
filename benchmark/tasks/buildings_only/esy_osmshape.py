from pathlib import Path
from typing import Any
import esy.osm.shape


def esyosmshape_get_buildings(pbf_file: Path, **kwargs: Any) -> None:
    shape = esy.osm.shape.Shape(str(pbf_file))
    _ = [shape for shape, _, _ in shape(lambda e: e.tags.get("building"))]
