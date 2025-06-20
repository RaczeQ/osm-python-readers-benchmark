from pathlib import Path
from typing import Any
import esy.osm.shape


def esyosmshape_get_all_data(pbf_file: Path, **kwargs: Any) -> None:
    shape = esy.osm.shape.Shape(str(pbf_file))
    for shape, _, _ in shape():
        pass
