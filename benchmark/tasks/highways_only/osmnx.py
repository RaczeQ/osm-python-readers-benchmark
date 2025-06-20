from pathlib import Path
from typing import Any

from osmnx._overpass import _download_overpass_features
from osmnx.features import features_from_polygon
from shapely.geometry.base import BaseGeometry
import osmnx.settings


def osmnx_download_highways(
    directory: Path, geometry: BaseGeometry, **kwargs: Any
) -> int:
    """Download cache and return total size."""
    osmnx.settings.cache_folder = directory
    list(_download_overpass_features(geometry, {"highway": True}))

    return sum(p.stat().st_size for p in osmnx.settings.cache_folder.glob("**/*"))


def osmnx_get_highways(geometry: BaseGeometry, **kwargs: Any) -> None:
    features_from_polygon(polygon=geometry, tags={"highway": True})
