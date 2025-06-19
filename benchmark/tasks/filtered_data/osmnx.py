from pathlib import Path
from typing import Any

from osmnx._overpass import _download_overpass_features
from osmnx.features import features_from_polygon
from shapely.geometry.base import BaseGeometry
import osmnx.settings


def osmnx_download_filtered_data(
    directory: Path, geometry: BaseGeometry, tags_filter: dict[str, Any], **kwargs: Any
) -> int:
    """Download cache and return total size."""
    osmnx.settings.cache_folder = directory
    list(_download_overpass_features(geometry, tags_filter))

    return sum(p.stat().st_size for p in osmnx.settings.cache_folder.glob("**/*"))


def osmnx_get_filtered_data(
    geometry: BaseGeometry, tags_filter: dict[str, Any], **kwargs: Any
) -> int:
    features_from_polygon(polygon=geometry, tags=tags_filter)
