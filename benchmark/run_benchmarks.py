from collections.abc import Callable
import shutil
from dataclasses import asdict
from functools import partial
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from pooch import retrieve
from quackosm.osm_extracts import get_extract_by_query
from quackosm.osm_extracts.extract import OpenStreetMapExtract
from tqdm import trange
from benchmark.geofabrik_filter import GEOFABRIK_FILTER

from benchmark.monitoring import monitor_function
from benchmark.read_tags import get_all_tags_filter
from benchmark.tasks.all_data.esy_osmshape import esyosmshape_get_all_data
from benchmark.tasks.all_data.osmium import osmium_get_all_data
from benchmark.tasks.all_data.osmnx import osmnx_get_all_data, osmnx_download_all_data
from benchmark.tasks.all_data.pydriosm import pydriosm_get_all_data
from benchmark.tasks.all_data.pyrosm import pyrosm_get_all_data
from benchmark.tasks.all_data.quackosm import quackosm_get_all_data
from benchmark.tasks.buildings_only.esy_osmshape import esyosmshape_get_buildings
from benchmark.tasks.buildings_only.osmium import osmium_get_buildings
from benchmark.tasks.buildings_only.osmnx import (
    osmnx_get_buildings,
    osmnx_download_buildings,
)
from benchmark.tasks.buildings_only.pyrosm import pyrosm_get_buildings
from benchmark.tasks.buildings_only.quackosm import quackosm_get_buildings
from benchmark.tasks.filtered_data.esy_osmshape import esyosmshape_get_filtered_data
from benchmark.tasks.filtered_data.osmium import osmium_get_filtered_data
from benchmark.tasks.filtered_data.osmnx import (
    osmnx_get_filtered_data,
    osmnx_download_filtered_data,
)
from benchmark.tasks.filtered_data.pyrosm import pyrosm_get_filtered_data
from benchmark.tasks.filtered_data.quackosm import quackosm_get_filtered_data
import osmnx.settings

OSMNX_CACHE_DIR = Path(__file__).parent.parent / "cache"


def get_osm_extracts_for_benchmarks() -> list[tuple[OpenStreetMapExtract, int]]:
    return [
        (get_extract_by_query(query), repeats)
        for query, repeats in (
            ("osmfr_europe_monaco", 2),
            ("geofabrik_north-america_us_us_district-of-columbia", 1),
            # ("osmfr_europe_france_ile_de_france_paris", 1),
            # ("osmfr_europe_united_kingdom_england_greater_london", 1),
            # ("osmfr_europe_monaco", 10),
            # ("geofabrik_europe_estonia", 1),
            # ("geofabrik_europe_poland", 3),
        )
    ]


def _download_pbf_file(url: str, directory: Path, file_name: str, **kwargs: Any) -> int:
    """Download pbf file and return total size."""
    pbf_file_path = Path(
        retrieve(
            url=url,
            known_hash=None,
            fname=file_name,
            path=directory,
        )
    )

    return pbf_file_path.stat().st_size


def _run_benchmark(
    benchmark_name: str,
    functions: dict[str, tuple[Callable, Callable[..., int]]],
    tags_filter: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    results = []
    for osm_extract, repeats in get_osm_extracts_for_benchmarks():
        directory = Path(__file__).parent.parent / "files"
        file_name = f"{osm_extract.file_name}.osm.pbf"
        pbf_file_path = directory / file_name

        geometry = osm_extract.geometry
        tags_filter = tags_filter or get_all_tags_filter(pbf_file_path)

        osmnx_cache_dir = OSMNX_CACHE_DIR / benchmark_name / osm_extract.file_name

        for function_name, (parsing_function, download_function) in functions.items():
            for idx in trange(repeats, desc=f"[{osm_extract.name}] {function_name}"):
                download_size = download_function(
                    url=osm_extract.url,
                    directory=directory if function_name != "osmnx" else osmnx_cache_dir,
                    file_name=file_name,
                    geometry=geometry,
                    tags_filter=tags_filter,
                )
                run_time = monitor_function(
                    partial(
                        parsing_function,
                        pbf_file=pbf_file_path,
                        geometry=geometry,
                        tags_filter=tags_filter,
                    )
                )

                results.append(
                    {
                        "benchmark": benchmark_name,
                        "region": osm_extract.name,
                        "function": function_name,
                        "idx": idx + 1,
                        "download_size_bytes": download_size,
                        "elapsed_time_seconds": run_time,
                    }
                )

    _df = pd.DataFrame(results)

    return _df


def run_buildings_benchmark() -> pd.DataFrame:
    functions = {
        # "esy_osmshape": (esyosmshape_get_buildings, _download_pbf_file),
        "osmnx": (osmnx_get_buildings, osmnx_download_buildings),
        "osmium": (osmium_get_buildings, _download_pbf_file),
        "pyrosm": (pyrosm_get_buildings, _download_pbf_file),
        "quackosm": (quackosm_get_buildings, _download_pbf_file),
    }
    return _run_benchmark("buildings", functions, tags_filter=None)


def run_geofabrik_layers_benchmark() -> pd.DataFrame:
    functions = {
        # "esy_osmshape": (esyosmshape_get_filtered_data, _download_pbf_file),
        # "osmnx": (osmnx_get_filtered_data, osmnx_download_filtered_data),
        # "osmium": (osmium_get_filtered_data, _download_pbf_file),
        "pyrosm": (pyrosm_get_filtered_data, _download_pbf_file),
        "quackosm": (quackosm_get_filtered_data, _download_pbf_file),
    }
    return _run_benchmark("geofabrik layers", functions, tags_filter=GEOFABRIK_FILTER)


def run_all_data_benchmark() -> pd.DataFrame:
    functions = {
        # "esy_osmshape": (esyosmshape_get_all_data, _download_pbf_file),
        # "osmnx": (osmnx_get_all_data, _download_pbf_file),
        # "osmium": (osmium_get_all_data, osmnx_download_all_data),
        # "pydriosm": (pydriosm_get_all_data, _download_pbf_file),
        "pyrosm": (pyrosm_get_all_data, _download_pbf_file),
        "quackosm": (quackosm_get_all_data, _download_pbf_file),
    }
    return _run_benchmark("all_buildings", functions, tags_filter=None)


if __name__ == "__main__":
    # m = get_extract_by_query('osmfr_europe_monaco')
    # # idx = _get_combined_index()
    # print(m)
    results = run_buildings_benchmark()
    results.to_csv("buildings_benchmark.csv")
    # results = run_all_data_benchmark()
    # results.to_csv("all_data_benchmark.csv")
    print(results)
