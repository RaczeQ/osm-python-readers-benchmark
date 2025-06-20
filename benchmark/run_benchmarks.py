from collections.abc import Callable
import shutil
from dataclasses import asdict, dataclass
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
from benchmark.tasks.highways_only.esy_osmshape import esyosmshape_get_highways
from benchmark.tasks.highways_only.osmium import osmium_get_highways
from benchmark.tasks.highways_only.osmnx import (
    osmnx_get_highways,
    osmnx_download_highways,
)
from benchmark.tasks.highways_only.pyrosm import pyrosm_get_highways
from benchmark.tasks.highways_only.quackosm import quackosm_get_highways
from benchmark.tasks.filtered_data.esy_osmshape import esyosmshape_get_filtered_data
from benchmark.tasks.filtered_data.osmium import osmium_get_filtered_data
from benchmark.tasks.filtered_data.osmnx import (
    osmnx_get_filtered_data,
    osmnx_download_filtered_data,
)
from benchmark.tasks.filtered_data.pyrosm import pyrosm_get_filtered_data
from benchmark.tasks.filtered_data.quackosm import quackosm_get_filtered_data

OSMNX_CACHE_DIR = Path(__file__).parent.parent / "cache"

@dataclass
class OsmBenchmarkingExtract:
    extract: OpenStreetMapExtract
    number_of_repeats: int
    allow_osmnx: bool

def get_osm_extracts_for_benchmarks() -> list[OsmBenchmarkingExtract]:
    return [
        OsmBenchmarkingExtract(extract=get_extract_by_query(query), number_of_repeats=repeats, allow_osmnx=allow_osmnx)
        for query, repeats, allow_osmnx in (
            ("osmfr_europe_monaco", 1, True),
            ("geofabrik_north-america_us_us_district-of-columbia", 1, True),
            ("osmfr_europe_france_ile_de_france_paris", 1, True),
            ("osmfr_europe_united_kingdom_england_greater_london", 1, True),
            ("geofabrik_europe_portugal", 1, True),
            ("geofabrik_europe_poland", 1, False),
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
            progressbar=True
        )
    )

    return pbf_file_path.stat().st_size


def _run_benchmark(
    benchmark_name: str,
    functions: dict[str, tuple[Callable, Callable[..., int]]],
    tags_filter: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    results = []
    for osm_benchmark_config in get_osm_extracts_for_benchmarks():
        osm_extract = osm_benchmark_config.extract
        repeats = osm_benchmark_config.number_of_repeats
        allow_osmnx = osm_benchmark_config.allow_osmnx
        directory = Path(__file__).parent.parent / "files"
        file_name = f"{osm_extract.file_name}.osm.pbf"
        pbf_file_path = directory / file_name
        geometry = osm_extract.geometry

        osmnx_cache_dir = OSMNX_CACHE_DIR / benchmark_name / osm_extract.file_name

        for function_name, (parsing_function, download_function) in functions.items():
            if function_name == "osmnx" and not allow_osmnx:
                continue
            for idx in trange(repeats, desc=f"[{benchmark_name}] [{osm_extract.name}] {function_name}"):
                download_size = download_function(
                    url=osm_extract.url,
                    directory=directory if function_name != "osmnx" else osmnx_cache_dir,
                    file_name=file_name,
                    geometry=geometry,
                    tags_filter=tags_filter,
                )
                tags_filter = tags_filter or get_all_tags_filter(pbf_file_path)
                # run_time = monitor_function(
                #     partial(
                #         parsing_function,
                #         pbf_file=pbf_file_path,
                #         geometry=geometry,
                #         tags_filter=tags_filter,
                #     )
                # )
                run_time = 0

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
        "esy_osmshape": (esyosmshape_get_buildings, _download_pbf_file),
        "osmnx": (osmnx_get_buildings, osmnx_download_buildings),
        "osmium": (osmium_get_buildings, _download_pbf_file),
        "pyrosm": (pyrosm_get_buildings, _download_pbf_file),
        "quackosm": (quackosm_get_buildings, _download_pbf_file),
    }
    return _run_benchmark("buildings", functions, tags_filter=None)

def run_highways_benchmark() -> pd.DataFrame:
    functions = {
        "esy_osmshape": (esyosmshape_get_highways, _download_pbf_file),
        "osmnx": (osmnx_get_highways, osmnx_download_highways),
        "osmium": (osmium_get_highways, _download_pbf_file),
        "pyrosm": (pyrosm_get_highways, _download_pbf_file),
        "quackosm": (quackosm_get_highways, _download_pbf_file),
    }
    return _run_benchmark("highways", functions, tags_filter={"highway": True})

def run_geofabrik_layers_benchmark() -> pd.DataFrame:
    functions = {
        "esy_osmshape": (esyosmshape_get_filtered_data, _download_pbf_file),
        # "osmnx": (osmnx_get_filtered_data, osmnx_download_filtered_data),
        "osmium": (osmium_get_filtered_data, _download_pbf_file),
        "pyrosm": (pyrosm_get_filtered_data, _download_pbf_file),
        "quackosm": (quackosm_get_filtered_data, _download_pbf_file),
    }
    return _run_benchmark("geofabrik layers", functions, tags_filter=GEOFABRIK_FILTER)


def run_all_data_benchmark() -> pd.DataFrame:
    functions = {
        "esy_osmshape": (esyosmshape_get_all_data, _download_pbf_file),
        # "osmnx": (osmnx_get_all_data, _download_pbf_file),
        "osmium": (osmium_get_all_data, _download_pbf_file),
        "pydriosm": (pydriosm_get_all_data, _download_pbf_file),
        "pyrosm": (pyrosm_get_all_data, _download_pbf_file),
        "quackosm": (quackosm_get_all_data, _download_pbf_file),
    }
    return _run_benchmark("all data", functions, tags_filter=None)


if __name__ == "__main__":
    # m = get_extract_by_query('osmfr_europe_monaco')
    # # idx = _get_combined_index()
    # print(m)
    results = run_buildings_benchmark()
    results.to_csv("buildings_benchmark.csv")
    # results = run_all_data_benchmark()
    # results.to_csv("all_data_benchmark.csv")
    print(results)
