import sys
from typing import Optional

import typer
from benchmark.run_benchmarks import (
    run_all_data_benchmark,
    run_buildings_benchmark,
    run_highways_benchmark,
)


def run_benchmarks(
    benchmark: Optional[str] = None,
    region: Optional[str] = None,
    function: Optional[str] = None,
) -> None:
    if benchmark == "buildings":
        run_buildings_benchmark(region, function)
    elif benchmark == "highways":
        run_highways_benchmark(region, function)
    elif benchmark == "all_data":
        run_all_data_benchmark(region, function)
    else:
        run_buildings_benchmark(region, function)
        run_highways_benchmark(region, function)
        run_all_data_benchmark(region, function)


if __name__ == "__main__":
    typer.run(run_benchmarks)

