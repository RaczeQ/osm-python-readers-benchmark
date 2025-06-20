from benchmark.run_benchmarks import run_all_data_benchmark, run_buildings_benchmark, run_highways_benchmark

if __name__ == "__main__":
    run_buildings_benchmark()
    run_highways_benchmark()
    run_all_data_benchmark()
