from benchmark.run_benchmarks import run_all_data_benchmark, run_buildings_benchmark, run_geofabrik_layers_benchmark

if __name__ == "__main__":
    results = run_buildings_benchmark()
    results.to_csv("buildings_benchmark.csv")
    # results = run_geofabrik_layers_benchmark()
    # results.to_csv("geofabrik_benchmark.csv")
    # results = run_all_data_benchmark()
    # results.to_csv("all_data_benchmark.csv")
    print(results)
