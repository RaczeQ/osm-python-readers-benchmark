"""Code for monitoring a function."""

import multiprocessing
import time
from collections.abc import Callable
from typing import Any, Optional

ctx: multiprocessing.context.SpawnContext = multiprocessing.get_context("spawn")

TIMEOUT_SECONDS = 10

def run_monitoring_in_background(function: Callable, write_conn: Any) -> None:
    start_time = time.perf_counter()
    function()
    elapsed_time = time.perf_counter() - start_time

    write_conn.send(elapsed_time)


def monitor_function(function: Callable) -> Optional[float]:
    """Runs a function and monitors time spent + network usage."""
    read_conn, write_conn = multiprocessing.Pipe(duplex=False)

    p = multiprocessing.Process(
        target=run_monitoring_in_background, args=(function, write_conn)
    )
    p.start()
    
    start_time = time.time()
    while p.is_alive():
        if (time.time() - start_time) > TIMEOUT_SECONDS:
            return None
        
        time.sleep(0.1)
        
    elapsed_time = read_conn.recv()

    return elapsed_time
