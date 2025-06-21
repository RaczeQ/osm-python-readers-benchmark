"""Code for monitoring a function."""

from dataclasses import dataclass
import multiprocessing
import time
from collections.abc import Callable
from typing import Any, Optional
from quackosm._parquet_multiprocessing import WorkerProcess

import psutil

ctx: multiprocessing.context.SpawnContext = multiprocessing.get_context("spawn")


@dataclass
class MonitoringResult:
    """Class to hold the monitoring results."""

    elapsed_time: float = 0
    cpu_avg: float = 0
    cpu_max: float = 0
    memory_avg: float = 0
    memory_max: float = 0
    timeout: bool = False
    exception_name: Optional[str] = None
    exception_message: Optional[str] = None


def run_monitoring_in_background(function: Callable, write_conn: Any) -> None:
    start_time = time.perf_counter()
    function()
    elapsed_time = time.perf_counter() - start_time

    write_conn.send(elapsed_time)


def get_cpu_cores(p: psutil.Process) -> tuple[float, float]:
    """Get CPU usage."""
    while True:
        try:
            return (
                p.cpu_percent(interval=None)
                + sum(
                    _pc.cpu_percent(interval=None) for _pc in p.children(recursive=True)
                )
            ) / 100
        except:
            pass


def get_memory_bytes_size(p: psutil.Process) -> tuple[float, float]:
    """Get RAM usage."""
    while True:
        try:
            return p.memory_full_info().rss
        except:
            pass


def monitor_function(function: Callable, possible_timeout_seconds: int) -> MonitoringResult:
    """Runs a function and monitors time spent + network usage."""
    read_conn, write_conn = multiprocessing.Pipe(duplex=False)

    p = WorkerProcess(target=run_monitoring_in_background, args=(function, write_conn))
    p.start()

    psutil_process = psutil.Process(p.pid)

    number_of_readings = 0
    current_cpu = get_cpu_cores(psutil_process)
    current_memory = get_memory_bytes_size(psutil_process)
    cpu_avg = current_cpu
    cpu_max = current_cpu
    memory_avg = current_memory
    memory_max = current_memory

    start_time = time.time()
    while p.is_alive():
        if (time.time() - start_time) > possible_timeout_seconds:
            print(f"Monitoring timed out after {possible_timeout_seconds} seconds.")
            p.terminate()
            return MonitoringResult(timeout=True)

        number_of_readings += 1

        current_cpu = get_cpu_cores(psutil_process)
        current_memory = get_memory_bytes_size(psutil_process)
        cpu_avg = (cpu_avg * number_of_readings + current_cpu) / (
            number_of_readings + 1
        )
        cpu_max = max(cpu_max, current_cpu)
        memory_avg = (memory_avg * number_of_readings + current_memory) / (
            number_of_readings + 1
        )
        memory_max = max(memory_max, current_memory)

        time.sleep(0.1)

    if p.exception:
        exc, _ = p.exception
        print(f"Monitoring raised an exception: {exc}")
        return MonitoringResult(
            exception_name=type(exc).__name__,
            exception_message=str(exc),
        )
    

    elapsed_time = read_conn.recv()

    return MonitoringResult(
        elapsed_time=elapsed_time,
        cpu_avg=cpu_avg,
        cpu_max=cpu_max,
        memory_avg=memory_avg,
        memory_max=memory_max,
    )
