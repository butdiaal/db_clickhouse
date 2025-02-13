import time
from typing import Callable, Tuple
from memory_profiler import memory_usage
from search_1 import main as method1
from search_2 import main as method2
from search_3 import main as method3


def measure_memory_and_time(method: Callable[[], None]) -> Tuple[float, float]:
    """
    Function to measure the execution time and peak memory usage of a given method.

    :param method: The method whose time and memory usage are to be measured.
    :return: A tuple containing the execution time in seconds and the peak memory usage in megabytes.
    """
    start_time = time.time()

    mem_usage = memory_usage((method,), max_usage=True)

    end_time = time.time()

    return end_time - start_time, mem_usage


if __name__ == "__main__":

    time1, mem1 = measure_memory_and_time(method1)
    print(
        f"Execution time for method 1: {time1:.4f} seconds, Peak memory usage: {mem1:.2f} MiB"
    )

    time2, mem2 = measure_memory_and_time(method2)
    print(
        f"Execution time for method 2: {time2:.4f} seconds, Peak memory usage: {mem2:.2f} MiB"
    )

    time3, mem3 = measure_memory_and_time(method3)
    print(
        f"Execution time for method 3: {time3:.4f} seconds, Peak memory usage: {mem3:.2f} MiB"
    )
