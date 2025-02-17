import time
from typing import Callable, Tuple
from memory_profiler import memory_usage
from search_Euclidean_Distance import main as method1
from search_Faiss_IndexFlatL2 import main as method2
from search_ClickHouse_L2Distance import main as method3
from search_Faiss_IndexIVFFlat import main as method4
from search_ClickHouse_cosineDistance import main as method5


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
    # Запрашиваем количество запусков
    n_runs = int(input("Введите количество запусков: "))

    times_method1 = []
    mems_method1 = []

    times_method2 = []
    mems_method2 = []

    times_method3 = []
    mems_method3 = []

    times_method4 = []
    mems_method4 = []

    times_method5 = []
    mems_method5 = []

    for _ in range(n_runs):

        time1, mem1 = measure_memory_and_time(method1)
        times_method1.append(time1)
        mems_method1.append(mem1)

        time2, mem2 = measure_memory_and_time(method2)
        times_method2.append(time2)
        mems_method2.append(mem2)

        time3, mem3 = measure_memory_and_time(method3)
        times_method3.append(time3)
        mems_method3.append(mem3)

        time4, mem4 = measure_memory_and_time(method4)
        times_method4.append(time4)
        mems_method4.append(mem4)

        time5, mem5 = measure_memory_and_time(method5)
        times_method5.append(time5)
        mems_method5.append(mem5)

    avg_time1 = sum(times_method1) / len(times_method1)
    avg_mem1 = sum(mems_method1) / len(mems_method1)

    avg_time2 = sum(times_method2) / len(times_method2)
    avg_mem2 = sum(mems_method2) / len(mems_method2)

    avg_time3 = sum(times_method3) / len(times_method3)
    avg_mem3 = sum(mems_method3) / len(mems_method3)

    avg_time4 = sum(times_method4) / len(times_method4)
    avg_mem4 = sum(mems_method4) / len(mems_method4)

    avg_time5 = sum(times_method5) / len(times_method5)
    avg_mem5 = sum(mems_method5) / len(mems_method5)

    # Выводим результаты
    print(f"Среднее время выполнения для метода 1: {avg_time1:.4f} секунд")
    print(f"Средняя пиковая нагрузка памяти для метода 1: {avg_mem1:.2f} MiB\n")

    print(f"Среднее время выполнения для метода 2: {avg_time2:.4f} секунд")
    print(f"Средняя пиковая нагрузка памяти для метода 2: {avg_mem2:.2f} MiB\n")

    print(f"Среднее время выполнения для метода 3: {avg_time3:.4f} секунд")
    print(f"Средняя пиковая нагрузка памяти для метода 3: {avg_mem3:.2f} MiB\n")

    print(f"Среднее время выполнения для метода 4: {avg_time4:.4f} секунд")
    print(f"Средняя пиковая нагрузка памяти для метода 4: {avg_mem4:.2f} MiB\n")

    print(f"Среднее время выполнения для метода 5: {avg_time5:.4f} секунд")
    print(f"Средняя пиковая нагрузка памяти для метода 5: {avg_mem5:.2f} MiB\n")
