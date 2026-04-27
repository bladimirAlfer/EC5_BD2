import os
import csv

from heap_file import export_to_heap
from external_sort import external_sort
from external_hashing import external_hash_group_by


PAGE_SIZE = 4096

EMPLOYEE_FORMAT = "i10s20s20s1s10s"
DEPT_EMP_FORMAT = "i4s10s10s"

EMPLOYEE_CSV = "data/employee.csv"
DEPT_EMP_CSV = "data/department_employee.csv"

EMPLOYEE_BIN = "data/employee.bin"
DEPT_EMP_BIN = "data/department_employee.bin"

SORTED_EMPLOYEE_BIN = "data/employee_sorted.bin"


def build_heap_files():
    print("Generando heap files...")

    export_to_heap(
        EMPLOYEE_CSV,
        EMPLOYEE_BIN,
        EMPLOYEE_FORMAT,
        PAGE_SIZE
    )

    export_to_heap(
        DEPT_EMP_CSV,
        DEPT_EMP_BIN,
        DEPT_EMP_FORMAT,
        PAGE_SIZE
    )

    print("Heap files generados correctamente.")


def run_benchmark():
    buffer_sizes = [
        64 * 1024,
        128 * 1024,
        256 * 1024
    ]

    print("\n===== EXTERNAL SORT - employee ORDER BY hire_date =====")

    for buffer_size in buffer_sizes:
        metrics = external_sort(
            EMPLOYEE_BIN,
            SORTED_EMPLOYEE_BIN,
            PAGE_SIZE,
            buffer_size,
            "hire_date"
        )

        total_io = metrics["pages_read"] + metrics["pages_written"]

        print(f"\nBUFFER_SIZE: {buffer_size // 1024} KB")
        print(f"M páginas RAM: {buffer_size // PAGE_SIZE}")
        print(f"Runs generados: {metrics['runs_generated']}")
        print(f"Tiempo Fase 1: {metrics['time_phase1_sec']:.6f} s")
        print(f"Tiempo Fase 2: {metrics['time_phase2_sec']:.6f} s")
        print(f"Tiempo Total: {metrics['time_total_sec']:.6f} s")
        print(f"I/O Total páginas: {total_io}")

    print("\n===== EXTERNAL HASHING - department_employee GROUP BY from_date =====")

    for buffer_size in buffer_sizes:
        metrics = external_hash_group_by(
            DEPT_EMP_BIN,
            PAGE_SIZE,
            buffer_size,
            "from_date"
        )

        total_io = metrics["pages_read"] + metrics["pages_written"]

        print(f"\nBUFFER_SIZE: {buffer_size // 1024} KB")
        print(f"M páginas RAM: {buffer_size // PAGE_SIZE}")
        print(f"Particiones creadas: {metrics['partitions_created']}")
        print(f"Grupos encontrados: {len(metrics['result'])}")
        print(f"Tiempo Fase 1: {metrics['time_phase1_sec']:.6f} s")
        print(f"Tiempo Fase 2: {metrics['time_phase2_sec']:.6f} s")
        print(f"Tiempo Total: {metrics['time_total_sec']:.6f} s")
        print(f"I/O Total páginas: {total_io}")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    build_heap_files()
    run_benchmark()