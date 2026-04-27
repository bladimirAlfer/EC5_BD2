import os
import time
import heapq
import tempfile

from heap_file import read_page, write_page, count_pages, get_records_per_page


EMPLOYEE_FORMAT = "i10s20s20s1s10s"

EMPLOYEE_COLUMNS = {
    "id": 0,
    "birth_date": 1,
    "first_name": 2,
    "last_name": 3,
    "gender": 4,
    "hire_date": 5
}


def generate_runs(heap_path: str, page_size: int, buffer_size: int, sort_key: str) -> list[str]:
    B = buffer_size // page_size
    total_pages = count_pages(heap_path, page_size)
    key_index = EMPLOYEE_COLUMNS[sort_key]

    run_paths = []
    temp_dir = tempfile.mkdtemp(prefix="runs_")

    run_id = 0
    page_id = 0

    while page_id < total_pages:
        records = []

        for _ in range(B):
            if page_id >= total_pages:
                break

            records.extend(read_page(heap_path, page_id, page_size, EMPLOYEE_FORMAT))
            page_id += 1

        records.sort(key=lambda x: x[key_index])

        run_path = os.path.join(temp_dir, f"run_{run_id}.bin")
        records_per_page = get_records_per_page(EMPLOYEE_FORMAT, page_size)

        out_page_id = 0
        for i in range(0, len(records), records_per_page):
            write_page(
                run_path,
                out_page_id,
                records[i:i + records_per_page],
                EMPLOYEE_FORMAT,
                page_size
            )
            out_page_id += 1

        run_paths.append(run_path)
        run_id += 1

    return run_paths


def multiway_merge(run_paths: list[str], output_path: str, page_size: int, buffer_size: int, sort_key: str):
    key_index = EMPLOYEE_COLUMNS[sort_key]
    records_per_page = get_records_per_page(EMPLOYEE_FORMAT, page_size)

    if os.path.exists(output_path):
        os.remove(output_path)

    run_states = []
    min_heap = []

    for run_id, run_path in enumerate(run_paths):
        first_page = read_page(run_path, 0, page_size, EMPLOYEE_FORMAT)

        state = {
            "path": run_path,
            "page_id": 0,
            "buffer": first_page,
            "index": 0,
            "total_pages": count_pages(run_path, page_size)
        }

        run_states.append(state)

        if first_page:
            record = first_page[0]
            heapq.heappush(min_heap, (record[key_index], run_id, record))

    output_buffer = []
    output_page_id = 0

    while min_heap:
        _, run_id, record = heapq.heappop(min_heap)
        output_buffer.append(record)

        if len(output_buffer) == records_per_page:
            write_page(output_path, output_page_id, output_buffer, EMPLOYEE_FORMAT, page_size)
            output_page_id += 1
            output_buffer = []

        state = run_states[run_id]
        state["index"] += 1

        if state["index"] >= len(state["buffer"]):
            state["page_id"] += 1

            if state["page_id"] < state["total_pages"]:
                state["buffer"] = read_page(
                    state["path"],
                    state["page_id"],
                    page_size,
                    EMPLOYEE_FORMAT
                )
                state["index"] = 0
            else:
                continue

        next_record = state["buffer"][state["index"]]
        heapq.heappush(min_heap, (next_record[key_index], run_id, next_record))

    if output_buffer:
        write_page(output_path, output_page_id, output_buffer, EMPLOYEE_FORMAT, page_size)


def external_sort(heap_path: str, output_path: str, page_size: int, buffer_size: int, sort_key: str) -> dict:
    B = buffer_size // page_size
    total_pages = count_pages(heap_path, page_size)

    start_total = time.time()

    start_phase1 = time.time()
    run_paths = generate_runs(heap_path, page_size, buffer_size, sort_key)
    end_phase1 = time.time()

    start_phase2 = time.time()
    multiway_merge(run_paths, output_path, page_size, buffer_size, sort_key)
    end_phase2 = time.time()

    output_pages = count_pages(output_path, page_size)

    metrics = {
        "runs_generated": len(run_paths),
        "pages_read": total_pages + sum(count_pages(r, page_size) for r in run_paths),
        "pages_written": sum(count_pages(r, page_size) for r in run_paths) + output_pages,
        "time_phase1_sec": end_phase1 - start_phase1,
        "time_phase2_sec": end_phase2 - start_phase2,
        "time_total_sec": time.time() - start_total
    }

    return metrics