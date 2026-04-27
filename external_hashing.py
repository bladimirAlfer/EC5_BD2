import os
import time
import tempfile

from heap_file import read_page, write_page, count_pages, get_records_per_page


DEPT_EMP_FORMAT = "i4s10s10s"

DEPT_EMP_COLUMNS = {
    "employee_id": 0,
    "department_id": 1,
    "from_date": 2,
    "to_date": 3
}


def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> list[str]:
    B = buffer_size // page_size
    k = B - 1

    total_pages = count_pages(heap_path, page_size)
    key_index = DEPT_EMP_COLUMNS[group_key]

    temp_dir = tempfile.mkdtemp(prefix="partitions_")
    partition_paths = [os.path.join(temp_dir, f"partition_{i}.bin") for i in range(k)]

    buffers = [[] for _ in range(k)]
    page_ids = [0 for _ in range(k)]

    records_per_page = get_records_per_page(DEPT_EMP_FORMAT, page_size)

    for page_id in range(total_pages):
        records = read_page(heap_path, page_id, page_size, DEPT_EMP_FORMAT)

        for record in records:
            partition_id = hash(record[key_index]) % k
            buffers[partition_id].append(record)

            if len(buffers[partition_id]) == records_per_page:
                write_page(
                    partition_paths[partition_id],
                    page_ids[partition_id],
                    buffers[partition_id],
                    DEPT_EMP_FORMAT,
                    page_size
                )
                page_ids[partition_id] += 1
                buffers[partition_id] = []

    for partition_id in range(k):
        if buffers[partition_id]:
            write_page(
                partition_paths[partition_id],
                page_ids[partition_id],
                buffers[partition_id],
                DEPT_EMP_FORMAT,
                page_size
            )

    return partition_paths


def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int, group_key: str) -> dict:
    result = {}
    key_index = DEPT_EMP_COLUMNS[group_key]

    for partition_path in partition_paths:
        local_hash = {}
        total_pages = count_pages(partition_path, page_size)

        for page_id in range(total_pages):
            records = read_page(partition_path, page_id, page_size, DEPT_EMP_FORMAT)

            for record in records:
                key = record[key_index]
                local_hash[key] = local_hash.get(key, 0) + 1

        for key, count in local_hash.items():
            result[key] = result.get(key, 0) + count

    return result


def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> dict:
    B = buffer_size // page_size

    start_total = time.time()

    start_phase1 = time.time()
    partition_paths = partition_data(heap_path, page_size, buffer_size, group_key)
    end_phase1 = time.time()

    start_phase2 = time.time()
    result = aggregate_partitions(partition_paths, page_size, buffer_size, group_key)
    end_phase2 = time.time()

    input_pages = count_pages(heap_path, page_size)
    partition_pages = sum(count_pages(p, page_size) for p in partition_paths)

    return {
        "result": result,
        "partitions_created": B - 1,
        "pages_read": input_pages + partition_pages,
        "pages_written": partition_pages,
        "time_phase1_sec": end_phase1 - start_phase1,
        "time_phase2_sec": end_phase2 - start_phase2,
        "time_total_sec": time.time() - start_total
    }