import csv
import os
import struct
from math import ceil

HEADER_FORMAT = "I"


def _encode_value(value, fmt):
    if fmt.endswith("s"):
        size = int(fmt[:-1])
        return value.encode("utf-8")[:size].ljust(size, b"\x00")
    return int(value)


def _decode_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8").replace("\x00", "").strip()
    return value


def get_record_size(record_format: str) -> int:
    return struct.calcsize(record_format)


def get_records_per_page(record_format: str, page_size: int) -> int:
    record_size = get_record_size(record_format)
    return (page_size - struct.calcsize(HEADER_FORMAT)) // record_size


def pack_record(row, record_format: str):
    formats = []
    i = 0

    while i < len(record_format):
        if record_format[i].isdigit():
            j = i
            while j < len(record_format) and record_format[j].isdigit():
                j += 1
            formats.append(record_format[i:j + 1])
            i = j + 1
        else:
            formats.append(record_format[i])
            i += 1

    values = [_encode_value(row[idx], formats[idx]) for idx in range(len(formats))]
    return struct.pack(record_format, *values)


def unpack_record(data: bytes, record_format: str):
    values = struct.unpack(record_format, data)
    return tuple(_decode_value(v) for v in values)


def export_to_heap(csv_path: str, heap_path: str, record_format: str, page_size: int):
    os.makedirs(os.path.dirname(heap_path), exist_ok=True)

    record_size = get_record_size(record_format)
    records_per_page = get_records_per_page(record_format, page_size)

    with open(csv_path, "r", encoding="utf-8") as csv_file, open(heap_path, "wb") as heap_file:
        reader = csv.reader(csv_file)
        next(reader)

        page_records = []

        for row in reader:
            page_records.append(row)

            if len(page_records) == records_per_page:
                page = struct.pack(HEADER_FORMAT, len(page_records))

                for record in page_records:
                    page += pack_record(record, record_format)

                page = page.ljust(page_size, b"\x00")
                heap_file.write(page)
                page_records = []

        if page_records:
            page = struct.pack(HEADER_FORMAT, len(page_records))

            for record in page_records:
                page += pack_record(record, record_format)

            page = page.ljust(page_size, b"\x00")
            heap_file.write(page)


def read_page(heap_path: str, page_id: int, page_size: int, record_format: str) -> list[tuple]:
    record_size = get_record_size(record_format)

    with open(heap_path, "rb") as file:
        file.seek(page_id * page_size)
        page = file.read(page_size)

    if not page:
        return []

    total_records = struct.unpack(HEADER_FORMAT, page[:4])[0]
    records = []

    offset = 4
    for _ in range(total_records):
        raw_record = page[offset:offset + record_size]
        records.append(unpack_record(raw_record, record_format))
        offset += record_size

    return records


def write_page(heap_path: str, page_id: int, records: list[tuple], record_format: str, page_size: int):
    records_per_page = get_records_per_page(record_format, page_size)

    if len(records) > records_per_page:
        raise ValueError("La cantidad de registros excede la capacidad de la página.")

    page = struct.pack(HEADER_FORMAT, len(records))

    for record in records:
        page += pack_record(record, record_format)

    page = page.ljust(page_size, b"\x00")

    with open(heap_path, "r+b" if os.path.exists(heap_path) else "wb") as file:
        file.seek(page_id * page_size)
        file.write(page)


def count_pages(heap_path: str, page_size: int) -> int:
    if not os.path.exists(heap_path):
        return 0

    return ceil(os.path.getsize(heap_path) / page_size)


def read_all_pages(heap_path: str, page_size: int, record_format: str):
    total_pages = count_pages(heap_path, page_size)

    for page_id in range(total_pages):
        yield read_page(heap_path, page_id, page_size, record_format)