"""
Lab02 Full Implementation
- Heap file system
- Binary record management layer
- SQL-like query processor (SELECT/INSERT)
"""

import os
import json
import struct
import re
from typing import Dict, Any, List, Tuple

# --------------------------------
# Constants
# --------------------------------
PAGE_SIZE = 4096
FOOTER_SIZE = 4
SLOT_ENTRY_SIZE = 4
UINT16_FMT = ">H"
INT32_FMT = ">i"
FLOAT32_FMT = ">f"

# --------------------------------
# Primitive pack/unpack helpers
# --------------------------------
def _pack_int(v: int) -> bytes:
    return struct.pack(INT32_FMT, int(v))

def _unpack_int(b: bytes) -> int:
    if len(b) != 4:
        raise ValueError(f"Expected 4 bytes for int, got {len(b)}")
    return struct.unpack(INT32_FMT, b)[0]

def _pack_float(v: float) -> bytes:
    return struct.pack(FLOAT32_FMT, float(v))

def _unpack_float(b: bytes) -> float:
    if len(b) != 4:
        raise ValueError(f"Expected 4 bytes for float, got {len(b)}")
    return struct.unpack(FLOAT32_FMT, b)[0]

def _pack_char(s: str, n: int) -> bytes:
    b = s.encode("utf-8")
    if len(b) > n:
        b = b[:n]
    return b + b'\x00' * (n - len(b))

def _unpack_char(b: bytes) -> str:
    return b.rstrip(b'\x00').decode("utf-8")

def _pack_varchar(s: str, n: int) -> bytes:
    b = s.encode("utf-8")
    if len(b) > n:
        b = b[:n]
    if len(b) >= 256:
        b = b[:255]
    length = len(b)
    return struct.pack(">B", length) + b + b'\x00' * (n - length)

def _unpack_varchar(b: bytes, n: int) -> str:
    if len(b) == 0:
        return ""
    length = b[0]
    data = b[1:1+length]
    return data.decode("utf-8")

# --------------------------------
# Heap page helpers
# --------------------------------
def _read_footer(page: bytes) -> Tuple[int, int]:
    if len(page) != PAGE_SIZE:
        raise ValueError("page must be PAGE_SIZE bytes")
    slot_count = struct.unpack(UINT16_FMT, page[PAGE_SIZE - FOOTER_SIZE : PAGE_SIZE - 2])[0]
    free_offset = struct.unpack(UINT16_FMT, page[PAGE_SIZE - 2 : PAGE_SIZE])[0]
    return slot_count, free_offset

def _write_footer(page: bytearray, slot_count: int, free_offset: int) -> None:
    page[PAGE_SIZE - FOOTER_SIZE : PAGE_SIZE - 2] = struct.pack(UINT16_FMT, slot_count)
    page[PAGE_SIZE - 2 : PAGE_SIZE] = struct.pack(UINT16_FMT, free_offset)

def _slot_pos(slot_index: int) -> int:
    return PAGE_SIZE - FOOTER_SIZE - (slot_index + 1) * SLOT_ENTRY_SIZE

def _read_slot(page: bytes, slot_index: int, slot_count: int) -> Tuple[int, int]:
    if slot_index < 0 or slot_index >= slot_count:
        raise IndexError("slot_index out of range")
    pos = _slot_pos(slot_index)
    offset = struct.unpack(UINT16_FMT, page[pos:pos+2])[0]
    length = struct.unpack(UINT16_FMT, page[pos+2:pos+4])[0]
    return offset, length

def _write_slot(page: bytearray, slot_index: int, offset: int, length: int) -> None:
    pos = _slot_pos(slot_index)
    page[pos:pos+2] = struct.pack(UINT16_FMT, offset)
    page[pos+2:pos+4] = struct.pack(UINT16_FMT, length)

def initialize_empty_page() -> bytes:
    p = bytearray(PAGE_SIZE)
    _write_footer(p, 0, 0)
    return bytes(p)

def free_space_in_page(page: bytes) -> int:
    slot_count, free_offset = _read_footer(page)
    slot_table_start = PAGE_SIZE - FOOTER_SIZE - slot_count * SLOT_ENTRY_SIZE
    return slot_table_start - free_offset

def insert_record_into_page(page: bytes, record: bytes) -> Tuple[bytes, int]:
    slot_count, free_offset = _read_footer(page)
    needed = len(record) + SLOT_ENTRY_SIZE
    if free_space_in_page(page) < needed:
        raise ValueError("Not enough space in page")
    pg = bytearray(page)
    rec_off = free_offset
    pg[rec_off:rec_off+len(record)] = record
    new_slot = slot_count
    _write_slot(pg, new_slot, rec_off, len(record))
    _write_footer(pg, slot_count + 1, free_offset + len(record))
    return bytes(pg), new_slot

def get_record_from_page(page: bytes, slot_index: int) -> bytes:
    slot_count, _ = _read_footer(page)
    offset, length = _read_slot(page, slot_index, slot_count)
    return page[offset:offset+length]

# --------------------------------
# File-level heap functions
# --------------------------------
def create_heap_file(fname: str) -> None:
    with open(fname, "wb") as f:
        f.write(initialize_empty_page())

def read_page(fname: str, page_num: int) -> bytes:
    with open(fname, "rb") as f:
        f.seek(page_num * PAGE_SIZE)
        data = f.read(PAGE_SIZE)
    if len(data) != PAGE_SIZE:
        raise IOError("Failed to read full page")
    return data

def write_page(fname: str, page_num: int, page_data: bytes) -> None:
    if len(page_data) != PAGE_SIZE:
        raise ValueError("page_data must be PAGE_SIZE bytes")
    with open(fname, "r+b") as f:
        f.seek(page_num * PAGE_SIZE)
        f.write(page_data)

def append_page(fname: str, page_data: bytes) -> None:
    if len(page_data) != PAGE_SIZE:
        raise ValueError("page_data must be PAGE_SIZE bytes")
    with open(fname, "ab") as f:
        f.write(page_data)

def insert_record(fname: str, record: bytes) -> Tuple[int,int]:
    if not os.path.exists(fname) or os.path.getsize(fname) == 0:
        create_heap_file(fname)
    file_size = os.path.getsize(fname)
    num_pages = file_size // PAGE_SIZE
    for p in range(num_pages):
        page = read_page(fname, p)
        try:
            new_page, slot = insert_record_into_page(page, record)
            write_page(fname, p, new_page)
            return p, slot
        except ValueError:
            continue
    # append new page
    new_pg = bytearray(initialize_empty_page())
    pg_bytes, slot = insert_record_into_page(bytes(new_pg), record)
    append_page(fname, pg_bytes)
    return num_pages, slot

def get_all_raw_records(fname: str) -> List[Tuple[int,int,bytes]]:
    if not os.path.exists(fname):
        return []
    results = []
    fsize = os.path.getsize(fname)
    if fsize == 0:
        return []
    num_pages = fsize // PAGE_SIZE
    for p in range(num_pages):
        page = read_page(fname, p)
        slot_count, _ = _read_footer(page)
        for si in range(slot_count):
            rec = get_record_from_page(page, si)
            results.append((p, si, rec))
    return results

# --------------------------------
# Schema & Structured Records
# --------------------------------
def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    tables = {}
    if isinstance(data, list):
        for t in data:
            tables[t["table_name"]] = t
    elif isinstance(data, dict):
        if "table_name" in data:
            tables[data["table_name"]] = data
        else:
            tables = data
    return tables

def _parse_type(ft: str) -> Tuple[str,Any]:
    ft = ft.strip()
    if ft.startswith("char(") and ft.endswith(")"):
        return "char", int(ft[len("char("):-1])
    if ft.startswith("varchar(") and ft.endswith(")"):
        return "varchar", int(ft[len("varchar("):-1])
    if ft == "int":
        return "int", None
    if ft == "float":
        return "float", None
    raise ValueError(f"Unknown field type: {ft}")

def encode_record(record_dict: Dict[str,Any], table_name: str, schema: Dict[str,Any]) -> bytes:
    if table_name not in schema:
        raise ValueError("table not in schema")
    table = schema[table_name]
    fields = table["fields"]
    out_parts: List[bytes] = []
    for f in fields:
        name = f["name"]
        ftype = f["type"]
        base, param = _parse_type(ftype)
        val = record_dict.get(name, "" if base in ("char","varchar") else 0)
        if base == "int":
            out_parts.append(_pack_int(int(val)))
        elif base == "float":
            out_parts.append(_pack_float(float(val)))
        elif base == "char":
            out_parts.append(_pack_char(str(val), param))
        elif base == "varchar":
            out_parts.append(_pack_varchar(str(val), param))
        else:
            raise ValueError("Unsupported type")
    return b"".join(out_parts)

def decode_record(record_bytes: bytes, table_name: str, schema: Dict[str,Any]) -> Dict[str,Any]:
    if table_name not in schema:
        raise ValueError("table not in schema")
    table = schema[table_name]
    fields = table["fields"]
    res: Dict[str,Any] = {}
    idx = 0
    for f in fields:
        name = f["name"]
        ftype = f["type"]
        base, param = _parse_type(ftype)
        if base == "int":
            chunk = record_bytes[idx:idx+4]
            res[name] = _unpack_int(chunk)
            idx += 4
        elif base == "float":
            chunk = record_bytes[idx:idx+4]
            res[name] = _unpack_float(chunk)
            idx += 4
        elif base == "char":
            chunk = record_bytes[idx:idx+param]
            res[name] = _unpack_char(chunk)
            idx += param
        elif base == "varchar":
            chunk = record_bytes[idx:idx+1+param]
            res[name] = _unpack_varchar(chunk, param)
            idx += 1 + param
        else:
            raise ValueError("Unsupported type")
    return res

def table_file_name(table_desc: Dict[str,Any]) -> str:
    return table_desc.get("file_name") or (table_desc["table_name"] + ".heap")

def insert_structured_record(table_name: str, schema: Dict[str,Any], record_dict: Dict[str,Any]) -> Tuple[int,int]:
    table = schema[table_name]
    fname = table_file_name(table)
    rec = encode_record(record_dict, table_name, schema)
    return insert_record(fname, rec)

def read_all_structured_records(table_name: str, schema: Dict[str,Any]) -> List[Dict[str,Any]]:
    table = schema[table_name]
    fname = table_file_name(table)
    raws = get_all_raw_records(fname)
    out = []
    for (_, _, rec_bytes) in raws:
        out.append(decode_record(rec_bytes, table_name, schema))
    return out

# --------------------------------
# SQL-like Query Processor
# --------------------------------
def parse_select_query(query: str, schema: dict) -> dict:
    q = query.strip().rstrip(";")
    pattern = r"SELECT\s+(?P<fields>[\*\w,\s]+)\s+FROM\s+(?P<table>\w+)(?:\s+WHERE\s+(?P<cond>.+))?$"
    m = re.match(pattern, q, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Invalid SELECT query")
    fields_raw = m.group("fields").strip()
    if fields_raw == "*":
        fields = ["*"]
    else:
        fields = [f.strip() for f in fields_raw.split(",")]
    table = m.group("table")
    if table not in schema:
        raise ValueError(f"Unknown table '{table}'")
    cond_raw = m.group("cond")
    condition = None
    if cond_raw:
        cond_m = re.match(r"(\w+)\s*=\s*(.+)", cond_raw.strip())
        if not cond_m:
            raise ValueError("Invalid WHERE condition")
        field, value = cond_m.group(1), cond_m.group(2)
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        else:
            if value.isdigit():
                value = int(value)
            else:
                try:
                    value = float(value)
                except:
                    pass
        condition = {"field": field, "value": value}
    return {"fields": fields, "table": table, "condition": condition}

def parse_insert_query(query: str, schema: dict) -> dict:
    q = query.strip().rstrip(";")
    pattern = (
        r"INSERT\s+INTO\s+(?P<table>\w+)\s*"
        r"\((?P<fields>[\w,\s]+)\)\s*"
        r"VALUES\s*\((?P<values>.+)\)$"
    )
    m = re.match(pattern, q, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Invalid INSERT query")
    table = m.group("table")
    if table not in schema:
        raise ValueError(f"Unknown table '{table}'")
    fields = [f.strip() for f in m.group("fields").split(",")]
    raw_values = [v.strip() for v in m.group("values").split(",")]
    values = []
    for v in raw_values:
        if v.startswith("'") and v.endswith("'"):
            values.append(v[1:-1])
        else:
            if v.isdigit():
                values.append(int(v))
            else:
                try:
                    values.append(float(v))
                except:
                    values.append(v)
    if len(fields) != len(values):
        raise ValueError("Field count does not match value count")
    return {"table": table, "fields": fields, "values": values}

def execute_query(query: str, schema: dict):
    q_lower = query.lower().strip()
    if q_lower.startswith("insert"):
        info = parse_insert_query(query, schema)
        table = info["table"]
        fields = info["fields"]
        values = info["values"]
        record = {f:v for f,v in zip(fields, values)}
        insert_structured_record(table, schema, record)
        return {"status": "OK", "message": "Record inserted"}
    elif q_lower.startswith("select"):
        info = parse_select_query(query, schema)
        table = info["table"]
        fields = info["fields"]
        cond = info["condition"]
        rows = read_all_structured_records(table, schema)
        if cond:
            field = cond["field"]
            value = cond["value"]
            rows = [r for r in rows if r.get(field) == value]
        if fields == ["*"]:
            return rows
        else:
            projected = [{f:r[f] for f in fields} for r in rows]
            return projected
    else:
        raise ValueError("Query must start with SELECT or INSERT")

# --------------------------------
# Demo / self-test
# --------------------------------
if __name__ == "__main__":
    demo_schema_path = "schema.json"
    if not os.path.exists(demo_schema_path):
        demo_schema = [
            {"table_name": "Employee","file_name": "employee.heap",
             "fields":[{"name":"id","type":"int"},{"name":"name","type":"char(20)"},{"name":"salary","type":"float"}]},
            {"table_name": "Dept","file_name": "dept.heap",
             "fields":[{"name":"id","type":"int"},{"name":"name","type":"char(20)"},{"name":"Location","type":"varchar(40)"}]}
        ]
        with open(demo_schema_path, "w", encoding="utf-8") as f:
            json.dump(demo_schema, f, indent=2)
        print("Wrote demo schema.json")

    schema = load_schema(demo_schema_path)
    print("Loaded tables:", list(schema.keys()))

    # Remove old heap files
    for t in schema.values():
        fn = table_file_name(t)
        if os.path.exists(fn):
            os.remove(fn)

    # Insert demo records
    insert_structured_record("Employee", schema, {"id":1,"name":"Alice","salary":4500.0})
    insert_structured_record("Employee", schema, {"id":2,"name":"Bob","salary":3800.5})
    insert_structured_record("Employee", schema, {"id":3,"name":"Charlie","salary":5200.0})
    insert_structured_record("Dept", schema, {"id":10,"name":"HR","Location":"Algiers"})
    insert_structured_record("Dept", schema, {"id":20,"name":"R&D","Location":"Oran"})

    print("\nEmployees:")
    emps = read_all_structured_records("Employee", schema)
    for e in emps:
        print(e)

    print("\nDepartments:")
    depts = read_all_structured_records("Dept", schema)
    for d in depts:
        print(d)

    # Demo queries
    print("\nQuery demo:")
    q1 = "SELECT name, salary FROM Employee WHERE id = 2;"
    print("Query:", q1)
    print("Result:", execute_query(q1, schema))

    q2 = "INSERT INTO Employee (id, name, salary) VALUES (4, 'David', 4900);"
    print("Query:", q2)
    print("Result:", execute_query(q2, schema))

    q3 = "SELECT * FROM Employee;"
    print("Query:", q3)
    print("Result:", execute_query(q3, schema))
