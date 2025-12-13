import re

# -------------------------------------------
# 1. Parse SELECT Query
# -------------------------------------------
def parse_select_query(query: str, schema: dict) -> dict:
    """
    Parse:
        SELECT fields FROM table WHERE field = value
    Supports:
        - SELECT *
        - SELECT f1, f2, ...
        - Optional WHERE
    Returns:
        {"fields": [...], "table": str, "condition": {"field":..., "value":...} or None}
    """

    q = query.strip().rstrip(";")

    # Basic SELECT ... FROM ... (WHERE ...) pattern
    pattern = r"SELECT\s+(?P<fields>[\*\w,\s]+)\s+FROM\s+(?P<table>\w+)(?:\s+WHERE\s+(?P<cond>.+))?$"
    m = re.match(pattern, q, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Invalid SELECT query")

    # Fields
    fields_raw = m.group("fields").strip()
    if fields_raw == "*":
        fields = ["*"]
    else:
        fields = [f.strip() for f in fields_raw.split(",")]

    # Table
    table = m.group("table")
    if table not in schema:
        raise ValueError(f"Unknown table '{table}'")

    # Condition
    cond_raw = m.group("cond")
    condition = None
    if cond_raw:
        # Only handle: field = value
        cond_m = re.match(r"(\w+)\s*=\s*(.+)", cond_raw.strip())
        if not cond_m:
            raise ValueError("Invalid WHERE condition")
        field, value = cond_m.group(1), cond_m.group(2)

        # Remove quotes if present
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        else:
            # Try int or float
            if value.isdigit():
                value = int(value)
            else:
                try:
                    value = float(value)
                except:
                    pass

        condition = {"field": field, "value": value}

    return {"fields": fields, "table": table, "condition": condition}


# -------------------------------------------
# 2. Parse INSERT Query
# -------------------------------------------
def parse_insert_query(query: str, schema: dict) -> dict:
    """
    Parse:
        INSERT INTO table (f1, f2,...) VALUES (v1,v2,...)
    Returns:
        {"table":..., "fields":[...], "values":[...]}
    """

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
        # Strip quotes
        if v.startswith("'") and v.endswith("'"):
            values.append(v[1:-1])
        else:
            # Try numeric
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


# -------------------------------------------
# 3. Execute Query
# -------------------------------------------
def execute_query(query: str, schema: dict):
    """
    Executes SELECT or INSERT query directly on heap-file records.
    """

    q_lower = query.lower().strip()

    # ---------------------
    # INSERT
    # ---------------------
    if q_lower.startswith("insert"):
        info = parse_insert_query(query, schema)
        table = info["table"]
        fields = info["fields"]
        values = info["values"]

        # Build record dict
        record = {}
        for f, v in zip(fields, values):
            record[f] = v

        insert_structured_record(table, schema, record)
        return {"status": "OK", "message": "Record inserted"}

    # ---------------------
    # SELECT
    # ---------------------
    elif q_lower.startswith("select"):
        info = parse_select_query(query, schema)
        table = info["table"]
        fields = info["fields"]
        cond = info["condition"]

        # Read all structured records
        rows = read_all_structured_records(table, schema)

        # Apply condition if exists
        if cond:
            field = cond["field"]
            value = cond["value"]
            rows = [r for r in rows if r.get(field) == value]

        # Project fields
        if fields == ["*"]:
            return rows
        else:
            projected = []
            for r in rows:
                projected.append({f: r[f] for f in fields})
            return projected

    else:
        raise ValueError("Query must start with SELECT or INSERT")
