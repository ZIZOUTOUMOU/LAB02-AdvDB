# Lab 02: Binary Record Management with Table Schema

## Objective

Build a binary record management layer on top of the heap file system. This lab introduces structured records defined by a JSON schema.

Table Description (JSON)

Each table is described using a JSON schema in a file named ```schema.json```:


```json
{
  {
    "table_name": "Employee",
    "file_name" : "<file location>",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "char(20)"},
      {"name": "salary", "type": "float"}
    ]
  },
  {
    "table_name": "Dept",
    "file_name" : "<file location>",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "char(20)"},
      {"name": "Location", "type": "varchar(40)"}
    ]
  }
}
```
* int: 4 bytes
* float: 4 bytes
* char(n): fixed-length string
* varchar(n): 1 byte for the length  + n bytes content

## Tasks

### 1. Encode a record (dictionary → bytes)

```python
def encode_record(record_dict, table_name, schema) -> bytes:
    """
    Convert a Python dictionary record into binary form
    based on the JSON table description.
    """
  
```
A record should be provided in **Python dictionary form**, for example:

```python
record = {
    "id": 12,
    "name": "Alice",
    "age": 30
}
```

### 2. Decode a record (bytes → dictionary)

```python
def decode_record(record_bytes, table_name, schema) -> dict:
    """
    Convert a binary record into a Python dictionary
    based on the JSON table description.
    """
    
```

### 3. Insert a Structured Record into the Heap File

```python
def insert_structured_record(table_name, schema, record_dict):
    """
    Encode a structured record and insert it into the heap file.
    """
```

### 4. Read All Structured Records from the Heap File

```python
def read_all_structured_records(table_name, schema):
    """
    Retrieve and decode all structured records from the heap file.
    """
```

# Lab 03 — Simple Query Processor over Heap File

## Objective
Build a minimal SQL-like query processor operating on structured records stored in the heap file.

### Supported Query Model
- **SELECT queries**:

```sql
SELECT field_list FROM table_name WHERE field = value
```

## INSERT Queries

- **INSERT queries**:

```sql
INSERT INTO table_name (field1, field2, ...) VALUES (value1, value2, ...)
```

### Examples

```sql
-- Select all fields
SELECT * FROM Employee;

-- Select specific fields with condition
SELECT name, salary FROM Employee WHERE id = 3;

-- Insert a new record
INSERT INTO Employee (id, name, salary) VALUES (4, 'Alice', 4500);
```

## Tasks

### 1. Parse SELECT Queries

```python
def parse_select_query(query, schema) -> dict:
    """
    Parse a simple SELECT query into a structured dictionary.
    Example output:
    {
        "fields": ["name", "salary"],
        "table": "Employee",
        "condition": {"field": "id", "value": 3}
    }
    """
   
```

### 2. Parse INSERT Queries

```python
def parse_insert_query(query, schema) -> dict:
    """
    Parse a simple INSERT query into a structured dictionary.
    Example output:
    {
        "table": "Employee",
        "fields": ["id", "name", "salary"],
        "values": [4, "Alice", 4500]
    }
    """
    
```

### 3. Execute Queries

```python
def execute_query(query, schema):
    """
    Execute a SELECT or INSERT query on the structured records stored in the heap file.
    """
    
```

### Optional
Update the code to support AND conditions and comparison operators (>, <, ...)