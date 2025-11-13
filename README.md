# Lab 02: Binary Record Management with Table Schema

## Objective

Build a binary record management layer on top of the heap file system. This lab introduces structured records defined by a JSON schema.

Table Description (JSON)

Each table is described using a JSON schema in a file named ```schema.json```:


```json
{
  "table_name": "Employee",
  "file_name" : "c:\db\fEmployee.bin",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "char(20)"},
    {"name": "salary", "type": "float"}
  ]
}
```
* int: 4 bytes
* float: 4 bytes
* char(n): fixed-length string