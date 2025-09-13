# scripts/test_01_sheets_first_rows.py
from app.activities.sheets import read_batch_rows
rows = read_batch_rows("Batch 1")
print(rows)
