# DECISIONS.md

1. **Indexes**: Created unique index `emp_code` on `employees` (guarantees employee uniqueness and fast lookup) and unique compound index `(emp_code, date)` on `attendance_logs` (prevents duplicate punch-ins per day). Added query indexes on `employees.department` and `attendance_logs(date desc, emp_code asc)` to back list/analytics queries. Considered and rejected indexing `history` because it is an embedded array read with the parent document.

2. **Punch-in race**: When two identical punch-in requests arrive simultaneously, both compute the same attendance date `YYYY-MM-DD`. Both attempt `insert_one()`. The unique index on `(emp_code, date)` allows exactly one insert to succeed (returning 201). The second request triggers a MongoDB `DuplicateKeyError`, caught by FastAPI to return a clean 409 conflict error without race conditions.

3. **Ties**: Uses MongoDB `$setWindowFields` with `$rank` for standard competition ranking (1, 2, 2, 4). Filtering `rank <= limit` occurs after ranking, so all employees tied at or below the cutoff rank are returned (potentially > `limit` items), ordered deterministically by `total_late_minutes` descending then `emp_code` ascending.

4. **Headcount**: The department summary aggregation starts from the `employees` collection filtered by `joined_on <= end_of_month` (R9). It performs a `$lookup` to `attendance_logs` for that month. Even employees with no attendance records exist in the `$group` stage and increment the department headcount.

5. **100x Data Scaling**: I would shard `attendance_logs` by `emp_code` (or hashed `emp_code`) to distribute read/write load, pre-aggregate daily/monthly summary collections using background workers or MongoDB Change Streams, and partition heavy trend queries with incremental materialized views.
