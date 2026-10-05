# Databricks Unity Catalog Governance & Data Policy

## Governance Architecture
Unity Catalog enforces a 3-level namespace (`formula1_catalog.<schema>.<table>`), centralizing data access control, column masking, and data lineage.

## Key Policies Implemented (`sql/governance.sql`)
1. **Catalog & Schemas**: Provisioned `formula1_catalog` with `bronze`, `silver`, and `gold` schemas.
2. **Role-Based Access Control (RBAC)**:
   - `f1_data_engineers`: Full DDL & DML permissions.
   - `f1_data_analysts`: Read-only access to `silver` and `gold`.
   - `f1_data_scientists`: Read access to `silver` and `gold`.
3. **Dynamic Column Masking**: Redacts driver date of birth (`dob`) unless the user belongs to compliance or engineering groups.
4. **Row-Level Security**: Filters visible race records based on contractor membership.
