-- SYNTAX TEST "source.leapsql" "YAML Frontmatter"

/*---
materialized: table
schema: staging
tags:
  - daily
---*/

SELECT * FROM users;
--^^^^ keyword.other.DML.sql

/*---
materialized: view
---*/

SELECT id FROM customers;
