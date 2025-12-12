-- SYNTAX TEST "source.leapsql" "Basic SQL statements"

SELECT id, name FROM users;
--^^^^ keyword.other.DML.sql
--              ^^^^ keyword.other.DML.sql

SELECT COUNT(*) AS total FROM orders;
--     ^^^^^ support.function.sql
--              ^^ keyword.other.DML.sql
--                       ^^^^ keyword.other.DML.sql

-- This is a line comment
-- <- comment.line.double-dash.sql

/* Block comment here */
-- <- comment.block.sql

SELECT 'hello world' FROM dual;
--     ^^^^^^^^^^^^^ string.quoted.single.sql

SELECT * FROM users WHERE active = TRUE;
--                                 ^^^^ constant.language.sql

SELECT * FROM users WHERE deleted_at IS NULL;
--                                      ^^^^ constant.language.sql

SELECT 123, 45.67 FROM numbers;
--     ^^^ constant.numeric.sql
--          ^^^^^ constant.numeric.sql

SELECT 
    CASE 
--  ^^^^ keyword.other.sql
        WHEN status = 1 THEN 'active'
--      ^^^^ keyword.other.sql
--                      ^^^^ keyword.other.sql
        ELSE 'inactive'
--      ^^^^ keyword.other.sql
    END
--  ^^^ keyword.other.sql

SELECT COALESCE(name, 'default') FROM users;
--     ^^^^^^^^ support.function.sql

CREATE TABLE users (id INT);
--^^^^ keyword.other.DDL.sql
--     ^^^^^ keyword.other.DDL.sql
