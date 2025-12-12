-- SYNTAX TEST "source.leapsql" "Template expressions and statements"

SELECT * FROM {{ ref("users") }}
--            ^^ punctuation.section.interpolation.begin.leapsql
--               ^^^ support.function.builtin.leapsql

SELECT * FROM {{ source("raw", "customers") }}
--            ^^ punctuation.section.interpolation.begin.leapsql
--               ^^^^^^ support.function.builtin.leapsql

{* if var("include_deleted", False) *}
-- <- punctuation.section.interpolation.begin.leapsql
WHERE deleted_at IS NULL
{* endif *}
-- <- punctuation.section.interpolation.begin.leapsql

{* for col in columns *}
-- <- punctuation.section.interpolation.begin.leapsql
    {{ col }},
--  ^^ punctuation.section.interpolation.begin.leapsql
{* endfor *}
-- <- punctuation.section.interpolation.begin.leapsql

SELECT {{ this.name }} AS model_name
--     ^^ punctuation.section.interpolation.begin.leapsql
--        ^^^^ support.function.builtin.leapsql

SELECT {{ env.get("ENV") }} AS environment
--     ^^ punctuation.section.interpolation.begin.leapsql
--        ^^^ support.function.builtin.leapsql

{* set my_var = "value" *}
-- <- punctuation.section.interpolation.begin.leapsql

SELECT {{ config.get("schema") }} FROM dual
--     ^^ punctuation.section.interpolation.begin.leapsql
--        ^^^^^^ support.function.builtin.leapsql
