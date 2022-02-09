import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison, get_tables_in_schema

# steps happened:
# 1. copy all of the sql statement over in python file
# 2. moved seed.sql over
# 3. select * from {{ source('raw', 'seed') }} has to be updated to {{ this.schema }}.seed
# 4. created models become created tables, assertFalse might need to be updated
# 5. assert_correct_schemas needs to be implemented

# Question how to validate the tests actually correctly tested the intended case

# idea of script to automate this:
# 1. unittest2pytest can handle convert assert,
# 2. probably need something like the steps described above to convert the sql files
# 3. another pass to fix the functions and report remaining functions

base_users_sql = """
{{
    config(
        materialized = 'ephemeral',
        tags = ['base']
    )
}}

select * from {{ this.schema }}.seed
"""

alternative_users_sql = """
{# Same as users model, but with dots in the model name #}
{{
    config(
        materialized = 'table',
        tags=['dots']
    )
}}

select * from {{ ref('base_users') }}
"""

emails_alt_sql = """
select distinct email from {{ ref('users') }}
"""

emails_sql = """
{{
    config(materialized='ephemeral', tags=['base'])
}}

select distinct email from {{ ref('base_users') }}
"""

never_selected_sql = """
{{
    config(schema='_and_then')
}}

select * from {{ this.schema }}.seed
"""

user_rollup_dependency_sql = """
{{
    config(materialized='table')
}}

select * from {{ ref('users_rollup') }}
"""

users_rollup_sql = """
{{
    config(
        materialized = 'view',
        tags = 'bi'
    )
}}

with users as (

    select * from {{ ref('users') }}

)

select
    gender,
    count(*) as ct
from users
group by 1
"""

users_sql = """
{{
    config(
        materialized = 'table',
        tags=['bi', 'users']
    )
}}

select * from {{ ref('base_users') }}
"""


@pytest.fixture
def models():
    return {
        "base_users.sql": base_users_sql,
        "alternative.users.sql": alternative_users_sql,
        "emails_alt.sql": emails_alt_sql,
        "emails.sql": emails_sql,
        "never_selected.sql": never_selected_sql,
        "user_rollup_dependency.sql": user_rollup_dependency_sql,
        "users_rollup.sql": users_rollup_sql,
        "users.sql": users_sql,
    }


def test__postgres__specific_model(project, create_tables):
    results = run_dbt(["run", "--select", "users"])
    assert len(results) == 1

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")

    created_tables = get_tables_in_schema(project.test_schema)
    assert "users_rollup" not in created_tables
    assert "alternative.users" not in created_tables
    assert "base_users" not in created_tables
    assert "emails" not in created_tables
    # TODO add assert_correct_schemas function
