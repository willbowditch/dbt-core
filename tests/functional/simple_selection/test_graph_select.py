import os
import json
import pytest

from dbt.tests.util import run_dbt, run_sql_file
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

nested_users_sql = """
select 1 as id
"""

subdir_sql = """
select 1 as id
"""

selectors_yml = """
            selectors:
            - name: bi_selector
              description: This is a BI selector
              definition:
                method: tag
                value: bi
        """

schema_yml = """
version: 2
models:
  - name: emails
    columns:
    - name: email
      tests:
      - not_null:
          severity: warn
  - name: users
    columns:
    - name: id
      tests:
      - unique
  - name: users_rollup
    columns:
    - name: gender
      tests:
      - unique

sources:
  - name: raw
    schema: '{{ target.schema }}'
    tables:
      - name: seed

exposures:
  - name: user_exposure
    type: dashboard
    depends_on:
      - ref('users')
      - ref('users_rollup')
    owner:
      email: nope@example.com
  - name: seed_ml_exposure
    type: ml
    depends_on:
      - source('raw', 'seed')
    owner:
      email: nope@example.com
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
        "schema.yml": schema_yml,
        "test": {
            "subdir.sql": subdir_sql,
            "subdir": {
                "nested_users.sql": nested_users_sql,
            },
        },
    }


@pytest.fixture
def selectors():
    return selectors_yml


@pytest.fixture
def create_tables(test_data_dir, unique_schema):
    path = os.path.join(test_data_dir, "seed.sql")
    run_sql_file(path, unique_schema)


def assert_correct_schemas(project, table_comp):
    with table_comp.get_connection():
        exists = project.adapter.check_schema_exists(project.database, project.test_schema)
        assert exists

        schema = project.test_schema + "_and_then"
        exists = project.adapter.check_schema_exists(project.database, schema)
        assert not exists


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
    assert_correct_schemas(project, table_comp)


def test__postgres__tags(project, create_tables, project_root):

    results = run_dbt(["run", "--selector", "bi_selector"])
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert len(results) == 2
    created_tables = get_tables_in_schema(project.test_schema)
    assert not ("alternative.users" in created_tables)
    assert not ("base_users" in created_tables)
    assert not ("emails" in created_tables)
    assert "users" in created_tables
    assert "users_rollup" in created_tables
    assert_correct_schemas(project, table_comp)
    manifest_path = project_root.join("target/manifest.json")
    assert os.path.exists(manifest_path)
    with open(manifest_path) as fp:
        manifest = json.load(fp)
        assert "selectors" in manifest


def test__postgres__tags_and_children(project, create_tables):
    results = run_dbt(["run", "--select", "tag:base+"])
    assert len(results) == 5
    created_models = get_tables_in_schema(project.test_schema)
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert "emails_alt" in created_models
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "alternative.users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)


def test__postgres__tags_and_children_limited(project, create_tables):
    results = run_dbt(["run", "--select", "tag:base+2"])
    assert len(results) == 4
    created_models = get_tables_in_schema(project.test_schema)
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert "emails_alt" in created_models
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "alternative.users" in created_models
    assert "users_rollup_dependency" not in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)


def test__postgres__specific_model_and_children(project, create_tables):

    results = run_dbt(["run", "--select", "users+"])
    assert len(results) == 4
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = get_tables_in_schema(project.test_schema)
    assert "emails_alt" in created_models
    assert "base_users" not in created_models
    assert "alternative.users" not in created_models
    assert "emails" not in created_models
    assert_correct_schemas(project, table_comp)


def test__postgres__specific_model_and_children_limited(project, create_tables):

    results = run_dbt(["run", "--select", "users+1"])
    assert len(results) == 3
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = get_tables_in_schema(project.test_schema)
    assert "emails_alt" in created_models
    assert "base_users" not in created_models
    assert "emails" not in created_models
    assert "users_rollup_dependency" not in created_models
    assert_correct_schemas(project, table_comp)


def test__postgres__specific_model_and_parents(project, create_tables):
    results = run_dbt(["run", "--select", "+users_rollup"])
    assert len(results) == 2
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = get_tables_in_schema(project.test_schema)
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert_correct_schemas(project, table_comp)


def test__postgres__specific_model_and_parents_limited(project, create_tables):

    results = run_dbt(["run", "--select", "1+users_rollup"])
    assert len(results) == 2
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = get_tables_in_schema(project.test_schema)
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert_correct_schemas(project, table_comp)


def test__postgres__specific_model_with_exclusion(project, create_tables):
    results = run_dbt(["run", "--select", "+users_rollup", "--exclude", "models/users_rollup.sql"])
    assert len(results) == 1

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")

    created_models = get_tables_in_schema(project.test_schema)
    assert not ("base_users" in created_models)
    assert not ("users_rollup" in created_models)
    assert not ("emails" in created_models)
    assert_correct_schemas(project, table_comp)


def test__postgres__locally_qualified_name(project, project_root):
    results = run_dbt(["run", "--select", "test.subdir"])
    assert len(results) == 2
    created_models = get_tables_in_schema(project.test_schema)
    assert "users_rollup" not in created_models
    assert "base_users" not in created_models
    assert "emails" not in created_models
    assert "subdir" in created_models
    assert "nested_users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)

    results = run_dbt(["run", "--select", "models/test/subdir*"])
    assert len(results) == 2
    created_models = get_tables_in_schema(project.test_schema)
    assert "users_rollup" not in created_models
    assert "base_users" not in created_models
    assert "emails" not in created_models
    assert "subdir" in created_models
    assert "nested_users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)


def test__postgres__locally_qualified_name_model_with_dots(project, create_tables):
    results = run_dbt(["run", "--select", "alternative.users"])
    assert len(results) == 1
    created_models = get_tables_in_schema(project.test_schema)
    assert "alternative.users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)

    results = run_dbt(["run", "--select", "models/alternative.*"])
    assert len(results) == 1
    created_models = get_tables_in_schema(project.test_schema)
    assert "alternative.users" in created_models
    assert_correct_schemas(project, table_comp)


def test__postgres__childrens_parents(project, create_tables):
    results = run_dbt(["run", "--select", "@base_users"])
    assert len(results) == 5
    created_models = get_tables_in_schema(project.test_schema)
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" in created_models
    assert "alternative.users" in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models

    results = run_dbt(["test", "--select", "test_name:not_null"])
    assert len(results) == 1
    assert results[0].node.name == "not_null_emails_email"


def test__postgres__more_childrens_parents(project, create_tables):

    results = run_dbt(["run", "--select", "@users"])
    assert len(results) == 4
    created_models = get_tables_in_schema(project.test_schema)
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models
    results = run_dbt(["test", "--select", "test_name:unique"])
    assert len(results) == 2
    assert sorted([r.node.name for r in results]) == [
        "unique_users_id",
        "unique_users_rollup_gender",
    ]


def test__postgres__concat(project, create_tables):
    results = run_dbt(["run", "--select", "@emails_alt", "users_rollup"])
    assert len(results) == 3
    created_models = get_tables_in_schema(project.test_schema)
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models


def test__postgres__concat_exclude(project, create_tables):
    results = run_dbt(
        ["run", "--select", "@emails_alt", "users_rollup", "--exclude", "emails_alt"]
    )
    assert len(results) == 2
    created_models = get_tables_in_schema(project.test_schema)
    assert "users" in created_models
    assert "users_rollup" in created_models
    assert "emails_alt" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models


def test__postgres__concat_exclude_concat(project, create_tables):
    results = run_dbt(
        [
            "run",
            "--select",
            "@emails_alt",
            "users_rollup",
            "--exclude",
            "emails_alt",
            "users_rollup",
        ]
    )
    assert len(results) == 1
    created_models = get_tables_in_schema(project.test_schema)
    assert "users" in created_models
    assert "emails_alt" not in created_models
    assert "users_rollup" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models
    results = run_dbt(
        [
            "test",
            "--select",
            "@emails_alt",
            "users_rollup",
            "--exclude",
            "emails_alt",
            "users_rollup",
        ]
    )
    assert len(results) == 1
    assert results[0].node.name == "unique_users_id"


def test__postgres__exposure_parents(project, create_tables):
    results = run_dbt(["ls", "--select", "+exposure:seed_ml_exposure"])
    assert len(results) == 2
    assert sorted(results) == ["exposure:test.seed_ml_exposure", "source:test.raw.seed"]
    results = run_dbt(["ls", "--select", "1+exposure:user_exposure"])
    assert len(results) == 5
    assert sorted(results) == [
        "exposure:test.user_exposure",
        "test.unique_users_id",
        "test.unique_users_rollup_gender",
        "test.users",
        "test.users_rollup",
    ]
    results = run_dbt(["run", "-m", "+exposure:user_exposure"])
    assert len(results) == 2
    created_models = get_tables_in_schema(project.test_schema)
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models
