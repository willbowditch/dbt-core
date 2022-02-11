# Include Module

Contains a few key default `dbt-core` sub directories used to generate default functionality for generated `dbt` projects.

- `global_project` which defines the default implementations of jinja2 macros for `dbt-core ` which can be overwritten in each adapter repo to work more in line with those adapter plugins. to view adapter specific jinja2 changes please check the relevant adapter repos [`adapter.sql` ](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/include/bigquery/macros/adapters.sql) file in the `include` directory or in the [`impl.py`](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/adapters/bigquery/impl.py) file for some ex. bigquery (truncate_relation).

- `starter_project` which produces the default  project after running the `dbt init` command for CLI. `dbt-cloud` initializes the project by using [dbt-starter-project](https://github.com/dbt-labs/dbt-starter-project).

- `index.html` a file built uising [dbt-docs](https://github.com/dbt-labs/dbt-docs) prior to new releases and replaced in the `dbt-core` directory; is used to generate the docs page after using the `generate docs` command in dbt.
