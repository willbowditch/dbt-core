# Include Module

contains the a few key default `dbt-core` sub directories inlcuding.

- `global_project` which defines the default implementations of jinja2 macros for `dbt-core ` which can be overwritten in each adapter repo to work more in line with those adapter plugins.

- `starter_project` which produces the default init project inside the `dbt-core` project after doing the `make dev` command

- A `index.html` file used to generate the docs page after using the `generate docs` command in dbt.

