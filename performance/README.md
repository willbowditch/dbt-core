# Performance Regression Testing
This directory includes dbt project setups to test on and a test runner written in Rust which runs specific dbt commands on each of the projects. Orchestration is done via the GitHub Action workflow in `/.github/workflows/performance.yml`. The workflow is scheduled to run every night on main and on supported release branches, but it can also be triggered manually.

The action spins up a github action container for each project-command combination. As this project grows, the number of concurrent containers is expected to become very large and make this test suite a more expensive and long running task. This is why this does not run on every PR, and instead must be triggered manually.

Performance baselines measured during our release process and are committed to this directory via github action. (TODO make the file and name it here).

## Threshold
Particle physicists commonly use a 5σ ("five sigma") standard for the threshold of a new discovery because that measurement could have been caused by random chance with less than one in a million odds. With performance regressions we want to be conservative enough that the vast majority of the time detected regressions are true while not being so conservative as to miss most actual regressions. This performance regression test suite uses a 3σ standard so that only about 1 in every 300 runs detects a false performance regression. Here is a chart of sigma standards and their cooresponding odds for a normal distribution:

| σ   | Chances            |
| --- | ------------------ |
| 1 σ | ~ 1 in 3           |
| 2 σ | ~ 1 in 22          |
| 3 σ | ~ 1 in 330         |
| 4 σ | ~ 1 in 16000       |
| 5 σ | ~ 1 in 1.8 million |
| 6 σ | ~ 1 in 500 million |

Here is a concrete example with real data:

In dbt v1.0.1, we have the following mean and standard deviation when parsing a dbt project with 2000 models:

mean: 49.82
stddev: 0.5212
3σ range: [48.26, 51.38]  =  49.82 ± (3 * 0.5212)

When we sample parsing the same project with dbt v1.0.2, if we get a mean of 52s we can be appropriately certain that this set of measurements does not comply with the model we derived from sampling dbt v1.0.1.

This can either mean that we have introduced a performance regression or that something has changed in the process we use to take the measurements such as variance within the Github action runtime.

## Adding a new dbt project
Just make a new directory under `performance/projects/`. It will automatically be picked up by the tests.

## Adding a new dbt command
TODO


# ::::: OLD README :::::
TODO delete this once it's not relevant.

# Performance Regression Testing
This directory includes dbt project setups to test on and a test runner written in Rust which runs specific dbt commands on each of the projects. Orchestration is done via the GitHub Action workflow in `/.github/workflows/performance.yml`. The workflow is scheduled to run every night, but it can also be triggered manually.

The github workflow hardcodes our baseline branch for performance metrics as `0.20.latest`. As future versions become faster, this branch will be updated to hold us to those new standards.

## Adding a new dbt project
Just make a new directory under `performance/projects/`. It will automatically be picked up by the tests.

## Adding a new dbt command
In `runner/src/measure.rs::measure` add a metric to the `metrics` Vec. The Github Action will handle recompilation if you don't have the rust toolchain installed.

## Future work
- add more projects to test different configurations that have been known bottlenecks
- add more dbt commands to measure
- possibly using the uploaded json artifacts to store these results so they can be graphed over time
- reading new metrics from a file so no one has to edit rust source to add them to the suite
- instead of building the rust every time, we could publish and pull down the latest version.
- instead of manually setting the baseline version of dbt to test, pull down the latest stable version as the baseline.
