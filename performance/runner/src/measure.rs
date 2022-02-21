use crate::exceptions::{CalculateError, IOError};
use crate::calculate::{Sample};
use serde::Deserialize;
use std::fs;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};


// TODO run hyperfine, read all files, create a Baseline value and write THAT out instead.

// `Metric` defines a dbt command that we want to measure on both the
// baseline and dev branches.
#[derive(Debug, Clone)]
struct Metric<'a> {
    name: &'a str,
    prepare: &'a str,
    cmd: &'a str,
}

impl Metric<'_> {
    // Returns the proper filename for the hyperfine output for this metric.
    fn outfile(&self, project: &str, branch: &str) -> String {
        [branch, "_", self.name, "_", project, ".json"].join("")
    }
}

// TODO this could have it's impure parts split out and tested.
//
// Given a directory, read all files in the directory and return each
// filename with the deserialized json contents of that file.
pub fn from_json_files<'a, A : Deserialize<'a>>(
    results_directory: &Path,
) -> Result<Vec<(PathBuf, A)>, CalculateError> {
    fs::read_dir(results_directory)
        .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
        .or_else(|e| Err(CalculateError::CalculateIOError(e)))?
        .into_iter()
        .map(|entry| {
            let ent: DirEntry = entry
                .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

            Ok(ent.path())
        })
        .collect::<Result<Vec<PathBuf>, CalculateError>>()?
        .iter()
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|path| {
            fs::read_to_string(path)
                .or_else(|e| Err(IOError::BadFileContentsErr(path.clone(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))
                .and_then(|contents| {
                    serde_json::from_str::<A>(&contents)
                        .or_else(|e| Err(CalculateError::BadJSONErr(path.clone(), Some(e))))
                })
                .map(|m| (path.clone(), m))
        })
        .collect()
}

fn get_projects<'a>(projects_directory: &PathBuf) -> Result<Vec<(PathBuf, String, &Metric<'a>)>, IOError> {
    let entries = fs::read_dir(projects_directory)
        .or_else(|e| Err(IOError::ReadErr(projects_directory.to_path_buf(), Some(e))))?;

    let unflattened_results = entries.map(|entry| {
        let path = entry
            .or_else(|e| Err(IOError::ReadErr(projects_directory.to_path_buf(), Some(e))))?
            .path();

        let project_name: String = path
            .file_name()
            .ok_or_else(|| IOError::MissingFilenameErr(path.clone().to_path_buf()))
            .and_then(|x| {
                x.to_str()
                    .ok_or_else(|| IOError::FilenameNotUnicodeErr(path.clone().to_path_buf()))
            })?
            .to_owned();

        // each project-metric pair we will run
        let pairs = metrics
            .iter()
            .map(|metric| (path.clone(), project_name.clone(), metric))
            .collect::<Vec<(PathBuf, String, &Metric<'a>)>>();

        Ok(pairs)
    })
    .collect::<Result<Vec<Vec<(PathBuf, String, &Metric<'a>)>>, IOError>>()?;

    Ok(unflattened_results.concat())
}

fn run_hyperfine(
    command: &str,
    prep: &str,
    runs: i32,
    output_file: &str
) -> Result<ExitStatus, IOError> {
    Command::new("hyperfine")
        .current_dir(path)
        // warms filesystem caches by running the command first without counting it.
        // alternatively we could clear them before each run
        .arg("--warmup")
        .arg("1")
        // --min-runs defaults to 10
        .arg("--min-runs")
        .arg(runs.to_string())
        .arg("--max-runs")
        .arg(runs.to_string())
        .arg("--prepare")
        .arg(prep)
        .arg(command)
        .arg("--export-json")
        .arg(output_file)
        // this prevents hyperfine from capturing dbt's output.
        // Noisy, but good for debugging when tests fail.
        .arg("--show-output")
        .status() // use spawn() here instead for more information
        .or_else(|e| Err(IOError::CommandErr(Some(e))))
}

pub fn take_samples(test_projects_dir: &PathBuf) -> Result<Vec<Sample>, IOError> {
    unimplemented!()
}

// Calls hyperfine via system command, and returns all the exit codes for each hyperfine run.
pub fn measure<'a>(
    projects_directory: &PathBuf,
    dbt_branch: &str,
) -> Result<Vec<ExitStatus>, IOError> {

    /*
        Strategy of this function body:
        1. Read all directory names in `projects_directory`
        2. Pair `n` projects with `m` metrics for a total of n*m pairs
        3. Run hyperfine on each project-metric pair
    */

    // To add a new metric to the test suite, simply define it in this list:
    // TODO: This could be read from a config file in a future version.
    let metrics: Vec<Metric> = vec![Metric {
        name: "parse",
        prepare: "rm -rf target/",
        cmd: "dbt parse --no-version-check",
    }];

    get_projects(projects_directory)?
        .iter()
        // run hyperfine on each pairing
        .map(|(path, project_name, &metric)| {
            let command = format!("{} --profiles-dir ../../project_config/", metric.cmd);
            let output_file = format!("../../results/{}", metric.outfile(project_name, dbt_branch));
            run_hyperfine(
                &command,
                metric.prepare,
                20,
                &output_file
            )
        })
        .collect()
}
