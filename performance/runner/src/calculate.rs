use crate::exceptions::CalculateError;
use crate::measure;
use crate::measure::Metricc;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::str::FromStr;


// TODO move this to measure.rs
//
// This type exactly matches the type of array elements
// from hyperfine's output. Deriving `Serialize` and `Deserialize`
// gives us read and write capabilities via json_serde.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurement {
    pub command: String,
    pub mean: f64,
    pub stddev: f64,
    pub median: f64,
    pub user: f64,
    pub system: f64,
    pub min: f64,
    pub max: f64,
    pub times: Vec<f64>,
}

// TODO move this to measure.rs
//
// This type exactly matches the type of hyperfine's output.
// Deriving `Serialize` and `Deserialize` gives us read and
// write capabilities via json_serde.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurements {
    pub results: Vec<Measurement>,
}

// TODO move this to measure.rs?
//
// struct representation for "major.minor.patch" version.
// useful for ordering versions to get the latest
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, DeserializeFromStr, SerializeDisplay)]
pub struct Version {
    major: i32,
    minor: i32,
    patch: i32,
}


impl FromStr for Version {
    type Err = CalculateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ints: Vec<i32> = s
            .split(".")
            .map(|x| x.parse::<i32>())
            .collect::<Result<Vec<i32>, <i32 as FromStr>::Err>>()
            .or_else(|_| Err(CalculateError::VersionParseFail(s.to_owned())))?;

        match ints[..] {
            [major, minor, patch] => Ok(Version {
                major: major,
                minor: minor,
                patch: patch,
            }),
            _ => Err(CalculateError::VersionParseFail(s.to_owned())),
        }
    }
}

impl Version {
    #[cfg(test)]
    fn new(major: i32, minor: i32, patch: i32) -> Version {
        Version {
            major: major,
            minor: minor,
            patch: patch,
        }
    }
}

// A model for a single project-command pair
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricModel {
    pub metric: Metricc,
    pub ts: DateTime<Utc>,
    pub measurement: Measurement,
}

// A JSON structure outputted by the release process that contains
// a models for all the measured project-command pairs for this version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Baseline {
    pub version: Version,
    pub models: Vec<MetricModel>
}

// A JSON structure outputted by the release process that contains
// a history of all previous version baseline measurements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub metric: Metricc,
    pub value: f64,
    pub ts: DateTime<Utc>
}

impl Sample {
    // TODO make these results not panics.
    pub fn from_measurement(metric: Metricc, ts: DateTime<Utc>, measurement: &Measurement) -> Sample {
        match &measurement.times[..] {
            [] => panic!("found a sample with no measurement"),
            [x] => Sample {
                metric: metric,
                value: *x,
                ts: ts
            },
            _ => panic!("found a sample with too many measurements!"),
        }
    }
}

// The full output from a comparison between runs on the baseline
// and dev branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Calculation {
    pub version: Version,
    pub metric: Metricc,
    pub regression: bool,
    pub ts: DateTime<Utc>,
    pub sigma: f64,
    pub mean: f64,
    pub stddev: f64,
    pub threshold: f64
}

// This display instance is used to derive Serialize as well via `SerializeDisplay`
impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

// TODO find an alternative to all this cloning
fn calculate_regressions(samples: &[Sample], baseline: Baseline, sigma: f64) -> Vec<Calculation> {
    let m_samples: HashMap<Metricc, (f64, DateTime<Utc>)> =
        samples.into_iter().map(|x| (x.metric.clone(), (x.value, x.ts))).collect();

    baseline.models.clone().into_iter().filter_map(|metric_model| {
        let model = metric_model.measurement.clone();
        m_samples
            .get(&metric_model.metric)
            .map(|(value, ts)| {
                let threshold = model.mean + sigma * model.stddev;
                Calculation {
                    version: baseline.version,
                    metric: metric_model.metric,
                    regression: *value > threshold,
                    ts: *ts,
                    sigma: sigma,
                    mean: model.mean,
                    stddev: model.stddev,
                    threshold: threshold
                }
            })
    })
    .collect()
}

// Top-level function. Given a path for the result directory, call the above
// functions to compare and collect calculations. Calculations include all samples
// regardless of whether they passed or failed.
pub fn regressions(baseline_dir: &PathBuf, projects_dir: &PathBuf, tmp_dir: &PathBuf) -> Result<Vec<Calculation>, CalculateError> {
    let baselines: Vec<Baseline> = measure::from_json_files::<Baseline>(Path::new(&baseline_dir))?
        .into_iter().map(|(_, x)| x).collect();
    let samples: Vec<Sample> = measure::take_samples(projects_dir, tmp_dir)?;

    // this is the baseline to compare these samples against
    let baseline: Baseline = match &baselines[..] {
        [] => panic!("no baselines found in dir"),
        [x, ..] => baselines.clone().into_iter().fold(x.clone(), |max, next| {
            if max.version >= next.version {
                max
            } else {
                next
            }
        })
    };

    // calculate regressions with a 3 sigma threshold
    Ok(calculate_regressions(&samples, baseline, 3.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_3sigma_regression() {
        let project = "test".to_owned();
        let metric = "detects 3 sigma".to_owned();

        let measurement = Measurement {
            command: "some command".to_owned(),
            mean: 1.00,
            stddev: 0.1,
            median: 1.00,
            user: 1.00,
            system: 1.00,
            min: 0.00,
            max: 2.00,
            times: vec![],
        };

        let baseline_metric = BaselineMetric {
            project: project.clone(),
            metric: metric.clone(),
            ts: Utc::now(),
            measurement: measurement,
        };

        let baseline = Baseline {
            version: Version::new(9,9,9),
            models: vec![baseline_metric]
        };

        let sample = Sample {
            project: project.clone(),
            metric: metric.clone(),
            value: 1.31,
            ts: Utc::now()
        };

        let calculations = calculate_regressions(
            &[sample],
            baseline,
            3.0 // 3 sigma
        );

        let regressions: Vec<&Calculation> =
            calculations.iter().filter(|calc| calc.regression).collect();

        // expect one regression for the mean being outside the 3 sigma
        println!("{:#?}", regressions);
        assert_eq!(regressions.len(), 1);
        assert_eq!(regressions[0].metric, "detects 3 sigma");
    }

    #[test]
    fn passes_near_3sigma() {
        let project = "test".to_owned();
        let metric = "passes near 3 sigma".to_owned();

        let measurement = Measurement {
            command: "some command".to_owned(),
            mean: 1.00,
            stddev: 0.1,
            median: 1.00,
            user: 1.00,
            system: 1.00,
            min: 0.00,
            max: 2.00,
            times: vec![],
        };

        let baseline_metric = BaselineMetric {
            project: project.clone(),
            metric: metric.clone(),
            ts: Utc::now(),
            measurement: measurement,
        };

        let baseline = Baseline {
            version: Version::new(9,9,9),
            models: vec![baseline_metric]
        };

        let sample = Sample {
            project: project.clone(),
            metric: metric.clone(),
            value: 1.29,
            ts: Utc::now()
        };

        let calculations = calculate_regressions(
            &[sample],
            baseline,
            3.0 // 3 sigma
        );

        let regressions: Vec<&Calculation> =
            calculations.iter().filter(|calc| calc.regression).collect();

        // expect no regressions
        println!("{:#?}", regressions);
        assert!(regressions.is_empty());
    }

    // The serializer and deserializer are custom implementations
    // so they should be tested that they match.
    #[test]
    fn version_serialize_loop() {
        let v = Version {
            major: 1,
            minor: 2,
            patch: 3,
        };
        let v2 = serde_json::from_str::<Version>(&serde_json::to_string_pretty(&v).unwrap());
        assert_eq!(v, v2.unwrap());
    }
}
