use color_eyre::Report;
use csv::Reader;
use fantoch::metrics::Histogram;
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Clone)]
pub struct Dstat {
    pub cpu_usr: Histogram,
    pub cpu_sys: Histogram,
    pub cpu_wait: Histogram,
    pub net_receive: Histogram,
    pub net_send: Histogram,
    pub mem_used: Histogram,
}

impl Dstat {
    pub fn new() -> Self {
        Self {
            cpu_usr: Histogram::new(),
            cpu_sys: Histogram::new(),
            cpu_wait: Histogram::new(),
            net_receive: Histogram::new(),
            net_send: Histogram::new(),
            mem_used: Histogram::new(),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.cpu_usr.merge(&other.cpu_usr);
        self.cpu_sys.merge(&other.cpu_sys);
        self.cpu_wait.merge(&other.cpu_wait);
        self.net_receive.merge(&other.net_receive);
        self.net_send.merge(&other.net_send);
        self.mem_used.merge(&other.mem_used);
    }

    pub fn from(start: u64, end: u64, path: String) -> Result<Self, Report> {
        // create all histograms
        let mut cpu_usr = Histogram::new();
        let mut cpu_sys = Histogram::new();
        let mut cpu_wait = Histogram::new();
        let mut net_receive = Histogram::new();
        let mut net_send = Histogram::new();
        let mut mem_used = Histogram::new();

        // open csv file
        let file = File::open(path)?;
        let mut buf = BufReader::new(file);

        // skip first 5 lines (non-header lines)
        for _ in 0..5 {
            let mut s = String::new();
            buf.read_line(&mut s)?;
        }

        // parse csv
        let mut reader = Reader::from_reader(buf);
        for record in reader.deserialize() {
            // parse csv row
            let record: DstatRow = record?;
            // only consider the record if within bounds
            if record.epoch >= start && record.epoch <= end {
                cpu_usr.increment(record.cpu_usr);
                cpu_sys.increment(record.cpu_sys);
                cpu_wait.increment(record.cpu_wait);
                net_receive.increment(record.net_receive);
                net_send.increment(record.net_send);
                mem_used.increment(record.mem_used);
            }
        }

        // create self
        let dstat = Self {
            cpu_usr,
            cpu_sys,
            cpu_wait,
            net_receive,
            net_send,
            mem_used,
        };
        Ok(dstat)
    }

    pub fn cpu_usr_mad(&self) -> (u64, u64) {
        Self::mad(&self.cpu_usr, None)
    }

    pub fn cpu_sys_mad(&self) -> (u64, u64) {
        Self::mad(&self.cpu_sys, None)
    }

    pub fn cpu_wait_mad(&self) -> (u64, u64) {
        Self::mad(&self.cpu_wait, None)
    }

    pub fn net_receive_mad(&self) -> (u64, u64) {
        Self::mad(&self.net_receive, Some(1_000_000f64))
    }

    pub fn net_send_mad(&self) -> (u64, u64) {
        Self::mad(&self.net_send, Some(1_000_000f64))
    }

    pub fn mem_used_mad(&self) -> (u64, u64) {
        Self::mad(&self.mem_used, Some(1_000_000f64))
    }

    // mad: Mean and Standard-deviation.
    fn mad(hist: &Histogram, norm: Option<f64>) -> (u64, u64) {
        let mut mean = hist.mean().value();
        let mut stddev = hist.stddev().value();
        if let Some(norm) = norm {
            mean = mean / norm;
            stddev = stddev / norm;
        }
        (mean.round() as u64, stddev.round() as u64)
    }
}

impl fmt::Debug for Dstat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let usr = self.cpu_usr_mad();
        let sys = self.cpu_sys_mad();
        let wait = self.cpu_wait_mad();
        let recv = self.net_receive_mad();
        let send = self.net_send_mad();
        let used = self.mem_used_mad();
        writeln!(f, "cpu:")?;
        writeln!(f, "  usr              {:>4}   stddev={}", usr.0, usr.1)?;
        writeln!(f, "  sys              {:>4}   stddev={}", sys.0, sys.1)?;
        writeln!(f, "  wait             {:>4}   stddev={}", wait.0, wait.1)?;
        writeln!(f, "net:")?;
        writeln!(f, "  (MB/s) receive   {:>4}   stddev={}", recv.0, recv.1)?;
        writeln!(f, "  (MB/s) send      {:>4}   stddev={}", send.0, send.1)?;
        writeln!(f, "mem:")?;
        writeln!(f, "  (MB) used        {:>4}   stddev={}", used.0, used.1)?;
        Ok(())
    }
}

// All fields:
// "time","epoch","usr","sys","idl","wai","stl","read","writ","recv","send"
// ,"used","free","buff","cach","read","writ"
#[derive(Debug, Deserialize)]
struct DstatRow {
    #[serde(deserialize_with = "parse_epoch")]
    epoch: u64,

    // cpu metrics
    #[serde(rename = "usr")]
    #[serde(deserialize_with = "f64_to_u64")]
    cpu_usr: u64,
    #[serde(rename = "sys")]
    #[serde(deserialize_with = "f64_to_u64")]
    cpu_sys: u64,
    #[serde(rename = "wai")]
    #[serde(deserialize_with = "f64_to_u64")]
    cpu_wait: u64,

    // net metrics
    #[serde(rename = "recv")]
    #[serde(deserialize_with = "f64_to_u64")]
    net_receive: u64,
    #[serde(rename = "send")]
    #[serde(deserialize_with = "f64_to_u64")]
    net_send: u64,

    // memory metrics
    #[serde(rename = "used")]
    #[serde(deserialize_with = "f64_to_u64")]
    mem_used: u64,
}

fn parse_epoch<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let epoch = String::deserialize(de)?;
    let epoch = epoch.parse::<f64>().expect("dstat value should be a float");
    // covert epoch to milliseconds
    let epoch = epoch * 1000f64;
    let epoch = epoch.round() as u64;
    Ok(epoch)
}

fn f64_to_u64<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let n = String::deserialize(de)?;
    let n = n.parse::<f64>().expect("dstat value should be a float");
    let n = n.round() as u64;
    Ok(n)
}
