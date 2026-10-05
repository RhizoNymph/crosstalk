//! What the converter saw, for the report: how injections arrived (the
//! label-class distribution) and the second hop.
//!
//! A **slot** is one injection vector of one attacked run. Its class is the
//! arrival of its first copy in a tool output the victim read, or `absent`
//! when no output the victim read holds it (the tool was never called, or a
//! defense removed the text). A **label** is one copy.

use std::fmt;

use serde::Serialize;

use super::classify::Arrival;

/// Counts per arrival class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ArrivalCounts {
    pub exact: u64,
    pub whitespace: u64,
    pub json_string: u64,
    pub yaml_string: u64,
    pub absent: u64,
}

impl ArrivalCounts {
    pub fn add_arrival(&mut self, arrival: Option<Arrival>) {
        match arrival {
            Some(Arrival::Exact) => self.exact += 1,
            Some(Arrival::Whitespace) => self.whitespace += 1,
            Some(Arrival::JsonString) => self.json_string += 1,
            Some(Arrival::YamlString) => self.yaml_string += 1,
            None => self.absent += 1,
        }
    }

    pub fn total(&self) -> u64 {
        self.exact + self.whitespace + self.json_string + self.yaml_string + self.absent
    }

    fn add(&mut self, other: &Self) {
        self.exact += other.exact;
        self.whitespace += other.whitespace;
        self.json_string += other.json_string;
        self.yaml_string += other.yaml_string;
        self.absent += other.absent;
    }
}

/// The attacker's indicators (a URL, an email address, an IBAN from the
/// injection) the victim later wrote into a tool call.
///
/// The truth model cannot label this hop yet: the attacker reads nothing
/// back, so there is no reader exchange for a victim → attacker
/// transmission. It is counted here instead (heuristic tier).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct SecondHop {
    /// Attacked runs whose attack succeeded (`security == true`).
    pub successful_attacks: u64,
    /// Of those, runs where the victim wrote an attacker indicator into a
    /// tool call after reading an injection.
    pub ioc_written: u64,
}

/// Per run or summed over a source.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Tally {
    pub runs: u64,
    /// Runs with at least one injection.
    pub attacked_runs: u64,
    pub slots: ArrivalCounts,
    pub labels: ArrivalCounts,
    pub second_hop: SecondHop,
}

impl Tally {
    pub fn add(&mut self, other: &Self) {
        self.runs += other.runs;
        self.attacked_runs += other.attacked_runs;
        self.slots.add(&other.slots);
        self.labels.add(&other.labels);
        self.second_hop.successful_attacks += other.second_hop.successful_attacks;
        self.second_hop.ioc_written += other.second_hop.ioc_written;
    }
}

fn share(part: u64, total: u64) -> String {
    if total == 0 {
        "-".to_owned()
    } else {
        // Counts stay far below 2^52, so the conversion is exact.
        format!("{:.1}%", 100.0 * part as f64 / total as f64)
    }
}

impl fmt::Display for Tally {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "agentdojo: {} runs, {} attacked",
            self.runs, self.attacked_runs
        )?;
        writeln!(f, "| class | slots | share | labels |")?;
        writeln!(f, "| --- | ---: | ---: | ---: |")?;
        let total = self.slots.total();
        let rows = [
            ("exact", self.slots.exact, self.labels.exact),
            ("whitespace", self.slots.whitespace, self.labels.whitespace),
            (
                "json_string",
                self.slots.json_string,
                self.labels.json_string,
            ),
            (
                "yaml_string",
                self.slots.yaml_string,
                self.labels.yaml_string,
            ),
            ("absent", self.slots.absent, 0),
        ];
        for (name, slots, labels) in rows {
            writeln!(
                f,
                "| {name} | {slots} | {} | {labels} |",
                share(slots, total)
            )?;
        }
        writeln!(
            f,
            "second hop (heuristic, unlabelled): {} of {} successful attacks wrote an attacker indicator into a tool call",
            self.second_hop.ioc_written, self.second_hop.successful_attacks
        )
    }
}
