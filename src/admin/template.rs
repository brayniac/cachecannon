//! cachecannon's KPI template: the `service_queries` metadata value that the
//! Rezolus viewer renders as cachecannon's KPI section.
//!
//! The template travels in the recording. `/metrics/stream` sends it in its
//! handshake metadata, which a stream recorder (`rezolus record` to
//! `.dendro`) copies into the source's metadata, and the parquet recorder
//! (`[admin] parquet`) writes it into the file's metadata. The JSON is a
//! Rezolus `ServiceExtension` (`crates/dashboard/src/service_extension.rs`
//! in rezolus): a `service_name` and a list of KPIs, each with a `role`,
//! `title`, `query`, `type` and optional `unit_system`.
//!
//! This file is the source of truth for cachecannon's template. Rezolus keeps
//! a copy in its built-in registry (`crates/dashboard/templates/
//! cachecannon.json`) only as the fallback for recordings that carry no
//! template, such as those made before this one was sent, so the two do not
//! have to match.

/// The key the template is stored under, in both the handshake and the
/// parquet metadata.
pub const KEY_SERVICE_QUERIES: &str = "service_queries";

/// The template, verbatim from `templates/cachecannon.json`.
pub const SERVICE_QUERIES: &str = include_str!("../../templates/cachecannon.json");

#[cfg(test)]
mod tests {
    use super::*;

    /// The unit systems the Rezolus viewer can scale: rezolus's
    /// `dashboard::plot::Unit` (`crates/dashboard/src/plot.rs`). The viewer
    /// refuses a template with any other value, so one outside this list
    /// would stop the KPI section rendering.
    const UNIT_SYSTEMS: [&str; 9] = [
        "time",
        "bytes",
        "bitrate",
        "datarate",
        "percentage",
        "frequency",
        "power",
        "count",
        "rate",
    ];

    fn template() -> serde_json::Value {
        serde_json::from_str(SERVICE_QUERIES).expect("the template is JSON")
    }

    /// The fields rezolus's `ServiceExtension` requires, with the types it
    /// reads them as.
    #[test]
    fn template_has_the_service_extension_shape() {
        let doc = template();
        assert_eq!(doc["service_name"], "cachecannon");
        let kpis = doc["kpis"].as_array().expect("kpis is an array");
        assert!(!kpis.is_empty());
        for kpi in kpis {
            for key in ["role", "title", "query", "type"] {
                assert!(kpi[key].is_string(), "KPI {kpi} has no string {key}");
            }
            if let Some(unit) = kpi.get("unit_system") {
                let unit = unit.as_str().expect("unit_system is a string");
                assert!(
                    UNIT_SYSTEMS.contains(&unit),
                    "KPI {} has unit_system {unit:?}, which the viewer does not know",
                    kpi["title"]
                );
            }
        }
    }

    /// Every metric a KPI query selects (a name followed by `{`) is one this
    /// binary registers, so a renamed metric fails here rather than leaving
    /// an empty chart.
    #[test]
    fn template_queries_name_registered_metrics() {
        let registered: std::collections::HashSet<String> = metriken::metrics()
            .iter()
            .map(|m| m.name().to_string())
            .collect();
        let doc = template();
        let mut checked = 0;
        for kpi in doc["kpis"].as_array().unwrap() {
            let query = kpi["query"].as_str().unwrap();
            for name in selected_metrics(query) {
                assert!(
                    registered.contains(name),
                    "KPI {} queries {name}, which cachecannon does not register",
                    kpi["title"]
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "no metric names found in the queries");
    }

    /// The identifiers in `query` that are directly followed by `{`.
    fn selected_metrics(query: &str) -> Vec<&str> {
        let bytes = query.as_bytes();
        let mut out = Vec::new();
        for (i, b) in bytes.iter().enumerate() {
            if *b != b'{' {
                continue;
            }
            let start = bytes[..i]
                .iter()
                .rposition(|c| !(c.is_ascii_alphanumeric() || *c == b'_' || *c == b':'))
                .map_or(0, |p| p + 1);
            if start < i {
                out.push(&query[start..i]);
            }
        }
        out
    }

    #[test]
    fn selected_metrics_reads_names_before_a_selector() {
        assert_eq!(
            selected_metrics("sum(irate(requests_sent{source=\"x\"}[5s])) / get_count{}"),
            ["requests_sent", "get_count"]
        );
    }
}
