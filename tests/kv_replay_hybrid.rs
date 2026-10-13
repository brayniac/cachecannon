//! KV replay rejects a hybrid model (linear-attention layers with a
//! recurrent state) before the run starts.

mod kv_common;

use cachecannon::{ColorMode, OutputFormat, create_formatter, run_benchmark_full};
use kv_common::{load_config, temp_path};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// A Hugging Face config with the given `layer_types`.
fn model_config(layer_types: &str) -> String {
    format!(
        r#"{{
            "num_hidden_layers": 4,
            "layer_types": {layer_types},
            "hidden_size": 512,
            "num_attention_heads": 8,
            "num_key_value_heads": 2,
            "head_dim": 64,
            "linear_num_value_heads": 4,
            "linear_num_key_heads": 2,
            "linear_key_head_dim": 32,
            "linear_value_head_dim": 32,
            "torch_dtype": "bfloat16"
        }}"#
    )
}

#[test]
fn kv_replay_rejects_a_hybrid_model() {
    // Control: the same config with every layer full attention is accepted.
    let dense = temp_path("dense.json");
    std::fs::write(
        &dense,
        model_config(r#"["full_attention", "full_attention", "full_attention", "full_attention"]"#),
    )
    .unwrap();
    let cfg = cachecannon::config::KvReplay {
        trace: "unused".into(),
        speedup: 0.0,
        model_config: Some(dense.clone()),
        kv_dtype: None,
        kv_bytes_per_token: None,
        mla: false,
        tp: 1,
        write_rule: Default::default(),
        key_prefix: "kv:".into(),
        key_format: Default::default(),
        requests_out: None,
    };
    cachecannon::kvreplay::geometry(&cfg).unwrap();
    std::fs::remove_file(&dense).unwrap();

    let hybrid = temp_path("hybrid.json");
    std::fs::write(
        &hybrid,
        model_config(
            r#"["linear_attention", "full_attention", "linear_attention", "full_attention"]"#,
        ),
    )
    .unwrap();
    let config = load_config(&format!(
        r#"
        [target]
        endpoints = ["127.0.0.1:1"]

        [workload.kv_replay]
        trace = "{}"
        model_config = "{}"

        [admin]
        format = "quiet"
        "#,
        temp_path("no-trace.parquet").display(),
        hybrid.display(),
    ));
    let err = run_benchmark_full(
        config,
        None,
        create_formatter(OutputFormat::Quiet, ColorMode::Never),
        Arc::new(AtomicBool::new(true)),
    )
    .unwrap_err()
    .to_string();
    std::fs::remove_file(&hybrid).unwrap();
    assert!(err.contains("Hybrid models are not supported"), "{err}");
}
