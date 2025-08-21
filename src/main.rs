use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::Request,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::post,
};
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::{Value, json};

use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Include the generated protobuf code
include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));

// Check if detailed time series printing is enabled
fn should_print_detailed_timeseries() -> bool {
    std::env::var("PRINT_TIMESERIES")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Create the router
    let app = Router::new()
        .route("/api/v1/write", post(handle_remote_write))
        .route("/receive", post(handle_remote_write)) // Alternative endpoint
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9090").await?;
    info!("Prometheus Remote Write Debug Server listening on http://0.0.0.0:9090");
    info!("Send remote write requests to:");
    info!("   POST http://localhost:9090/api/v1/write");
    info!("   POST http://localhost:9090/receive");
    info!("");

    if should_print_detailed_timeseries() {
        info!("Detailed time series printing: ENABLED (PRINT_TIMESERIES=true)");
    } else {
        info!("Detailed time series printing: DISABLED (set PRINT_TIMESERIES=1 or true to enable)");
    }
    info!("");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_remote_write(
    headers: HeaderMap,
    request: Request,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to read request body: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Failed to read request body"})),
            ));
        }
    };

    // Print separator
    println!("\n{}", "=".repeat(80));
    println!("NEW REMOTE WRITE REQUEST");
    println!("{}", "=".repeat(80));

    // Print timestamp
    let now: DateTime<Utc> = Utc::now();
    println!("Timestamp: {}", now.format("%Y-%m-%d %H:%M:%S UTC"));

    // Print headers
    print_headers(&headers);

    // Decompress the body if it's snappy compressed
    let decompressed_body = match decompress_body(&body, &headers) {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to decompress body: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Decompression failed: {}", e)})),
            ));
        }
    };

    // Parse the protobuf message
    let write_request = match WriteRequest::decode(decompressed_body.as_slice()) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to decode protobuf: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Protobuf decode failed: {}", e)})),
            ));
        }
    };

    // Print the parsed data
    print_write_request(&write_request);

    println!("{}", "=".repeat(80));
    println!("Request processed successfully");
    println!("{}", "=".repeat(80));

    Ok((StatusCode::OK, Json(json!({"status": "success"}))))
}

fn print_headers(headers: &HeaderMap) {
    println!("\nREQUEST HEADERS:");
    println!("{}", "-".repeat(40));

    let mut sorted_headers: Vec<_> = headers.iter().collect();
    sorted_headers.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));

    for (name, value) in sorted_headers {
        let value_str = value.to_str().unwrap_or("<invalid UTF-8>");
        println!("   {}: {}", name, value_str);
    }

    // Print content length and encoding info
    if let Some(content_length) = headers.get("content-length") {
        println!(
            "\nContent-Length: {} bytes",
            content_length.to_str().unwrap_or("unknown")
        );
    }

    if let Some(content_encoding) = headers.get("content-encoding") {
        println!(
            "Content-Encoding: {}",
            content_encoding.to_str().unwrap_or("unknown")
        );
    }

    if let Some(user_agent) = headers.get("user-agent") {
        println!("User-Agent: {}", user_agent.to_str().unwrap_or("unknown"));
    }
}

fn decompress_body(body: &Bytes, headers: &HeaderMap) -> Result<Vec<u8>> {
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    match content_encoding {
        "snappy" => {
            let original_size = body.len();
            println!("\nDecompressing Snappy data...");
            println!("Compressed size: {} bytes", original_size);
            let decompressed = snap::raw::Decoder::new().decompress_vec(body)?;
            let decompressed_size = decompressed.len();
            let ratio = decompressed_size as f64 / original_size as f64;
            println!(
                "Decompressed size: {} bytes (ratio: {:.2}x)",
                decompressed_size, ratio
            );
            Ok(decompressed)
        }
        "zstd" => {
            let original_size = body.len();
            println!("\nDecompressing Zstd data...");
            println!("Compressed size: {} bytes", original_size);
            let decompressed = zstd::bulk::decompress(body, 10 * 1024 * 1024)?; // 10MB limit
            let decompressed_size = decompressed.len();
            let ratio = decompressed_size as f64 / original_size as f64;
            println!(
                "Decompressed size: {} bytes (ratio: {:.2}x)",
                decompressed_size, ratio
            );
            Ok(decompressed)
        }
        "" | "identity" => {
            println!("\nNo compression detected");
            println!("Raw body size: {} bytes", body.len());
            Ok(body.to_vec())
        }
        other => {
            println!(
                "\nUnknown content encoding: {}, treating as uncompressed",
                other
            );
            println!("Raw body size: {} bytes", body.len());
            Ok(body.to_vec())
        }
    }
}

fn print_write_request(request: &WriteRequest) {
    println!("\nPROMETHEUS WRITE REQUEST:");
    println!("{}", "-".repeat(40));

    println!("TimeSeries count: {}", request.timeseries.len());
    println!("Metadata count: {}", request.metadata.len());

    // Print metadata first
    if !request.metadata.is_empty() {
        println!("\nMETADATA:");
        for (i, metadata) in request.metadata.iter().enumerate() {
            println!(
                "   [{}] {} ({})",
                i,
                metadata.metric_family_name,
                format_metric_type(metadata.r#type)
            );
            if !metadata.help.is_empty() {
                println!("       Help: {}", metadata.help);
            }
            if !metadata.unit.is_empty() {
                println!("       Unit: {}", metadata.unit);
            }
        }
    }

    // Print time series (optional, controlled by environment variable)
    if should_print_detailed_timeseries() {
        for (i, ts) in request.timeseries.iter().enumerate() {
            println!("\nTimeSeries [{}]:", i);

            // Print labels
            print_labels(&ts.labels);

            // Print samples
            if !ts.samples.is_empty() {
                println!("   Samples ({}):", ts.samples.len());
                for (j, sample) in ts.samples.iter().enumerate() {
                    let dt = timestamp_to_datetime(sample.timestamp);
                    println!(
                        "      [{}] {} @ {} ({})",
                        j, sample.value, sample.timestamp, dt
                    );
                }
            }

            // Print exemplars
            if !ts.exemplars.is_empty() {
                println!("   Exemplars ({}):", ts.exemplars.len());
                for (j, exemplar) in ts.exemplars.iter().enumerate() {
                    let dt = timestamp_to_datetime(exemplar.timestamp);
                    println!(
                        "      [{}] {} @ {} ({})",
                        j, exemplar.value, exemplar.timestamp, dt
                    );
                    if !exemplar.labels.is_empty() {
                        print!("         Labels: ");
                        for (k, label) in exemplar.labels.iter().enumerate() {
                            if k > 0 {
                                print!(", ");
                            }
                            print!("{}=\"{}\"", label.name, label.value);
                        }
                        println!();
                    }
                }
            }

            // Print histograms
            if !ts.histograms.is_empty() {
                println!("   Histograms ({}):", ts.histograms.len());
                for (j, histogram) in ts.histograms.iter().enumerate() {
                    let dt = timestamp_to_datetime(histogram.timestamp);
                    println!("      [{}] @ {} ({})", j, histogram.timestamp, dt);

                    match &histogram.count {
                        Some(histogram::Count::CountInt(c)) => {
                            println!("         Count: {}", c)
                        }
                        Some(histogram::Count::CountFloat(c)) => {
                            println!("         Count: {}", c)
                        }
                        None => {}
                    }

                    println!("         Sum: {}", histogram.sum);
                    println!("         Schema: {}", histogram.schema);
                    println!("         Zero threshold: {}", histogram.zero_threshold);

                    match &histogram.zero_count {
                        Some(histogram::ZeroCount::ZeroCountInt(c)) => {
                            println!("         Zero count: {}", c)
                        }
                        Some(histogram::ZeroCount::ZeroCountFloat(c)) => {
                            println!("         Zero count: {}", c)
                        }
                        None => {}
                    }

                    if !histogram.positive_spans.is_empty() {
                        println!(
                            "         Positive spans: {} buckets",
                            histogram.positive_spans.len()
                        );
                    }
                    if !histogram.negative_spans.is_empty() {
                        println!(
                            "         Negative spans: {} buckets",
                            histogram.negative_spans.len()
                        );
                    }
                }
            }
        }
    } else {
        println!("\nTimeSeries details: HIDDEN (set PRINT_TIMESERIES=1 to show)");

        // Show just a count of unique metric names
        let mut metric_names = std::collections::HashSet::new();
        for ts in &request.timeseries {
            if let Some(name_label) = ts.labels.iter().find(|l| l.name == "__name__") {
                metric_names.insert(name_label.value.clone());
            }
        }

        if !metric_names.is_empty() {
            println!("Unique metrics: {}", metric_names.len());
        }
    }

    // Print summary statistics
    let total_samples: usize = request.timeseries.iter().map(|ts| ts.samples.len()).sum();
    let total_exemplars: usize = request.timeseries.iter().map(|ts| ts.exemplars.len()).sum();
    let total_histograms: usize = request
        .timeseries
        .iter()
        .map(|ts| ts.histograms.len())
        .sum();

    println!("\nSUMMARY:");
    println!("   TimeSeries: {}", request.timeseries.len());
    println!("   Total Samples: {}", total_samples);
    println!("   Total Exemplars: {}", total_exemplars);
    println!("   Total Histograms: {}", total_histograms);
    println!("   Metadata entries: {}", request.metadata.len());
}

fn print_labels(labels: &[Label]) {
    if labels.is_empty() {
        return;
    }

    // Group labels by type for better readability
    let mut metric_labels = Vec::new();
    let mut other_labels = Vec::new();

    for label in labels {
        if label.name == "__name__" {
            metric_labels.insert(0, label);
        } else if label.name.starts_with("__") {
            other_labels.push(label);
        } else {
            metric_labels.push(label);
        }
    }

    print!("   Labels: ");
    let mut first = true;

    // Print metric name first
    for label in &metric_labels {
        if !first {
            print!(", ");
        }
        print!("{}=\"{}\"", label.name, label.value);
        first = false;
    }

    // Print internal labels
    for label in &other_labels {
        if !first {
            print!(", ");
        }
        print!("{}=\"{}\"", label.name, label.value);
        first = false;
    }

    println!();
}

fn timestamp_to_datetime(timestamp_ms: i64) -> String {
    if timestamp_ms == 0 {
        return "N/A".to_string();
    }

    match DateTime::from_timestamp(
        timestamp_ms / 1000,
        ((timestamp_ms % 1000) * 1_000_000) as u32,
    ) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("Invalid timestamp: {}", timestamp_ms),
    }
}

fn format_metric_type(metric_type: i32) -> String {
    match metric_type {
        0 => "UNKNOWN".to_string(),
        1 => "COUNTER".to_string(),
        2 => "GAUGE".to_string(),
        3 => "HISTOGRAM".to_string(),
        4 => "GAUGEHISTOGRAM".to_string(),
        5 => "SUMMARY".to_string(),
        6 => "INFO".to_string(),
        7 => "STATESET".to_string(),
        _ => format!("UNKNOWN({})", metric_type),
    }
}
