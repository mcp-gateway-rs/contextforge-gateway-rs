use contextforge_gateway_rs_lib::Config;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    Layer, Registry, filter,
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[allow(dead_code)]
pub enum Guard {
    Appender(WorkerGuard),
}

const CONTROLLER_NAME: &str = "CONTEXTFORGE-GATEWAY-RS";

pub fn init_tracing_logging(configuration: &Config) -> Guard {
    let registry = Registry::default();

    let file_appender = tracing_appender::rolling::minutely(".", "contextforge-gateway-rs.log");
    let (non_blocking_appender, guard) = tracing_appender::non_blocking(file_appender);
    let file_filter =
        tracing_subscriber::EnvFilter::new(std::env::var("RUST_FILE_LOG").unwrap_or_else(|_| "debug".to_owned()));
    let console_filter =
        tracing_subscriber::EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".to_owned()));
    let tracing_filter =
        tracing_subscriber::EnvFilter::new(std::env::var("RUST_TRACE_LOG").unwrap_or_else(|_| "info".to_owned()));

    let console_layer = fmt::layer()
        .event_format(fmt::format().compact())
        .with_target(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_filter(filter::filter_fn(|meta| !meta.is_span()))
        .with_filter(console_filter);

    let file_layer = fmt::layer()
        .with_writer(non_blocking_appender)
        .with_target(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_filter(filter::filter_fn(|meta| !meta.is_span()))
        .with_filter(file_filter);

    if let Some(true) = configuration.enable_open_telemetry {
        if let Ok(exporter) = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint("http://127.0.0.1:4317")
            .with_timeout(std::time::Duration::from_secs(3))
            .build()
        {
            let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_id_generator(RandomIdGenerator::default())
                .with_sampler(Sampler::AlwaysOn)
                .with_resource(
                    opentelemetry_sdk::Resource::builder()
                        .with_attributes(vec![opentelemetry::KeyValue::new("service.name", CONTROLLER_NAME)])
                        .build(),
                )
                .build();

            let tracer = tracer_provider.tracer(CONTROLLER_NAME);
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

            registry.with(console_layer).with(file_layer).with(telemetry.with_filter(tracing_filter)).init();

            Guard::Appender(guard)
        } else {
            panic!("Can't do tracing");
        }
    } else {
        registry.with(console_layer).with(file_layer).init();
        Guard::Appender(guard)
    }
}
