"""OpenTelemetry observability setup for Temporal workflows and activities."""
import os
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.semconv.resource import ResourceAttributes
from typing import Optional

_tracer: Optional[trace.Tracer] = None
_meter: Optional[metrics.Meter] = None
_sites_counter: Optional[metrics.Counter] = None
_ads_counter: Optional[metrics.Counter] = None
_anomalies_counter: Optional[metrics.Counter] = None
_api_calls_counter: Optional[metrics.Counter] = None


def setup_telemetry(
    service_name: str = "fraud-detection",
    otlp_endpoint: Optional[str] = None,
    enable_console: bool = False,
) -> None:
    """
    Initialize OpenTelemetry tracing and metrics.

    Args:
        service_name: Name of the service for telemetry
        otlp_endpoint: OTLP collector endpoint (e.g., "http://localhost:4317")
        enable_console: Also export spans to console for debugging
    """
    global _tracer, _meter

    # Create resource with service identification
    resource = Resource.create({
        SERVICE_NAME: service_name,
        ResourceAttributes.SERVICE_VERSION: os.getenv("SERVICE_VERSION", "1.0.0"),
        ResourceAttributes.DEPLOYMENT_ENVIRONMENT: os.getenv("ENVIRONMENT", "development"),
    })

    # Setup Tracer
    tracer_provider = TracerProvider(resource=resource)

    # Add console exporter if enabled
    if enable_console:
        console_exporter = ConsoleSpanExporter()
        tracer_provider.add_span_processor(BatchSpanProcessor(console_exporter))

    # Add OTLP exporter if endpoint provided
    if otlp_endpoint:
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    trace.set_tracer_provider(tracer_provider)
    _tracer = tracer_provider.get_tracer(service_name)

    # Setup Metrics
    metrics_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
    ) if otlp_endpoint else None

    meter_provider = MeterProvider(resource=resource, metric_readers=[metrics_reader] if metrics_reader else [])
    metrics.set_meter_provider(meter_provider)
    _meter = meter_provider.get_meter(service_name)

    # Instrument logging
    LoggingInstrumentor().instrument()

    # Create standard metrics
    _create_standard_metrics()

    print(f"[telemetry] OpenTelemetry initialized for service: {service_name}")


def _create_standard_metrics() -> None:
    """Create standard application metrics."""
    global _sites_counter, _ads_counter, _anomalies_counter, _api_calls_counter
    if _meter is None:
        return

    _sites_counter = _meter.create_counter(
        name="sites_processed_total",
        description="Total number of sites processed",
        unit="1"
    )

    _ads_counter = _meter.create_counter(
        name="ads_detected_total",
        description="Total number of sites with ads detected",
        unit="1"
    )

    _anomalies_counter = _meter.create_counter(
        name="anomalies_flagged_total",
        description="Total number of anomalies flagged by Z-score",
        unit="1"
    )

    _meter.create_histogram(
        name="site_processing_duration_seconds",
        description="Time taken to process a single site",
        unit="s"
    )

    _api_calls_counter = _meter.create_counter(
        name="api_calls_total",
        description="Total API calls made to similar sites endpoint",
        unit="1"
    )


def get_tracer() -> trace.Tracer:
    """Get the global tracer instance."""
    global _tracer
    if _tracer is None:
        raise RuntimeError("Telemetry not initialized. Call setup_telemetry() first.")
    return _tracer


def get_meter() -> metrics.Meter:
    """Get the global meter instance."""
    global _meter
    if _meter is None:
        raise RuntimeError("Telemetry not initialized. Call setup_telemetry() first.")
    return _meter


# Helper functions for common telemetry patterns
def record_site_processed(success: bool = True, has_ads: bool = False) -> None:
    """Record metrics for a processed site."""
    try:
        if _sites_counter is not None:
            _sites_counter.add(1, {"success": str(success)})
        if has_ads and _ads_counter is not None:
            _ads_counter.add(1)
    except Exception:
        pass


def record_anomaly_flagged(status: str) -> None:
    """Record an anomaly being flagged."""
    try:
        if _anomalies_counter is not None:
            _anomalies_counter.add(1, {"status": status})
    except Exception:
        pass


def record_api_call(success: bool = True) -> None:
    """Record an API call."""
    try:
        if _api_calls_counter is not None:
            _api_calls_counter.add(1, {"success": str(success)})
    except Exception:
        pass
