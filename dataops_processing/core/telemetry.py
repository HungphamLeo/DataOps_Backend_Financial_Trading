from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

def init_spark_tracer(service_name: str = "dataops-processing-spark", endpoint: str = "otel-collector:4317"):
    resource = Resource.create({"service.name": service_name, "service.version": "1.0.0"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)

def extract_trace_context(headers_list):
    """
    Trích xuất traceparent từ Kafka headers cấu trúc PySpark:
    [Row(key='traceparent', value=b'00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01')]
    """
    carrier = {}
    if headers_list:
        for header in headers_list:
            if header.key == "traceparent":
                carrier["traceparent"] = header.value.decode("utf-8") if isinstance(header.value, (bytes, bytearray)) else str(header.value)
    return TraceContextTextMapPropagator().extract(carrier)