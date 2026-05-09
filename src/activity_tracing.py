"""Activity tracing utility for OpenTelemetry integration."""
import functools
from typing import Callable, Any
from temporalio import activity
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


def traced_activity(func: Callable) -> Callable:
    """
    Decorator to add OpenTelemetry spans to Temporal activities.

    Usage:
        @activity.defn
        @traced_activity
        async def my_activity(...):
            ...
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        activity_name = func.__name__
        with tracer.start_as_current_span(f"activity.{activity_name}") as span:
            # Add activity info to span
            span.set_attribute("activity.name", activity_name)
            span.set_attribute("temporal.activity.type", "activity")
            # Add arguments (sanitized)
            if args:
                span.set_attribute("activity.args.count", len(args))
            if kwargs:
                span.set_attribute("activity.kwargs.keys", str(list(kwargs.keys())))
            try:
                result = await func(*args, **kwargs)
                span.set_attribute("activity.status", "success")
                return result
            except Exception as e:
                span.set_attribute("activity.status", "failed")
                span.set_attribute("activity.error", str(e))
                raise
    return wrapper
