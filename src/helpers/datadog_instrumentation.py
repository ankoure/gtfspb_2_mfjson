"""
DataDog Instrumentation Module

Provides unified access to tracing, metrics, and logging configuration.
Initializes ddtrace auto-instrumentation and DogStatsD client.
"""

import os
from functools import wraps
from typing import Callable, Any

from ddtrace import tracer, patch_all
from datadog import initialize, statsd

# Initialize DogStatsD client
initialize(
    statsd_host=os.getenv("DD_AGENT_HOST", "localhost"),
    statsd_port=int(os.getenv("DD_DOGSTATSD_PORT", 8125)),
    statsd_constant_tags=[
        f"agency:{os.getenv('PROVIDER', 'unknown')}",
        f"env:{os.getenv('DD_ENV', 'development')}",
    ],
)

# Enable auto-instrumentation for supported libraries
# This patches: requests, boto3, threading, logging
patch_all()

# Configure tracer with service info
tracer.configure(
    hostname=os.getenv("DD_AGENT_HOST", "localhost"),
    port=int(os.getenv("DD_TRACE_AGENT_PORT", 8126)),
)


class Metrics:
    """Namespace for custom metrics with consistent naming."""

    PREFIX = "gtfspb"

    # Feed consumption metrics
    FEED_FETCH_COUNT = f"{PREFIX}.feed.fetch.count"
    FEED_FETCH_SUCCESS = f"{PREFIX}.feed.fetch.success"
    FEED_FETCH_FAILURE = f"{PREFIX}.feed.fetch.failure"
    FEED_FETCH_DURATION = f"{PREFIX}.feed.fetch.duration"
    FEED_ENTITY_COUNT = f"{PREFIX}.feed.entity.count"
    FEED_EMPTY_COUNT = f"{PREFIX}.feed.empty.count"

    # Entity management metrics
    ENTITY_CREATED = f"{PREFIX}.entity.created"
    ENTITY_UPDATED = f"{PREFIX}.entity.updated"
    ENTITY_SAVED = f"{PREFIX}.entity.saved"
    ENTITY_DISCARDED = f"{PREFIX}.entity.discarded"
    ENTITY_DIRECTION_CHANGED = f"{PREFIX}.entity.direction_changed"
    ENTITY_MEMORY_CULLED = f"{PREFIX}.entity.memory_culled"
    ENTITY_ACTIVE_COUNT = f"{PREFIX}.entity.active.gauge"

    # Aggregation metrics
    AGGREGATION_FILES_PROCESSED = f"{PREFIX}.aggregation.files.processed"
    AGGREGATION_DURATION = f"{PREFIX}.aggregation.duration"
    AGGREGATION_SUCCESS = f"{PREFIX}.aggregation.success"
    AGGREGATION_FAILURE = f"{PREFIX}.aggregation.failure"
    AGGREGATION_TRAJECTORIES = f"{PREFIX}.aggregation.trajectories.count"

    # S3 metrics
    S3_UPLOAD_SUCCESS = f"{PREFIX}.s3.upload.success"
    S3_UPLOAD_FAILURE = f"{PREFIX}.s3.upload.failure"
    S3_UPLOAD_DURATION = f"{PREFIX}.s3.upload.duration"

    # Service lifecycle
    SERVICE_STARTUP = f"{PREFIX}.service.startup"
    SERVICE_STARTUP_FAILURE = f"{PREFIX}.service.startup.failure"


def trace_function(operation_name: str = None, resource: str = None):
    """
    Decorator for tracing functions with custom span names.

    Usage:
        @trace_function("feed.consumption", resource="VehiclePositionFeed")
        def consume_pb(self):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            span_name = operation_name or func.__name__
            with tracer.trace(span_name, resource=resource or func.__name__) as span:
                span.set_tag("function.name", func.__name__)
                span.set_tag("function.module", func.__module__)
                try:
                    result = func(*args, **kwargs)
                    span.set_tag("result.success", True)
                    return result
                except Exception as e:
                    span.set_tag("result.success", False)
                    span.set_tag("error.message", str(e))
                    span.set_tag("error.type", type(e).__name__)
                    raise

        return wrapper

    return decorator


def get_statsd():
    """Return the initialized statsd client."""
    return statsd


def get_tracer():
    """Return the configured tracer instance."""
    return tracer
