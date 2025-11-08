from enum import Enum


class DeliveryStatus(Enum):
    """Enumeration of possible delivery outcomes."""

    SUCCESS = "success"
    RETRIABLE_ERROR = "retriable_error"
    FATAL_ERROR = "fatal_error"
    TIMEOUT = "timeout"
