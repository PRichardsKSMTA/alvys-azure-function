"""Activity to relay error notifications to the Logic App."""
from typing import Dict

from utils.alerts import send_error_notification


def main(params: Dict[str, str]) -> None:
    function_name = params.get("functionName", "unknown")
    message = params.get("message", "")
    stack_trace = params.get("stackTrace", "")
    correlation_id = params.get("correlationId")
    send_error_notification(function_name, message, stack_trace, correlation_id)
