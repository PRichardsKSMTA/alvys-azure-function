"""Timer-triggered starter for the weekly ingest orchestrator."""

import logging
import traceback

import azure.durable_functions as df
import azure.functions as func

from utils.alerts import send_error_notification


async def main(mytimer: func.TimerRequest, starter: str) -> None:
    try:
        client = df.DurableOrchestrationClient(starter)
        instance_id = await client.start_new("weekly_ingest")
        logging.info("Started orchestration with ID %s", instance_id)
    except Exception as err:  # pragma: no cover - notify and re-raise
        stack = traceback.format_exc()
        send_error_notification("weekly_ingest_start", str(err), stack)
        raise

