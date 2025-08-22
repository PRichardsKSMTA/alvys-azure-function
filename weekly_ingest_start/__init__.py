"""Timer-triggered starter for the weekly ingest orchestrator."""

import logging

import azure.durable_functions as df
import azure.functions as func


async def main(mytimer: func.TimerRequest, starter: str) -> None:
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new("weekly_ingest")
    logging.info("Started orchestration with ID %s", instance_id)

