"""Durable entity to track clients that failed ingest."""

import azure.durable_functions as df


def entity_function(context: df.DurableEntityContext):
    """Store SCAC codes that encountered ingest errors."""
    scacs = context.get_state(lambda: [])

    if context.operation_name == "add":
        scacs.append(context.get_input())
        context.set_state(scacs)
    elif context.operation_name == "get":
        context.set_result(scacs)


main = df.Entity.create(entity_function)

