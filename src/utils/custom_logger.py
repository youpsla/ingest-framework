import logging

# Enable suds logging
# logging.basicConfig(level=logging.INFO)
# logging.getLogger("suds.client").setLevel(logging.DEBUG)
# logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)


logger = logging.getLogger("ingest")

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
