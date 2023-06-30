import asyncio
import logging
from os import path
from pathlib import Path

from odd_collector_sdk.collector import Collector
from odd_collector_gcp.domain.plugin import PLUGIN_FACTORY

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)

try:
    loop = asyncio.get_event_loop()

    config_path = Path().cwd() / "collector_config.yaml"
    root_package = "odd_collector_gcp"

    collector = Collector(config_path, root_package, PLUGIN_FACTORY)

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()

    asyncio.get_event_loop().run_forever()
except Exception as e:
    logging.error(e, exc_info=True)
    asyncio.get_event_loop().stop()
