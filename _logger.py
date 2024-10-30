import logging
from rich.logging import RichHandler
logging.basicConfig(level=logging.INFO,
                    format="%(message)s",
                    datefmt="[%X]",
                    handlers=[RichHandler(markup=True)])
logger = logging.getLogger("khamsat_scraper")