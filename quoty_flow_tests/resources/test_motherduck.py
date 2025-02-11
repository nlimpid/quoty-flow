import logging
import pytest
from src.quoty_flow.io_managers.motherduck import MotherDuckResource

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@pytest.fixture
def motherduck():
    return MotherDuckResource()


def test_execute_query(motherduck):
    query = "SELECT * FROM iris LIMIT 10"
    result = motherduck.execute_query(query)
    logger.info(f"result: {result}")
