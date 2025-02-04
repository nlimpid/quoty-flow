import logging
import pytest
from src.quoty_flow.resources.hkex import HKEXScraperResource

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@pytest.fixture
def scraper():
    return HKEXScraperResource()


def test_get_symbol_from_title(scraper):
    """测试股票代码提取"""
    assert scraper.get_symbol_from_title("Stock Code: 4242") == "4242"
    assert scraper.get_symbol_from_title("Invalid text") == ""


def test_normalize_date(scraper):
    """测试日期格式化"""
    assert scraper.normalize_date("2024-01-01*") == "2024-01-01"
    assert scraper.normalize_date(" 2024-01-01 ") == "2024-01-01"


def test_scrape_real_pages(scraper):
    """测试实际页面爬取"""
    # 测试 iBond 页面
    ibond_results = scraper.scrape_page(
        "https://www.hkgb.gov.hk/en/retail/iBond_Rates.html", "iBond"
    )
    logger.info(f"ibond_results: {ibond_results}")
    assert len(ibond_results) > 0
    assert all(r["bond_type"] == "iBond" for r in ibond_results)

    # 测试 GreenBond 页面
    green_results = scraper.scrape_page(
        "https://www.hkgb.gov.hk/en/greenbond/retail_Rates.html", "GreenBond"
    )
    assert len(green_results) > 0
    assert all(r["bond_type"] == "GreenBond" for r in green_results)

    # 验证数据格式
    for result in ibond_results + green_results:
        assert "symbol" in result
        assert "payment_date" in result
        assert "determination_date" in result
        assert "interest_rate" in result
        assert result["symbol"].isdigit()
        # assert "%" in result["interest_rate"]
