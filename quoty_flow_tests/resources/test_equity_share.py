import json
import pytest
from src.quoty_flow.resources.equity_share import (
    YahooFinanceResource,
    NasdaqScreenerResource,
    OrbisfnResource,
    get_us_eastern_date,
)
from src.quoty_flow.models.equity_share import EquityShare
import logging
from rich import print as rprint
from rich.table import Table
from rich.console import Console

console = Console()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def print_share_data(title: str, data: list[EquityShare]):
    """使用 rich 打印股本数据"""
    table = Table(title=title)
    table.add_column("Symbol", style="cyan")
    table.add_column("Total Shares", style="magenta")
    table.add_column("Source", style="green")
    table.add_column("Date", style="yellow")

    for item in data:
        table.add_row(
            item.symbol,
            f"{float(item.total_shares):,.0f}",
            item.source,
            item.dt.strftime("%Y-%m-%d"),
        )

    console.print(table)


@pytest.fixture
def yahoo_finance():
    return YahooFinanceResource(batch_size=50, max_workers=10)


@pytest.fixture
def nasdaq_screener():
    return NasdaqScreenerResource()


@pytest.fixture
def orbisfn():
    return OrbisfnResource()


@pytest.fixture
def current_dt():
    return get_us_eastern_date()


def test_yahoo_finance_get_shares(yahoo_finance, current_dt):
    """测试 Yahoo Finance 数据获取"""
    console.rule("[bold red]Testing Yahoo Finance")

    # 使用一些知名股票作为测试
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    console.print(f"[bold cyan]Testing symbols:[/] {', '.join(symbols)}")

    results = yahoo_finance.get_equity_shares(symbols, current_dt)

    # 验证结果
    assert len(results) > 0, "应该返回至少一条数据"
    for item in results:
        assert isinstance(item, EquityShare), "结果应该是 EquityShare 类型"
        assert item.symbol in symbols, f"股票代码 {item.symbol} 应该在请求列表中"
        assert item.total_shares > 0, "总股本应该大于0"
        assert item.source == "yahoo", "数据来源应该是 yahoo"

    print_share_data("Yahoo Finance Results", results)


def test_nasdaq_screener_get_shares(nasdaq_screener, current_dt):
    """测试 Nasdaq Screener 数据获取"""
    console.rule("[bold red]Testing Nasdaq Screener")

    results = nasdaq_screener.get_equity_shares()

    # 验证结果
    assert len(results) > 0, "应该返回至少一条数据"
    for item in results:
        assert isinstance(item, EquityShare), "结果应该是 EquityShare 类型"
        assert len(item.symbol) > 0, "股票代码不应该为空"
        assert item.total_shares > 0, "总股本应该大于0"
        assert item.source == "nasdaq", "数据来源应该是 nasdaq"

    print_share_data("Nasdaq Screener Results", results[:10])  # 只显示前10条
    console.print(f"[bold green]Total records:[/] {len(results)}")


def test_orbisfn_get_shares(orbisfn, current_dt):
    """测试 Orbisfn 数据获取"""
    console.rule("[bold red]Testing Orbisfn")

    results = orbisfn.get_equity_shares(current_dt)

    logger.info(f"Orbisfn Results: {results.head()}")

    # 验证结果
    assert len(results) > 0, "应该返回至少一条数据"


def test_get_us_eastern_date():
    """测试美东时间获取"""
    console.rule("[bold red]Testing US Eastern Date")

    dt = get_us_eastern_date()
    console.print(f"[bold green]Current US Eastern Date:[/] {dt}")


def test_data_consistency(yahoo_finance, nasdaq_screener, orbisfn, current_dt):
    """测试不同数据源的数据一致性"""
    console.rule("[bold red]Testing Data Consistency")

    # 获取 Nasdaq 数据
    console.print("[bold cyan]Fetching Nasdaq data...")
    nasdaq_data = {
        item.symbol: item.total_shares
        for item in nasdaq_screener.get_equity_shares(current_dt)
    }

    # 获取 Yahoo 数据（仅获取 Nasdaq 中有的股票）
    console.print("[bold cyan]Fetching Yahoo Finance data...")
    yahoo_data = {
        item.symbol: item.total_shares
        for item in yahoo_finance.get_equity_shares(
            list(nasdaq_data.keys())[:5], current_dt
        )
    }

    # 获取 Orbisfn 数据
    console.print("[bold cyan]Fetching Orbisfn data...")
    orbisfn_data = {
        item.symbol: item.total_shares for item in orbisfn.get_equity_shares(current_dt)
    }

    # 找出所有数据源都有的股票
    common_symbols = (
        set(nasdaq_data.keys()) & set(yahoo_data.keys()) & set(orbisfn_data.keys())
    )

    if common_symbols:
        # 创建比较表格
        table = Table(title="Data Comparison")
        table.add_column("Symbol", style="cyan")
        table.add_column("Nasdaq Shares", style="magenta")
        table.add_column("Yahoo Shares", style="green")
        table.add_column("Orbisfn Shares", style="yellow")
        table.add_column("Difference %", style="red")

        # 计算差异
        for symbol in common_symbols:
            nasdaq_shares = float(nasdaq_data[symbol])
            yahoo_shares = float(yahoo_data[symbol])
            orbisfn_shares = float(orbisfn_data[symbol])

            # 计算最大差异百分比
            max_value = max(nasdaq_shares, yahoo_shares, orbisfn_shares)
            min_value = min(nasdaq_shares, yahoo_shares, orbisfn_shares)
            diff_percentage = (max_value - min_value) / min_value * 100

            table.add_row(
                symbol,
                f"{nasdaq_shares:,.0f}",
                f"{yahoo_shares:,.0f}",
                f"{orbisfn_shares:,.0f}",
                f"{diff_percentage:.2f}%",
            )

            # 差异不应该超过50%（这是一个相对宽松的标准，可以根据实际情况调整）
            # assert diff_percentage < 50, (
            #     f"股票 {symbol} 的数据差异过大: {diff_percentage:.2f}%"
            # )

        console.print(table)


def test_get_screener_data():
    resource = NasdaqScreenerResource()

    # 测试单页获取
    response = resource._get_screener_data(limit=5, offset=0)
    # response = json.loads(data)
    logger.info(f"Screener Data: {response['data']['rows'][0]}")

    assert "data" in response
    assert "rows" in response["data"]
    assert len(response["data"]["rows"]) >= 5


def test_get_all_screener_data():
    resource = NasdaqScreenerResource()

    # 测试多页获取
    all_data = resource.get_all_screener_data(page_size=100)

    assert isinstance(all_data, list)
    assert len(all_data) > 0
    logger.info(f"All Data: {all_data.shape()}")

    # 检查数据结构
    first_item = all_data[0]
    assert "symbol" in first_item
    assert "lastsale" in first_item
    assert "marketCap" in first_item
