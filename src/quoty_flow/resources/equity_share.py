import asyncio
from dagster import ConfigurableResource, get_dagster_logger
import httpx
import json
from typing import List
from datetime import datetime
from decimal import Decimal
import io
import pandas as pd
from ..models.equity_share import EquityShare
from concurrent.futures import ThreadPoolExecutor
import pytz
import yfinance as yf

logger = get_dagster_logger()


class YahooFinanceResource(ConfigurableResource):
    """Yahoo Finance API 资源"""

    batch_size: int = 100
    max_workers: int = 2

    def get_equity_shares(self, symbols: pd.Series, dt: str) -> pd.DataFrame:
        """获取股本数据"""
        results = pd.DataFrame()
        # 分批处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            # 使用 Series 的 iloc 进行切片
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols.iloc[i : i + self.batch_size]
                futures.append(
                    executor.submit(self._get_batch_equity_shares, batch, dt)
                )

            for future in futures:
                try:
                    batch_results = future.result()
                    results = pd.concat([results, batch_results], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error getting Yahoo Finance data: {e}")

        return results

    def _get_batch_equity_shares(self, symbols: pd.Series, dt: str) -> pd.DataFrame:
        """获取一批股票的股本数据"""
        try:
            # Get data for batch of symbols
            tickers = yf.Tickers(",".join(symbols))

            # 创建空的 DataFrame，预先定义好列
            df = pd.DataFrame(columns=["symbol", "shares", "source", "dt"])

            for symbol, ticker in tickers.tickers.items():
                try:
                    info = ticker.fast_info
                    # 创建单行数据
                    row = pd.DataFrame(
                        [
                            {
                                "symbol": symbol,
                                "shares": info.shares,
                                "source": "yahoo",
                                "dt": dt,
                            }
                        ]
                    )
                    # 追加到主 DataFrame
                    df = pd.concat([df, row], ignore_index=True)
                except Exception as e:
                    logger.warning(f"Error getting data for {symbol}: {e}")
                    continue

            if df.empty:
                return pd.DataFrame(columns=["symbol", "shares", "source", "dt"])

            return df

        except Exception as e:
            logger.error(f"Error in batch Yahoo Finance request: {e}")
            return pd.DataFrame()


class NasdaqScreenerResource(ConfigurableResource):
    """Nasdaq Screener 爬虫资源"""

    def get_equity_shares(self) -> List[EquityShare]:
        """获取股本数据"""
        try:
            # 获取原始数据
            data = self._get_screener_data()

            # 解析数据
            response = json.loads(data)
            items = response.get("data", {}).get("rows", [])

            results = []
            for item in items:
                try:
                    last_price = float(item["lastsale"].replace("$", ""))
                    market_cap = float(item["marketCap"])
                    total_shares = (
                        Decimal(str(int(market_cap / last_price)))
                        if last_price > 0
                        else Decimal("0")
                    )

                    results.append(
                        EquityShare(
                            symbol=item["symbol"],
                            total_shares=total_shares,
                            source="nasdaq",
                            dt=dt,
                        )
                    )
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error processing Nasdaq item: {e}")
                    continue

            return results
        except Exception as e:
            logger.error(f"Error getting Nasdaq data: {e}")
            raise

    async def _get_screener_data(
        self, limit: int = 100, offset: int = 0, client: httpx.AsyncClient = None
    ) -> pd.DataFrame:
        """获取 Nasdaq Screener 数据

        Args:
            limit: 每页数量，默认25
            offset: 偏移量，默认0

        Returns:
            str: JSON 格式的响应数据
        """
        url = "https://api.nasdaq.com/api/screener/stocks"
        params = {
            "tableonly": "true",
            "limit": limit,
            "offset": offset,
            "download": "true",
        }
        headers = {
            "Authority": "api.nasdaq.com",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Origin": "https://www.nasdaq.com",
            "Referer": "https://www.nasdaq.com/",
        }

        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        # 直接转换为 DataFrame
        return pd.DataFrame(data.get("data", {}).get("rows", []))

    def get_all_screener_data(self, page_size: int = 100) -> pd.DataFrame:
        """获取所有 Nasdaq Screener 数据

        Args:
            page_size: 每页数量，默认25

        Returns:
            pd.DataFrame: 所有股票数据表格
        """

        async def _fetch_all():
            all_dfs = []
            offset = 0

            async with httpx.AsyncClient(verify=False) as client:
                while True:
                    try:
                        df = await self._get_screener_data(
                            limit=page_size, offset=offset, client=client
                        )

                        if df.empty:
                            break
                        if len(df) < page_size:
                            break

                        all_dfs.append(df)
                        offset += page_size

                        # 可选：添加日志
                        logger.info(
                            f"Retrieved {len(df)} rows, total: {sum(len(df) for df in all_dfs)}"
                        )

                    except Exception as e:
                        logger.error(f"Error fetching page at offset {offset}: {e}")
                        break

            # 合并所有 DataFrame
            if not all_dfs:
                return pd.DataFrame()

            return pd.concat(all_dfs, ignore_index=True)

        return asyncio.run(_fetch_all())


class OrbisfnResource(ConfigurableResource):
    """Orbisfn EOD 数据资源"""

    def get_equity_shares(self) -> pd.DataFrame:
        try:
            # 获取原始数据
            data = self._get_eod_data()
            # 转换成 utf-string 之后去掉第一个 #
            data = data.decode("utf-8").lstrip("#")

            # 直接返回 DataFrame
            df = pd.read_csv(io.StringIO(data))
            df["dt"] = get_us_eastern_date()
            return df

        except Exception as e:
            logger.error(f"Error getting Orbisfn data: {e}")
            raise

    def _get_eod_data(self) -> bytes:
        """获取 Orbisfn EOD 数据"""
        url = "https://services.orbisfn.com/eod/USA.txt"
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.content


def get_us_eastern_date() -> str:
    """获取美东时间的日期"""
    eastern = pytz.timezone("US/Eastern")
    return datetime.now(eastern).strftime("%Y%m%d")
