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

logger = get_dagster_logger()


class YahooFinanceResource(ConfigurableResource):
    """Yahoo Finance API 资源"""

    batch_size: int = 50
    max_workers: int = 10

    def get_equity_shares(self, symbols: List[str], dt: datetime) -> List[EquityShare]:
        """获取股本数据"""
        results = []
        # 分批处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i : i + self.batch_size]
                futures.append(
                    executor.submit(self._get_batch_equity_shares, batch, dt)
                )

            for future in futures:
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                except Exception as e:
                    logger.error(f"Error getting Yahoo Finance data: {e}")

        return results

    def _get_batch_equity_shares(
        self, symbols: List[str], dt: datetime
    ) -> List[EquityShare]:
        """获取一批股票的股本数据"""
        # TODO: 实现 Yahoo Finance API 调用
        # 这里需要替换成实际的 Yahoo Finance API 实现
        return []


class NasdaqScreenerResource(ConfigurableResource):
    """Nasdaq Screener 爬虫资源"""

    def get_equity_shares(self, dt: datetime) -> List[EquityShare]:
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

    def _get_screener_data(self) -> str:
        """获取 Nasdaq Screener 数据"""
        url = "https://api.nasdaq.com/api/screener/stocks"
        params = {"tableonly": "true", "limit": 25, "offset": 0, "download": "true"}
        headers = {
            "Authority": "api.nasdaq.com",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Origin": "https://www.nasdaq.com",
            "Referer": "https://www.nasdaq.com/",
        }

        with httpx.Client(verify=False) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.text


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
