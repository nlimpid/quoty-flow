from dagster import ConfigurableResource
from bs4 import BeautifulSoup
import httpx
import re
from typing import List, Dict, Any
from datetime import datetime


class HKEXScraperResource(ConfigurableResource):
    def get_symbol_from_title(self, text: str) -> str:
        match = re.search(r'Stock Code: (\d+)', text)
        return match.group(1) if match else ""

    def normalize_date(self, date_str: str) -> str:
        return date_str.strip().rstrip('*')

    def scrape_page(self, url: str, bond_type: str) -> List[Dict[str, Any]]:
        """爬取单个页面的数据"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        results = []
        with httpx.Client(verify=False, http2=True) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 获取所有债券代码
            symbols = []
            for h6 in soup.find_all('h6'):
                symbol = self.get_symbol_from_title(h6.text)
                if symbol:
                    symbols.append(symbol)

            if not symbols:
                print(f"No symbols found in {url}")
                return results

            # 处理表格
            tables = soup.find_all('table')
            for table_idx, table in enumerate(tables):
                if len(table.text) < 100:  # 跳过小表格
                    continue

                # 确保 table_idx 在 symbols 范围内
                if table_idx >= len(symbols):
                    print(
                        f"Table index {table_idx} exceeds symbols length {len(symbols)}")
                    continue

                rows = table.find_all('tr')[1:]  # 跳过表头
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        try:
                            results.append({
                                'symbol': symbols[table_idx],
                                'payment_date': self.normalize_date(cells[0].text),
                                'determination_date': self.normalize_date(cells[1].text),
                                'interest_rate': self.normalize_date(cells[2].text),
                                'bond_type': bond_type
                            })
                        except Exception as e:
                            print(f"Error processing row: {e}")
                            continue

            print(f"Scraped {len(results)} records from {url}")
            return results

    def get_bond_data(self) -> List[Dict[str, Any]]:
        """获取所有债券数据"""
        ibond_data = self.scrape_page(
            "https://www.hkgb.gov.hk/en/retail/iBond_Rates.html",
            "iBond"
        )
        print(ibond_data)
        greenbond_data = self.scrape_page(
            "https://www.hkgb.gov.hk/en/greenbond/retail_Rates.html",
            "GreenBond"
        )
        return ibond_data + greenbond_data
