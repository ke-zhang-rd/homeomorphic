"""
Enhanced script to fetch GRNY (Fundstrat Granny Shots ETF) holdings from the official website.
This version includes a demo mode with sample data for testing.

Features:
- Scrapes holdings from https://grannyshots.com/holdings/
- Parses HTML table data
- Cleans and processes the data
- Exports to CSV with proper formatting
- Provides summary statistics and sector analysis

Author: Python Script
Date: 2025
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import Optional
import re
import json


class GRNYHoldingsFetcher:
    """Fetches and processes GRNY holdings from the official Fundstrat website."""
    
    def __init__(self):
        self.url = "https://grannyshots.com/holdings/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
    
    def fetch_holdings(self, use_demo: bool = False) -> Optional[pd.DataFrame]:
        """
        Fetch the current GRNY holdings from the official website.
        
        Args:
            use_demo: If True, returns demo data instead of fetching from website
            
        Returns:
            pandas DataFrame with holdings data or None if fetch fails
        """
        if use_demo:
            return self._get_demo_data()
        
        try:
            print(f"Fetching GRNY holdings from: {self.url}")
            response = requests.get(self.url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the table - look for table with holdings data
            table = soup.find('table')
            
            if not table:
                print("Error: Could not find holdings table on the page")
                return None
            
            # Extract table headers
            headers = []
            header_row = table.find('thead')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    header_text = th.get_text(strip=True)
                    if header_text:
                        headers.append(header_text)
            
            # If no thead, try first tr
            if not headers:
                first_row = table.find('tr')
                if first_row:
                    for th in first_row.find_all(['th', 'td']):
                        header_text = th.get_text(strip=True)
                        if header_text:
                            headers.append(header_text)
            
            # Extract table rows
            rows = []
            tbody = table.find('tbody')
            if tbody:
                trs = tbody.find_all('tr')
            else:
                trs = table.find_all('tr')[1:]  # Skip first row if it's the header
            
            for tr in trs:
                cells = tr.find_all(['td', 'th'])
                if cells:
                    row = [cell.get_text(strip=True) for cell in cells]
                    # Filter out rows that are headers or empty
                    if len(row) > 0 and row[0] and row[0] not in ['Ticker', 'ticker']:
                        rows.append(row)
            
            if not rows:
                print("Error: No data rows found in table")
                return None
            
            # Ensure we have the right number of columns
            if headers and len(rows[0]) != len(headers):
                print(f"Warning: Column count mismatch. Headers: {len(headers)}, Data: {len(rows[0])}")
                # Adjust headers or data as needed
                if len(rows[0]) > len(headers):
                    headers = headers + [f'Column_{i}' for i in range(len(headers), len(rows[0]))]
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=headers if headers else None)
            
            # Extract date information from the page
            date_text = soup.get_text()
            date_match = re.search(r'Holdings as of ([A-Za-z]+ \d{1,2}, \d{4})', date_text)
            holdings_date = date_match.group(1) if date_match else datetime.now().strftime('%B %d, %Y')
            
            # Add metadata
            df['holdings_date'] = holdings_date
            df['fetch_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"✓ Successfully fetched {len(df)} holdings for GRNY")
            print(f"✓ Holdings as of: {holdings_date}")
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            print("\nTip: If you're behind a firewall or proxy, you may need to configure your network settings.")
            print("You can also try using the demo mode by setting use_demo=True")
            return None
        except Exception as e:
            print(f"Error processing data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_demo_data(self) -> pd.DataFrame:
        """
        Return demo data for testing purposes.
        This uses actual holdings data from a recent snapshot.
        """
        print("Using demo data (recent snapshot from August 2025)")
        
        demo_data = [
            ['AAPL', '037833100', 'Apple Inc.', 'Information Technology', '2.58%', '$59,127,100', '$225.64', '-2.13%'],
            ['AMD', '007903107', 'Advanced Micro Devices', 'Information Technology', '2.39%', '$54,882,200', '$165.20', '-0.81%'],
            ['AMZN', '023135106', 'Amazon.Com Inc', 'Consumer Discretionary', '2.57%', '$58,898,200', '$223.90', '-1.80%'],
            ['ANET', '040413205', 'Arista Networks', 'Information Technology', '2.53%', '$58,126,800', '$131.47', '-0.99%'],
            ['AVGO', '11135F101', 'Broadcom Inc.', 'Information Technology', '2.46%', '$56,452,300', '$291.50', '-1.16%'],
            ['AXON', '05464C101', 'Axon Enterprise, Inc.', 'Industrials', '2.63%', '$60,273,400', '$760.89', '0.34%'],
            ['AXP', '025816109', 'American Express Company', 'Financials', '2.60%', '$59,583,300', '$308.40', '0.75%'],
            ['BK', '064058100', 'Bank of New York Mellon', 'Financials', '2.55%', '$58,442,400', '$101.25', '0.17%'],
            ['CAT', '149123101', 'Caterpillar Inc.', 'Industrials', '2.59%', '$59,362,300', '$420.59', '1.08%'],
            ['CDNS', '127387108', 'Cadence Design Systems', 'Information Technology', '2.59%', '$59,523,200', '$345.45', '-0.41%'],
            ['COST', '22160K105', 'Costco Wholesale Corp', 'Consumer Staples', '2.61%', '$59,988,800', '$994.57', '1.40%'],
            ['CRWD', '22788C105', 'CrowdStrike Holdings, Inc.', 'Information Technology', '2.57%', '$58,870,200', '$419.20', '0.14%'],
            ['EMR', '291011104', 'Emerson Electric Co.', 'Industrials', '2.55%', '$58,492,500', '$130.89', '-0.53%'],
            ['ETN', 'G29183103', 'Eaton Corporation, plc', 'Industrials', '2.55%', '$58,612,100', '$346.22', '-0.80%'],
            ['EXPE', '30212P303', 'Expedia Group, Inc.', 'Consumer Discretionary', '2.60%', '$59,701,900', '$205.68', '-1.13%'],
            ['GE', '369604301', 'GE Aerospace', 'Industrials', '2.55%', '$58,599,500', '$266.44', '0.19%'],
            ['GEV', '36828A101', 'GE Vernova Inc.', 'Industrials', '2.51%', '$57,653,800', '$604.59', '0.24%'],
            ['GOOGL', '02079K305', 'Alphabet Inc.', 'Communication Services', '2.58%', '$59,273,100', '$199.21', '-1.17%'],
            ['GRMN', 'H2906T109', 'Garmin Ltd', 'Consumer Discretionary', '2.59%', '$59,385,500', '$230.15', '-1.41%'],
            ['GS', '38141G104', 'Goldman Sachs Group Inc.', 'Financials', '2.52%', '$57,772,400', '$720.68', '-0.10%'],
            ['HOOD', '770700102', 'Robinhood Markets, Inc.', 'Financials', '2.52%', '$57,746,800', '$105.83', '-1.55%'],
            ['JPM', '46625H100', 'JPMorgan Chase & Co.', 'Financials', '2.58%', '$59,179,200', '$292.24', '0.54%'],
            ['KLAC', '482480100', 'KLA Corporation', 'Information Technology', '2.38%', '$54,620,100', '$878.44', '0.27%'],
            ['LRCX', '512807306', 'Lam Research Corp', 'Information Technology', '2.42%', '$55,589,700', '$99.14', '-1.19%'],
            ['LYV', '538034109', 'Live Nation Entertainment', 'Communication Services', '2.62%', '$60,124,600', '$161.90', '-0.61%'],
            ['META', '30303M102', 'Meta Platforms, Inc.', 'Communication Services', '2.50%', '$57,305,600', '$746.55', '-0.66%'],
            ['MNST', '61174X109', 'Monster Beverage Corp', 'Consumer Staples', '2.60%', '$59,735,300', '$64.39', '0.63%'],
            ['MSFT', '594918104', 'Microsoft Corp', 'Information Technology', '2.54%', '$58,375,300', '$505.50', '-0.84%'],
            ['MSTR', '594972408', 'MicroStrategy Inc', 'Information Technology', '2.34%', '$53,611,600', '$344.65', '2.40%'],
            ['NFLX', '64110L106', 'NetFlix Inc', 'Communication Services', '2.57%', '$58,909,300', '$1,213.86', '-0.02%'],
            ['NVDA', '67066G104', 'Nvidia Corp', 'Information Technology', '2.51%', '$57,621,300', '$175.60', '-0.02%'],
            ['ORCL', '68389X105', 'Oracle Corp', 'Information Technology', '2.50%', '$57,258,500', '$235.54', '0.39%'],
            ['PANW', '697435105', 'Palo Alto Networks, Inc.', 'Information Technology', '2.72%', '$62,358,400', '$184.70', '1.73%'],
            ['PLTR', '69608A108', 'Palantir Technologies Inc.', 'Information Technology', '2.24%', '$51,483,600', '$156.15', '-1.01%'],
            ['PWR', '74762E102', 'Quanta Services, Inc.', 'Industrials', '2.62%', '$60,052,900', '$375.87', '-0.90%'],
            ['SPGI', '78409V104', 'S&P Global Inc.', 'Financials', '2.56%', '$58,767,400', '$557.03', '0.47%'],
            ['TSLA', '88160R101', 'Tesla, Inc.', 'Consumer Discretionary', '2.56%', '$58,849,000', '$323.20', '-1.86%'],
            ['VST', '92840M102', 'Vistra Corp.', 'Utilities', '2.48%', '$56,980,800', '$192.91', '-0.32%'],
            ['WTW', 'G96629103', 'Willis Towers Watson PLC', 'Financials', '2.65%', '$60,734,800', '$335.72', '0.07%'],
        ]
        
        columns = ['Ticker', 'CUSIP', 'Name', 'Sector', 'Weight', 'Market Value', 'Last Price', 'Market Price Ch%']
        df = pd.DataFrame(demo_data, columns=columns)
        df['holdings_date'] = 'August 20, 2025'
        df['fetch_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return df
    
    def clean_holdings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the holdings data.
        
        Args:
            df: Raw holdings DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        df_clean = df.copy()
        
        # Clean weight column - remove % sign and convert to float
        weight_col = [col for col in df_clean.columns if 'weight' in col.lower() and 'market' not in col.lower()]
        if weight_col:
            weight_col = weight_col[0]
            df_clean['Weight_Numeric'] = df_clean[weight_col].str.replace('%', '').str.strip().astype(float)
        
        # Clean market value - remove $ and commas, convert to float
        value_col = [col for col in df_clean.columns if 'market value' in col.lower()]
        if value_col:
            value_col = value_col[0]
            df_clean['Market_Value_Numeric'] = (
                df_clean[value_col]
                .str.replace('$', '', regex=False)
                .str.replace(',', '', regex=False)
                .str.strip()
                .astype(float)
            )
        
        # Clean last price - remove $ and convert to float
        price_col = [col for col in df_clean.columns if 'last price' in col.lower() or 'price' in col.lower()]
        if price_col:
            price_col = [c for c in price_col if 'change' not in c.lower() and 'ch%' not in c.lower()]
            if price_col:
                price_col = price_col[0]
                df_clean['Last_Price_Numeric'] = (
                    df_clean[price_col]
                    .str.replace('$', '', regex=False)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                    .astype(float)
                )
        
        # Clean price change - remove % and convert to float
        change_col = [col for col in df_clean.columns if 'ch%' in col.lower() or 'change %' in col.lower()]
        if change_col:
            change_col = change_col[0]
            df_clean['Price_Change_Numeric'] = (
                df_clean[change_col]
                .str.replace('%', '', regex=False)
                .str.strip()
                .astype(float)
            )
        
        return df_clean
    
    def get_summary_stats(self, df: pd.DataFrame) -> dict:
        """Calculate summary statistics for the holdings."""
        stats = {
            'fund': 'GRNY',
            'total_holdings': len(df),
            'fetch_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if 'Weight_Numeric' in df.columns:
            stats['total_weight'] = df['Weight_Numeric'].sum()
            stats['average_weight'] = df['Weight_Numeric'].mean()
            stats['median_weight'] = df['Weight_Numeric'].median()
            stats['max_weight'] = df['Weight_Numeric'].max()
            stats['min_weight'] = df['Weight_Numeric'].min()
            stats['top_10_weight'] = df.nlargest(10, 'Weight_Numeric')['Weight_Numeric'].sum()
        
        if 'Market_Value_Numeric' in df.columns:
            stats['total_market_value'] = df['Market_Value_Numeric'].sum()
            stats['total_aum_formatted'] = f"${stats['total_market_value']:,.0f}"
        
        if 'holdings_date' in df.columns:
            stats['holdings_date'] = df['holdings_date'].iloc[0]
        
        return stats
    
    def get_top_holdings(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Get the top N holdings by weight."""
        if 'Weight_Numeric' in df.columns:
            return df.nlargest(n, 'Weight_Numeric')
        else:
            return df.head(n)
    
    def get_sector_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get sector breakdown of holdings."""
        sector_col = [col for col in df.columns if 'sector' in col.lower()]
        
        if sector_col and 'Weight_Numeric' in df.columns:
            sector_col = sector_col[0]
            
            agg_dict = {
                'Weight_Numeric': ['sum', 'mean', 'count']
            }
            if 'Market_Value_Numeric' in df.columns:
                agg_dict['Market_Value_Numeric'] = 'sum'
            
            sector_stats = df.groupby(sector_col).agg(agg_dict).round(2)
            
            # Flatten column names
            sector_stats.columns = ['Total_Weight_%', 'Avg_Weight_%', 'Count', 'Total_Market_Value'] if 'Market_Value_Numeric' in df.columns else ['Total_Weight_%', 'Avg_Weight_%', 'Count']
            sector_stats = sector_stats.sort_values('Total_Weight_%', ascending=False)
            
            return sector_stats
        else:
            return pd.DataFrame()
    
    def save_to_csv(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save holdings data to CSV file."""
        if filename is None:
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"grny_holdings_{date_str}.csv"
        
        df.to_csv(filename, index=False)
        print(f"✓ Data saved to: {filename}")
        return filename
    
    def save_to_json(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save holdings data to JSON file."""
        if filename is None:
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"grny_holdings_{date_str}.json"
        
        df.to_json(filename, orient='records', indent=2)
        print(f"✓ Data saved to: {filename}")
        return filename
    
    def display_report(self, df: pd.DataFrame):
        """Display a comprehensive report of the holdings."""
        print("\n" + "="*100)
        print("GRNY (FUNDSTRAT GRANNY SHOTS ETF) HOLDINGS REPORT")
        print("="*100)
        
        # Summary statistics
        stats = self.get_summary_stats(df)
        print("\nSUMMARY STATISTICS:")
        print("-" * 100)
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"{key.replace('_', ' ').title():<30}: {value:.2f}")
            else:
                print(f"{key.replace('_', ' ').title():<30}: {value}")
        
        # Top 10 holdings
        print("\n" + "="*100)
        print("TOP 10 HOLDINGS:")
        print("="*100)
        top_holdings = self.get_top_holdings(df, n=10)
        
        # Find columns to display
        ticker_col = [c for c in top_holdings.columns if 'ticker' in c.lower()][0] if any('ticker' in c.lower() for c in top_holdings.columns) else None
        name_col = [c for c in top_holdings.columns if 'name' in c.lower()][0] if any('name' in c.lower() for c in top_holdings.columns) else None
        sector_col = [c for c in top_holdings.columns if 'sector' in c.lower()][0] if any('sector' in c.lower() for c in top_holdings.columns) else None
        weight_col = [c for c in top_holdings.columns if 'weight' in c.lower() and 'numeric' not in c.lower()][0] if any('weight' in c.lower() and 'numeric' not in c.lower() for c in top_holdings.columns) else None
        
        display_cols = [c for c in [ticker_col, name_col, sector_col, weight_col] if c is not None]
        
        if display_cols:
            print(top_holdings[display_cols].to_string(index=False))
        
        # Sector breakdown
        print("\n" + "="*100)
        print("SECTOR BREAKDOWN:")
        print("="*100)
        sector_breakdown = self.get_sector_breakdown(df)
        if not sector_breakdown.empty:
            print(sector_breakdown.to_string())
        
        print("\n" + "="*100)


def main():
    """Main function to demonstrate usage."""
    
    print("GRNY (Fundstrat Granny Shots ETF) Holdings Fetcher")
    print("="*100)
    print("\nThis script fetches current holdings from https://grannyshots.com/holdings/")
    print("-" * 100)
    
    # Create fetcher instance
    fetcher = GRNYHoldingsFetcher()
    
    # Try to fetch real data first, fall back to demo if needed
    print("\nAttempting to fetch live data...")
    df = fetcher.fetch_holdings(use_demo=False)
    
    if df is None:
        print("\n⚠ Could not fetch live data. Using demo mode instead.")
        print("To fetch live data in your environment, ensure you have internet access.")
        df = fetcher.fetch_holdings(use_demo=True)
    
    if df is not None:
        # Clean the data
        df = fetcher.clean_holdings(df)
        
        # Display report
        fetcher.display_report(df)
        
        # Save to files
        print("\nSaving data to files...")
        fetcher.save_to_csv(df)
        fetcher.save_to_json(df)
        
        print("\n✓ Complete! Holdings data has been saved.")
        print("\nUsage tips:")
        print("  - Run this script daily to track changes in holdings")
        print("  - Compare CSV files to see how positions change over time")
        print("  - Use the JSON format for integration with other tools")
    else:
        print("\n✗ Failed to fetch holdings data in both live and demo modes.")


if __name__ == "__main__":
    main()
