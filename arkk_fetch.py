"""
Script to fetch ARKK (ARK Innovation ETF) holdings and weights from the official ARK Invest website.
The data is downloaded as a CSV file from ARK's official repository.
"""

import pandas as pd
import requests
from datetime import datetime
from typing import Optional

class ARKKHoldingsFetcher:
    """Fetches and processes ARKK holdings data from ARK Invest."""
    
    def __init__(self):
        self.base_url = "https://assets.ark-funds.com/fund-documents/funds-etf-csv/"
        self.arkk_filename = "ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"
        # self.arkk_filename = "ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def fetch_holdings(self) -> Optional[pd.DataFrame]:
        """
        Fetch the current ARKK holdings from ARK Invest website.
        
        Returns:
            pandas DataFrame with holdings data or None if fetch fails
        """
        url = f"{self.base_url}{self.arkk_filename}"
        
        try:
            print(f"Fetching ARKK holdings from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Read CSV from response content
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            print(f"Successfully fetched {len(df)} holdings")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except Exception as e:
            print(f"Error processing data: {e}")
            return None
    
    def process_holdings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and clean the holdings data.
        
        Args:
            df: Raw holdings DataFrame
            
        Returns:
            Processed DataFrame with relevant columns
        """
        # Common columns in ARK CSV files
        # The exact column names may vary, so we'll handle them flexibly
        
        # Print available columns for debugging
        print("\nAvailable columns:")
        print(df.columns.tolist())
        
        return df
    
    def get_top_holdings(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """
        Get the top N holdings by weight.
        
        Args:
            df: Holdings DataFrame
            n: Number of top holdings to return
            
        Returns:
            DataFrame with top N holdings
        """
        # Try to find weight column (could be 'weight(%)', 'weight (%)', etc.)
        weight_cols = [col for col in df.columns if 'weight' in col.lower()]
        
        if weight_cols:
            weight_col = weight_cols[0]
            df_sorted = df.sort_values(by=weight_col, ascending=False)
            return df_sorted.head(n)
        else:
            print("Warning: Weight column not found, returning first N rows")
            return df.head(n)
    
    def save_to_csv(self, df: pd.DataFrame, filename: Optional[str] = None):
        """
        Save holdings data to CSV file.
        
        Args:
            df: Holdings DataFrame
            filename: Output filename (default: arkk_holdings_YYYYMMDD.csv)
        """
        if filename is None:
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"arkk_holdings_{date_str}.csv"
        
        df.to_csv(filename, index=False)
        print(f"\nData saved to: {filename}")


def main():
    """Main function to demonstrate usage."""
    
    # Create fetcher instance
    fetcher = ARKKHoldingsFetcher()
    
    # Fetch holdings
    holdings_df = fetcher.fetch_holdings()
    
    if holdings_df is not None:
        # Process holdings
        processed_df = fetcher.process_holdings(holdings_df)
        
        # Display full dataframe info
        print(f"\nTotal Holdings: {len(processed_df)}")
        print("\nDataFrame Info:")
        print(processed_df.info())
        
        # Display top 10 holdings
        print("\n" + "="*80)
        print("TOP 10 HOLDINGS:")
        print("="*80)
        top_holdings = fetcher.get_top_holdings(processed_df, n=10)
        print(top_holdings.to_string())
        
        # Save to CSV
        fetcher.save_to_csv(processed_df)
        
    else:
        print("Failed to fetch holdings data")


if __name__ == "__main__":
    main()
