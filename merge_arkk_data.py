#!/usr/bin/env python3
"""
Merge script for ARKK holdings data
- Appends weight column to the LAST column position using YYYYMMDD format
- Extracts date from the first column of holdings CSV (MM/DD/YYYY format)
- Adds new tickers as new rows if not found in constituents file
- Ensures new tickers have 0 in the new date column before merging
"""

import pandas as pd
import re
import glob
import os
from datetime import datetime

def extract_date_from_holdings(holdings_file):
    """Extract date from the first column of holdings CSV file"""
    try:
        # Read just the first data row to get the date
        df = pd.read_csv(holdings_file, nrows=1)
        
        if 'date' not in df.columns:
            print(f"Warning: 'date' column not found in {holdings_file}")
            return None
        
        date_str = df['date'].iloc[0]
        
        # Parse MM/DD/YYYY format
        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        
        # Return in YYYYMMDD format
        return date_obj.strftime('%Y%m%d')
    
    except Exception as e:
        print(f"Error extracting date from {holdings_file}: {e}")
        return None

def merge_holdings_data():
    """Main function to merge ARKK holdings data"""
    
    # Check if arkk_constituents.csv exists
    constituents_file = 'arkk_constituents.csv'
    
    if not os.path.exists(constituents_file):
        print(f"Error: {constituents_file} not found!")
        return False
    
    # Read the main constituents file
    df_constituents = pd.read_csv(constituents_file)
    print(f"Loaded {constituents_file}: {df_constituents.shape}")
    
    # Ensure ticker column exists
    if 'ticker' not in df_constituents.columns:
        print("Error: 'ticker' column not found in constituents file!")
        return False
    
    # Find all arkk_holdings_*.csv files
    holdings_files = glob.glob('arkk_holdings_*.csv')
    
    if not holdings_files:
        print("No arkk_holdings_*.csv files found!")
        return False
    
    print(f"Found {len(holdings_files)} holdings files")
    
    # Track if we made any changes
    changes_made = False
    
    # Process each holdings file
    for holdings_file in sorted(holdings_files):
        # Extract date from the holdings file itself (from first column)
        date_col = extract_date_from_holdings(holdings_file)
        
        if not date_col:
            print(f"Warning: Could not extract date from {holdings_file}, skipping...")
            continue
        
        # Check if this date column already exists
        if date_col in df_constituents.columns:
            print(f"Date column '{date_col}' already exists, skipping {holdings_file}")
            continue
        
        print(f"Processing {holdings_file} -> column '{date_col}'")
        
        # Read the holdings file
        df_holdings = pd.read_csv(holdings_file)
        
        # Verify required columns exist
        if 'ticker' not in df_holdings.columns or 'weight (%)' not in df_holdings.columns:
            print(f"Warning: {holdings_file} missing required columns, skipping...")
            continue
        
        # Get tickers and weights, filtering out NaN tickers
        df_weights = df_holdings[['ticker', 'weight (%)']].copy()
        df_weights = df_weights[df_weights['ticker'].notna()]
        
        # Clean the weight column - remove % sign and convert to float
        df_weights['weight (%)'] = df_weights['weight (%)'].astype(str).str.rstrip('%').astype(float)
        
        df_weights.rename(columns={'weight (%)': date_col}, inplace=True)
        
        # Find new tickers that don't exist in constituents
        existing_tickers = set(df_constituents['ticker'].dropna())
        new_tickers = set(df_weights['ticker']) - existing_tickers
        
        if new_tickers:
            print(f"  Found {len(new_tickers)} new tickers: {sorted(new_tickers)}")
            # Add new tickers as new rows with 0 for all existing columns (except ticker)
            new_rows = []
            for ticker in new_tickers:
                # Create a new row with ticker and 0 for all other columns
                new_row = {'ticker': ticker}
                for col in df_constituents.columns:
                    if col != 'ticker':
                        new_row[col] = 0
                # Initialize the new date column with 0 for this new ticker
                new_row[date_col] = 0
                new_rows.append(new_row)
            
            df_constituents = pd.concat([df_constituents, pd.DataFrame(new_rows)], ignore_index=True)
        
        # Now merge the weights - this will update existing rows and new ones
        # First, set ticker as index for both dataframes
        df_constituents_indexed = df_constituents.set_index('ticker')
        df_weights_indexed = df_weights.set_index('ticker')
        
        # For existing rows that don't have the date column yet, initialize with 0
        if date_col not in df_constituents_indexed.columns:
            df_constituents_indexed[date_col] = 0
        
        # Update with actual weights from the holdings file
        df_constituents_indexed.update(df_weights_indexed)
        
        # Reset index to get ticker back as a column
        df_constituents = df_constituents_indexed.reset_index()
        
        # Report matching stats
        non_zero = (df_constituents[date_col] != 0).sum()
        total = len(df_constituents)
        print(f"  Matched {non_zero}/{total} tickers with non-zero weights ({non_zero/total*100:.1f}%)")
        
        changes_made = True
    
    if changes_made:
        # Save the updated constituents file
        df_constituents.to_csv(constituents_file, index=False)
        print(f"\n✓ Successfully updated {constituents_file}")
        print(f"  New shape: {df_constituents.shape}")
        print(f"  Columns: {list(df_constituents.columns)}")
        return True
    else:
        print("\nNo changes made - all data already merged")
        return False

if __name__ == '__main__':
    try:
        success = merge_holdings_data()
        exit(0 if success else 1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
