#!/usr/bin/env python3
"""
Combined Excel Processor Split - Handles imports.xlsx and local.xlsx files

A processor that combines delivery data from split files:
- imports.xlsx: Contains imported data (Sailling, Landed, For Pull out Return, UNSERVED)
- local.xlsx: Contains local unserved data
- inventory.xlsx: Inventory data
- global_usage.csv: Global usage data for enhanced projections
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional
import logging
import os
from datetime import datetime
import openpyxl

# Import the original CombinedProcessor
from combined_processor import CombinedProcessor

# Set up logging
os.makedirs('logs', exist_ok=True)

# Create a unique log filename with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'logs/combined_processor_split_{timestamp}.log'

# Set up logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='w'),
        logging.StreamHandler()  # This keeps console output
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Logging to file: {log_filename}")

class CombinedProcessorSplit(CombinedProcessor):
    """
    A class to process and combine delivery data from split files (imports.xlsx + local.xlsx),
    inventory data, and global usage data.
    Inherits from CombinedProcessor to reuse all the existing logic.
    """
    
    def __init__(self, imports_file: str, local_file: str, inventory_file: str, global_usage_file: str = None):
        """
        Initialize the Combined processor for split files.
        
        Args:
            imports_file (str): Path to the imports Excel file
            local_file (str): Path to the local Excel file
            inventory_file (str): Path to the inventory Excel file
            global_usage_file (str, optional): Path to the global usage CSV file
        """
        # Store the split file paths
        self.imports_file = imports_file
        self.local_file = local_file
        
        # Initialize the parent class with a dummy delivery file (we'll override the processing)
        super().__init__("dummy_delivery.xlsx", inventory_file, global_usage_file)
        
        # Override the delivery file path for date extraction
        self.delivery_file = imports_file  # Use imports file for date extraction
        
    def _process_delivery_data(self):
        """
        Process all delivery data from split files (imports.xlsx + local.xlsx).
        Override the parent method to handle split files.
        """
        # Process imports data
        self._process_imports_data()
        
        # Process local data
        self._process_local_data()
    
    def _process_imports_data(self):
        """
        Process all imports data sheets.
        """
        # Load imports file
        imports_excel = pd.ExcelFile(self.imports_file)
        logger.info(f"Available imports sheets: {imports_excel.sheet_names}")
        
        # Process imports sheets
        for sheet_name in imports_excel.sheet_names:
            if sheet_name == 'Sailling':
                # Process SAILING sheet
                df = self.process_sailing_sheet(sheet_name)
                if not df.empty:
                    self.delivery_data['SAILING'] = df
            elif sheet_name == 'Landed':
                # Process LANDED sheet
                df = self.process_landed_sheet(sheet_name)
                if not df.empty:
                    self.delivery_data['LANDED'] = df
            elif sheet_name == 'For Pull out Return':
                # Process PULLOUT sheet
                df = self.process_pullout_sheet(sheet_name)
                if not df.empty:
                    self.delivery_data['PULLOUT'] = df
            elif sheet_name == 'UNSERVED':
                # Process UNSERVED IMPORTED sheet
                df = self.process_unserved_imported_sheet(sheet_name)
                if not df.empty:
                    self.delivery_data['UNSERVED IMPORTED'] = df
    
    def _process_local_data(self):
        """
        Process local data from local.xlsx.
        """
        # Load local file
        local_excel = pd.ExcelFile(self.local_file)
        logger.info(f"Available local sheets: {local_excel.sheet_names}")
        
        # Process local sheets (typically just one sheet with local unserved data)
        for sheet_name in local_excel.sheet_names:
            df = self.process_unserved_local_sheet(sheet_name)
            if not df.empty:
                self.delivery_data['UNSERVED LOCAL'] = df
    
    def process_sailing_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process the SAILING sheet from imports.xlsx.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            pd.DataFrame: Processed data
        """
        try:
            logger.info(f"Processing SAILING sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(self.imports_file, sheet_name=sheet_name)
            logger.info(f"Raw SAILING data shape: {df.shape}")
            
            # Find the header row (look for row with 'CONTRACT' or similar)
            header_row = None
            for idx, row in df.iterrows():
                if any('CONTRACT' in str(cell).upper() for cell in row if pd.notna(cell)):
                    header_row = idx
                    break
            
            if header_row is not None:
                # Use the found header row
                df = df.iloc[header_row:].copy()
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                logger.info(f"Found header at row {header_row}")
            else:
                logger.warning("Could not find header row, using first row as header")
            
            # Clean up the data
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            # Add STATUS column
            df['STATUS'] = 'SAILING'
            
            logger.info(f"Processed SAILING data shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing SAILING sheet {sheet_name}: {e}")
            return pd.DataFrame()
    
    def process_landed_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process the LANDED sheet from imports.xlsx.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            pd.DataFrame: Processed data
        """
        try:
            logger.info(f"Processing LANDED sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(self.imports_file, sheet_name=sheet_name)
            logger.info(f"Raw LANDED data shape: {df.shape}")
            
            # Find the header row (look for row with 'BILL OF LADING' or similar)
            header_row = None
            for idx, row in df.iterrows():
                if any('BILL OF LADING' in str(cell).upper() for cell in row if pd.notna(cell)):
                    header_row = idx
                    break
            
            if header_row is not None:
                # Use the found header row
                df = df.iloc[header_row:].copy()
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                logger.info(f"Found header at row {header_row}")
            else:
                logger.warning("Could not find header row, using first row as header")
            
            # Clean up the data
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            # Add STATUS column
            df['STATUS'] = 'LANDED'
            
            logger.info(f"Processed LANDED data shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing LANDED sheet {sheet_name}: {e}")
            return pd.DataFrame()
    
    def process_pullout_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process the PULLOUT sheet from imports.xlsx.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            pd.DataFrame: Processed data
        """
        try:
            logger.info(f"Processing PULLOUT sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(self.imports_file, sheet_name=sheet_name)
            logger.info(f"Raw PULLOUT data shape: {df.shape}")
            
            # Find the header row (look for row with 'BILL OF LADING' or similar)
            header_row = None
            for idx, row in df.iterrows():
                if any('BILL OF LADING' in str(cell).upper() for cell in row if pd.notna(cell)):
                    header_row = idx
                    break
            
            if header_row is not None:
                # Use the found header row
                df = df.iloc[header_row:].copy()
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                logger.info(f"Found header at row {header_row}")
            else:
                logger.warning("Could not find header row, using first row as header")
            
            # Clean up the data
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            # Add STATUS column
            df['STATUS'] = 'PULLOUT'
            
            logger.info(f"Processed PULLOUT data shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing PULLOUT sheet {sheet_name}: {e}")
            return pd.DataFrame()
    
    def process_unserved_imported_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process the UNSERVED IMPORTED sheet from imports.xlsx.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            pd.DataFrame: Processed data
        """
        try:
            logger.info(f"Processing UNSERVED IMPORTED sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(self.imports_file, sheet_name=sheet_name)
            logger.info(f"Raw UNSERVED IMPORTED data shape: {df.shape}")
            
            # The UNSERVED sheet should have proper headers already
            # Clean up the data
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            # Add STATUS column
            df['STATUS'] = 'UNSERVED IMPORTED'
            
            logger.info(f"Processed UNSERVED IMPORTED data shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing UNSERVED IMPORTED sheet {sheet_name}: {e}")
            return pd.DataFrame()
    
    def process_unserved_local_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process the UNSERVED LOCAL sheet from local.xlsx.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            pd.DataFrame: Processed data
        """
        try:
            logger.info(f"Processing UNSERVED LOCAL sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(self.local_file, sheet_name=sheet_name)
            logger.info(f"Raw UNSERVED LOCAL data shape: {df.shape}")
            
            # Find the header row (look for row with 'RAW MATERIALS' or similar)
            header_row = None
            for idx, row in df.iterrows():
                if any('RAW MATERIALS' in str(cell).upper() for cell in row if pd.notna(cell)):
                    header_row = idx
                    break
            
            if header_row is not None:
                # Use the found header row
                df = df.iloc[header_row:].copy()
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                logger.info(f"Found header at row {header_row}")
            else:
                logger.warning("Could not find header row, using first row as header")
            
            # Clean up the data
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            # Add STATUS column
            df['STATUS'] = 'UNSERVED LOCAL'
            
            logger.info(f"Processed UNSERVED LOCAL data shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing UNSERVED LOCAL sheet {sheet_name}: {e}")
            return pd.DataFrame()
    


def main():
    """
    Main function to demonstrate the Combined processor usage.
    """
    import os
    
    # Check if global usage file exists
    global_usage_file = None
    if os.path.exists('input/global_usage.csv'):
        global_usage_file = 'input/global_usage.csv'
        print("🌍 Global usage file found - will create enhanced pivot table with projections")
    else:
        print("📊 Running in standard mode (no global usage file)")
    
    # Initialize the processor
    processor = CombinedProcessorSplit(
        'input/imports.xlsx', 
        'input/local.xlsx', 
        'input/inventory.xlsx', 
        global_usage_file
    )
    
    # Process all data
    results = processor.process_all_data()
    
    # Ensure global usage data is loaded if available
    if global_usage_file:
        processor._load_global_usage_data()
        print(f"🌍 Global usage data loaded: {len(processor.global_usage_data.get('global_usage', pd.DataFrame()))} ingredients")
    
    # Print summary
    summary = processor.get_summary()
    print("\n=== COMBINED PROCESSING SUMMARY ===")
    for sheet_name, info in summary.items():
        print(f"\n{sheet_name.upper()}:")
        print(f"  Rows: {info['rows']}")
        print(f"  Columns: {info['columns']}")
        if info['column_names']:
            print(f"  Columns: {', '.join(info['column_names'])}")
    
    # Save processed data
    processor.save_processed_data()
    
    # Combine all data
    combined_df = processor.combine_all_data()
    
    # Save combined data
    if not combined_df.empty:
        combined_file = os.path.join(processor.output_dir, "COMBINED_ALL_DATA.csv")
        combined_df.to_csv(combined_file, index=False)
        print(f"✅ Saved combined data to {combined_file}")
        print(f"📊 Total rows: {len(combined_df)}")
    
    print(f"\n🎉 Processing complete!")
    print(f"📁 Check your results in: {processor.output_dir}")


if __name__ == "__main__":
    main()