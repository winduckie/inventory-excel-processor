#!/usr/bin/env python3
"""
Combined Excel Processor

A processor that combines delivery and inventory data from multiple Excel files.
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional
import logging
import os
from datetime import datetime
import openpyxl

# Set up logging
os.makedirs('logs', exist_ok=True)

# Create a unique log filename with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'logs/combined_processor_{timestamp}.log'

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

class CombinedProcessor:
    """
    A class to process and combine delivery and inventory data.
    """
    
    def __init__(self, delivery_file: str, inventory_file: str):
        """
        Initialize the Combined processor.
        
        Args:
            delivery_file (str): Path to the delivery Excel file
            inventory_file (str): Path to the inventory Excel file
        """
        self.delivery_file = delivery_file
        self.inventory_file = inventory_file
        self.delivery_data = {}
        self.inventory_data = {}
        self.column_mapping = self._load_column_mapping()
        
    def _load_column_mapping(self) -> Dict:
        """
        Load the column mapping from JSON file.
        
        Returns:
            Dict: Column mapping dictionary
        """
        try:
            with open('config/table_column_mapping.json', 'r') as f:
                mapping = json.load(f)
                return mapping['table_column_mapping']
        except FileNotFoundError:
            logger.error("config/table_column_mapping.json not found")
            return {}
        except Exception as e:
            logger.error(f"Error loading column mapping: {e}")
            return {}
    
    def _load_product_categories(self) -> Dict[str, str]:
        """
        Load product categories from CSV file.
        
        Returns:
            Dict[str, str]: Product to category mapping
        """
        try:
            import csv
            categories = {}
            with open('config/product_categories.csv', 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    product = row['Product'].strip()
                    category = row['Category'].strip()
                    categories[product] = category
            logger.info(f"Loaded {len(categories)} product categories")
            return categories
        except FileNotFoundError:
            logger.warning("config/product_categories.csv not found, creating new file")
            return {}
        except Exception as e:
            logger.error(f"Error loading product categories: {e}")
            return {}
    
    def _save_product_categories(self, categories: Dict[str, str]):
        """
        Save product categories to CSV file.
        
        Args:
            categories (Dict[str, str]): Product to category mapping
        """
        try:
            import csv
            with open('config/product_categories.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Product', 'Category'])
                for product, category in sorted(categories.items()):
                    writer.writerow([product, category])
            logger.info(f"Saved {len(categories)} product categories")
        except Exception as e:
            logger.error(f"Error saving product categories: {e}")
    
    def _validate_required_columns(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Validate that required product and quantity columns exist in the table.
        
        Args:
            df (pd.DataFrame): The dataframe to validate
            table_name (str): Name of the table for validation
            
        Returns:
            bool: True if all required columns exist, False otherwise
        """
        if table_name not in self.column_mapping:
            logger.warning(f"No column mapping found for table: {table_name}")
            return True  # Skip validation if no mapping exists
            
        required_columns = self.column_mapping[table_name]
        product_col = required_columns['product_column']
        quantity_col = required_columns['quantity_column']
        
        missing_columns = []
        if product_col not in df.columns:
            missing_columns.append(product_col)
        if quantity_col not in df.columns:
            missing_columns.append(quantity_col)
            
        if missing_columns:
            error_msg = f"Missing required columns for {table_name}: {missing_columns}. Available columns: {list(df.columns)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.info(f"✅ Validated required columns for {table_name}: {product_col}, {quantity_col}")
        return True
    
    def _set_table_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Programmatically find the best header row and set it as column names.
        
        Args:
            df (pd.DataFrame): The dataframe to process
            
        Returns:
            pd.DataFrame: Processed dataframe with proper headers
        """
        if df.empty:
            return df
            
        # Look for the best header row in the first 10 rows
        best_header_row = -1
        max_meaningful_headers = 0
        
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            # Count non-empty, non-numeric values that could be headers
            meaningful_count = 0
            for val in row:
                if pd.notna(val) and str(val).strip() != '':
                    val_str = str(val).strip()
                    # Check if it looks like a header (not numeric, not too long, not a data value)
                    if (not val_str.replace('.', '').replace('-', '').isdigit() and 
                        len(val_str) < 50 and
                        not val_str.replace('.', '').replace(',', '').isdigit() and
                        not any(char.isdigit() for char in val_str[:3]) and  # Avoid values that start with numbers
                        not val_str.isdigit()):  # Avoid pure numeric values
                        meaningful_count += 1
            
            if meaningful_count > max_meaningful_headers:
                max_meaningful_headers = meaningful_count
                best_header_row = i
        
        if best_header_row >= 0 and max_meaningful_headers >= 2:
            # Use this row as headers and remove all rows above it
            headers = df.iloc[best_header_row]
            # Clean header names
            clean_headers = []
            for i, header in enumerate(headers):
                if pd.notna(header) and str(header).strip() != '':
                    clean_headers.append(str(header).strip())
                else:
                    clean_headers.append(f'Column_{i}')
            
            # Create new dataframe starting from the row after the header
            new_df = df.iloc[best_header_row + 1:].copy()
            new_df.columns = clean_headers
            new_df = new_df.reset_index(drop=True)
            
            logger.info(f"Found best header row at index {best_header_row} with {max_meaningful_headers} meaningful headers, removed {best_header_row + 1} rows above")
            return new_df
        else:
            logger.warning("Could not find suitable header row, using default column names")
            return df
    
    def _handle_merged_cells(self, df: pd.DataFrame, sheet_name: str = None) -> pd.DataFrame:
        """
        Handle merged cells by propagating values.
        For UNSERVED LOCAL, only fill RAW MATERIALS cells that are part of a merged cell in Excel.
        """
        if df.empty:
            return df
        
        df_processed = df.copy()
        
        # Only apply merged cell handling to specific columns that need it
        # For UNSERVED LOCAL, only handle RAW MATERIALS column
        if sheet_name == 'UNSERVED LOCAL' and 'RAW MATERIALS' in df_processed.columns:
            col = 'RAW MATERIALS'
            # Use openpyxl to detect merged cells
            try:
                wb = openpyxl.load_workbook(self.delivery_file, data_only=True)
                ws = wb['UNSERVED LOCAL']
                merged_ranges = ws.merged_cells.ranges
                # Find the column index for RAW MATERIALS in the DataFrame
                header_row_idx = None
                for i in range(1, 11):  # search first 10 rows for header
                    if ws.cell(row=i, column=1).value and str(ws.cell(row=i, column=1).value).strip().upper() == 'RAW MATERIALS':
                        header_row_idx = i
                        break
                if header_row_idx is None:
                    logger.warning('Could not find RAW MATERIALS header row in openpyxl sheet')
                    return df_processed
                # Data starts after header
                data_start_row = header_row_idx + 1
                # Map DataFrame index to Excel row
                excel_row_for_df = lambda df_idx: data_start_row + df_idx
                # For each cell in RAW MATERIALS, only fill if part of a merged cell
                filled_values = []
                for df_idx, value in enumerate(df_processed[col]):
                    excel_row = excel_row_for_df(df_idx)
                    cell = ws.cell(row=excel_row, column=1)  # RAW MATERIALS is col 1
                    is_merged = any([cell.coordinate in rng for rng in merged_ranges])
                    if is_merged:
                        # Find the top-left cell of the merged range
                        for rng in merged_ranges:
                            if cell.coordinate in rng:
                                top_left = ws.cell(row=rng.min_row, column=rng.min_col)
                                filled_values.append(top_left.value)
                                break
                    else:
                        filled_values.append(value)
                df_processed[col] = filled_values
                # Clear RAW MATERIALS for summary rows where SUPPLIER is empty
                if 'SUPPLIER' in df_processed.columns:
                    supplier_empty_mask = df_processed['SUPPLIER'].isna() | (df_processed['SUPPLIER'] == '')
                    df_processed.loc[supplier_empty_mask, col] = ''
            except Exception as e:
                logger.warning(f'openpyxl merged cell handling failed: {e}')
        logger.info("Applied selective merged cell handling (openpyxl for UNSERVED LOCAL)")
        return df_processed
    
    def _clean_data_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data formatting (remove .0 from integers, etc.).
        
        Args:
            df (pd.DataFrame): The dataframe to clean
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        if df.empty:
            return df
        
        for col in df.columns:
            if df[col].dtype == 'float64':
                # Check if all values are whole numbers
                if df[col].dropna().apply(lambda x: x.is_integer()).all():
                    df[col] = df[col].astype('Int64')
                else:
                    # Remove trailing .0 for display
                    df[col] = df[col].astype(str).str.replace('.0', '', regex=False)
        
        logger.info("Applied data formatting cleanup")
        return df
    
    def _clean_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the table by removing empty rows and columns.
        
        Args:
            df (pd.DataFrame): The dataframe to clean
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        if df.empty:
            return df
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')
        
        # Fill NaN values with empty strings for string columns
        for col in df.columns:
            if hasattr(df[col], 'dtype'):
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
        
        return df
    
    def process_delivery_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process a delivery sheet with standard processing.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            pd.DataFrame: Processed data
        """
        try:
            logger.info(f"Processing {sheet_name} sheet")
            
            # Read the sheet
            df = pd.read_excel(self.delivery_file, sheet_name=sheet_name)
            logger.info(f"Loaded {sheet_name} sheet with {len(df)} rows")
            
            # Set proper headers
            df = self._set_table_headers(df)
            
            # Handle merged cells
            df = self._handle_merged_cells(df, sheet_name)
            
            # Clean data formatting
            df = self._clean_data_formatting(df)
            
            # Clean the table
            df = self._clean_table(df)
            
            logger.info(f"Processed {sheet_name} sheet: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error processing {sheet_name} sheet: {e}")
            return pd.DataFrame()
    
    def process_landed_pullout_sheet(self, sheet_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process a sheet that contains both LANDED and PULL data and split into two tables.
        
        Args:
            sheet_name (str): Name of the sheet to process
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Landed and Pullout tables
        """
        try:
            logger.info(f"Processing {sheet_name} sheet (detected as LANDED/PULLOUT combined)")
            
            # Read the sheet
            df = pd.read_excel(self.delivery_file, sheet_name=sheet_name)
            logger.info(f"Loaded {sheet_name} sheet with {len(df)} rows")
            
            # Find the split points
            landed_start = -1
            pullout_start = -1
            
            # Search for LANDED and PULL headers in all columns of each row
            for idx, row in df.iterrows():
                # Check all columns in the row for headers
                for col_idx, cell_val in enumerate(row):
                    if pd.notna(cell_val):
                        cell_str = str(cell_val).strip().upper()
                        
                        # Look for LANDED header (more flexible pattern)
                        if 'LANDED' in cell_str and ('HEADER' in cell_str or 'TABLE' in cell_str or col_idx == 0):
                            landed_start = idx + 1  # Start below the header
                            logger.info(f"Found 'LANDED' header at row {idx}, column {col_idx}, table starts at row {landed_start}")
                            break
                        
                        # Look for PULL header (more flexible pattern)
                        elif ('PULL' in cell_str or 'PULLOUT' in cell_str) and ('HEADER' in cell_str or 'TABLE' in cell_str or col_idx == 0):
                            pullout_start = idx + 1  # Start below the header
                            logger.info(f"Found 'PULL' header at row {idx}, column {col_idx}, table starts at row {pullout_start}")
                            break
                
                # If we found both headers, we can stop searching
                if landed_start >= 0 and pullout_start >= 0:
                    break
            
            # If no explicit LANDED header found, assume LANDED data starts from the beginning
            if landed_start == -1 and pullout_start > 0:
                landed_start = 0  # Start from the beginning
                logger.info(f"No explicit LANDED header found, assuming LANDED data starts from row 0")
            
            # Extract the tables
            if landed_start >= 0 and pullout_start > landed_start:
                # For LANDED: include all rows between headers
                landed_df = df.iloc[landed_start:pullout_start-1].copy()
                
                # For PULLOUT: include all rows after header
                pullout_df = df.iloc[pullout_start:].copy()
                
                logger.info(f"LANDED data: rows {landed_start} to {pullout_start-2}")
                logger.info(f"PULLOUT data: rows {pullout_start} to {len(df)-1}")
            else:
                logger.warning(f"Could not find proper split points for {sheet_name}")
                return pd.DataFrame(), pd.DataFrame()
            
            # Process each table
            landed_df = self._set_table_headers(landed_df)
            landed_df = self._handle_merged_cells(landed_df, 'LANDED')
            landed_df = self._clean_data_formatting(landed_df)
            landed_df = self._clean_table(landed_df)
            
            pullout_df = self._set_table_headers(pullout_df)
            pullout_df = self._handle_merged_cells(pullout_df, 'PULLOUT')
            pullout_df = self._clean_data_formatting(pullout_df)
            pullout_df = self._clean_table(pullout_df)
            
            # Validate columns
            self._validate_required_columns(landed_df, 'LANDED')
            self._validate_required_columns(pullout_df, 'PULLOUT')
            
            logger.info(f"Processed LANDED table: {len(landed_df)} rows")
            logger.info(f"Processed PULLOUT table: {len(pullout_df)} rows")
            
            return landed_df, pullout_df
            
        except Exception as e:
            logger.error(f"Error processing {sheet_name} sheet: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def process_inventory_sheet(self) -> pd.DataFrame:
        """
        Process the inventory sheet.
        
        Returns:
            pd.DataFrame: Processed inventory data
        """
        try:
            logger.info("Processing inventory sheet")
            
            # Read the inventory file
            df = pd.read_excel(self.inventory_file)
            logger.info(f"Loaded inventory sheet with {len(df)} rows")
            
            # Find the header row (look for "ITEM DESCRIPTION" and "QTY IN KGS")
            header_row = -1
            for i in range(min(25, len(df))):
                row_values = [str(val).strip() if pd.notna(val) else '' for val in df.iloc[i]]
                if 'ITEM DESCRIPTION' in row_values and 'QTY IN KGS' in row_values:
                    header_row = i
                    break
            
            if header_row == -1:
                logger.warning("Could not find inventory header row")
                return pd.DataFrame()
            
            logger.info(f"Found header row at index {header_row}")
            
            # Extract headers
            headers = df.iloc[header_row]
            clean_headers = []
            for i, header in enumerate(headers):
                if pd.notna(header) and str(header).strip() != '':
                    clean_headers.append(str(header).strip())
                else:
                    clean_headers.append(f'Column_{i}')
            
            # Start data from the row after header
            data_df = df.iloc[header_row + 1:].copy()
            data_df.columns = clean_headers
            
            # Clean the data
            data_df = data_df.dropna(how='all')  # Remove completely empty rows
            
            # Extract only the relevant columns (ITEM DESCRIPTION and QTY IN KGS)
            relevant_columns = []
            for col in data_df.columns:
                if 'ITEM DESCRIPTION' in col or 'QTY IN KGS' in col or 'DESCRIPTION' in col or 'QTY' in col:
                    relevant_columns.append(col)
            
            if relevant_columns:
                data_df = data_df[relevant_columns]
            
            # Clean up the data
            for col in data_df.columns:
                if data_df[col].dtype == 'object':
                    data_df[col] = data_df[col].fillna('')
            
            # Remove rows that are completely empty or contain only formatting
            data_df = data_df[data_df.apply(lambda row: row.astype(str).str.strip().ne('').any(), axis=1)]
            
            logger.info(f"Processed inventory sheet: {len(data_df)} rows, {len(data_df.columns)} columns")
            return data_df
            
        except Exception as e:
            logger.error(f"Error processing inventory sheet: {e}")
            return pd.DataFrame()
    
    def process_all_data(self) -> Dict[str, pd.DataFrame]:
        """
        Process all delivery and inventory data.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of processed dataframes
        """
        # Load delivery file
        delivery_excel = pd.ExcelFile(self.delivery_file)
        logger.info(f"Available delivery sheets: {delivery_excel.sheet_names}")
        
        results = {}
        
        # Process delivery sheets
        for sheet_name in delivery_excel.sheet_names:
            if sheet_name == 'SUMMARY':
                continue  # Skip SUMMARY sheet
            elif 'LANDED' in sheet_name.upper() and 'PULL' in sheet_name.upper():
                # Auto-detect and split LANDED/PULLOUT combined sheets
                landed_df, pullout_df = self.process_landed_pullout_sheet(sheet_name)
                if not landed_df.empty:
                    results['LANDED'] = landed_df
                if not pullout_df.empty:
                    results['PULLOUT'] = pullout_df
            else:
                # Standard processing for other sheets
                df = self.process_delivery_sheet(sheet_name)
                if not df.empty:
                    # Validate columns if mapping exists
                    if sheet_name in self.column_mapping:
                        self._validate_required_columns(df, sheet_name)
                    results[sheet_name] = df
        
        # Process inventory data
        inventory_df = self.process_inventory_sheet()
        if not inventory_df.empty:
            results['INVENTORY'] = inventory_df
        
        self.delivery_data = {k: v for k, v in results.items() if k != 'INVENTORY'}
        self.inventory_data = {'INVENTORY': inventory_df} if not inventory_df.empty else {}
        
        return results
    
    def combine_all_data(self) -> pd.DataFrame:
        """
        Combine all tables (except SUMMARY) including inventory data.
        
        Returns:
            pd.DataFrame: Combined dataframe with all data
        """
        combined_data = []
        
        # Load product categories
        categories = self._load_product_categories()
        new_products = set()
        blank_categories = []
        
        # Process delivery data
        for table_name, df in self.delivery_data.items():
            if df.empty:
                continue
                
            # Get the required columns for this table
            # Handle table name variations (spaces vs underscores)
            mapping_key = table_name
            if table_name not in self.column_mapping:
                # Try with underscores
                mapping_key = table_name.replace(' ', '_')
                if mapping_key not in self.column_mapping:
                    logger.warning(f"No column mapping found for table: {table_name} or {mapping_key}")
                    continue
            
            required_columns = self.column_mapping[mapping_key]
            product_col = required_columns['product_column']
            quantity_col = required_columns['quantity_column']
            
            # Check if required columns exist
            if product_col not in df.columns or quantity_col not in df.columns:
                logger.warning(f"Missing required columns for {table_name}: {product_col}, {quantity_col}")
                continue
            
            # Extract product and quantity data
            table_data = df[[product_col, quantity_col]].copy()
            
            # Add status column
            table_data['STATUS'] = table_name.replace('_', ' ')
            
            # Convert UNSERVED LOCAL quantity from kilos to tons (divide by 1000)
            # Use fuzzy matching to handle different variations (UNSERVED LOCAL, UNSERVED_LOCAL, etc.)
            if 'UNSERVED' in table_name.upper() and 'LOCAL' in table_name.upper():
                # Convert to numeric, handle errors, then divide by 1000 to convert kilos to tons
                table_data[quantity_col] = pd.to_numeric(table_data[quantity_col], errors='coerce')
                table_data[quantity_col] = table_data[quantity_col] / 1000
                logger.info(f"Converted {table_name} quantities from kilos to tons (divided by 1000)")
            
            # Rename columns to standard names
            table_data = table_data.rename(columns={
                product_col: 'PRODUCT',
                quantity_col: 'QUANTITY'
            })
            
            # Remove rows where product is empty
            table_data = table_data.dropna(subset=['PRODUCT'])
            table_data = table_data[table_data['PRODUCT'] != '']
            
            # Filter out summary rows only
            # Remove rows where product is empty or contains only whitespace
            table_data = table_data[table_data['PRODUCT'].str.strip() != '']
            
            logger.info(f"Filtered {table_name} data: removed empty product rows")
            
            # Add category column and track new/blank categories
            table_data['CATEGORY'] = ''
            for idx, row in table_data.iterrows():
                product = row['PRODUCT'].strip()
                
                # Check if product exists in categories
                if product in categories:
                    category = categories[product]
                    table_data.at[idx, 'CATEGORY'] = category
                    
                    # Track blank categories
                    if not category:
                        blank_categories.append(product)
                else:
                    # New product found
                    new_products.add(product)
                    table_data.at[idx, 'CATEGORY'] = ''
            
            combined_data.append(table_data)
            logger.info(f"Added {len(table_data)} rows from {table_name}")
        
        # Process inventory data
        if 'INVENTORY' in self.inventory_data:
            inventory_df = self.inventory_data['INVENTORY']
            if not inventory_df.empty:
                # Rename inventory columns to match format
                inventory_data = inventory_df.copy()
                inventory_data = inventory_data.rename(columns={
                    'ITEM DESCRIPTION': 'PRODUCT',
                    'QTY IN KGS': 'QUANTITY'
                })
                
                # Divide inventory quantity by 1000
                inventory_data['QUANTITY'] = pd.to_numeric(inventory_data['QUANTITY'], errors='coerce')
                inventory_data['QUANTITY'] = inventory_data['QUANTITY'] / 1000
                logger.info(f"Divided INVENTORY quantities by 1000")
                
                # Add status column
                inventory_data['STATUS'] = 'INVENTORY'
                
                # Remove rows where product is empty
                inventory_data = inventory_data.dropna(subset=['PRODUCT'])
                inventory_data = inventory_data[inventory_data['PRODUCT'] != '']
                
                # Add category column and track new/blank categories
                inventory_data['CATEGORY'] = ''
                for idx, row in inventory_data.iterrows():
                    product = row['PRODUCT'].strip()
                    
                    # Check if product exists in categories
                    if product in categories:
                        category = categories[product]
                        inventory_data.at[idx, 'CATEGORY'] = category
                        
                        # Track blank categories
                        if not category:
                            blank_categories.append(product)
                    else:
                        # New product found
                        new_products.add(product)
                        inventory_data.at[idx, 'CATEGORY'] = ''
                
                combined_data.append(inventory_data)
                logger.info(f"Added {len(inventory_data)} rows from INVENTORY")
        
        if combined_data:
            # Combine all dataframes
            final_df = pd.concat(combined_data, ignore_index=True)
            logger.info(f"Combined data: {len(final_df)} total rows from {len(combined_data)} tables")
            
            # Handle new products and warnings
            if new_products:
                logger.info(f"Found {len(new_products)} new products, adding to categories file")
                for product in new_products:
                    categories[product] = ''
                self._save_product_categories(categories)
                
                logger.warning(f"NEW PRODUCTS ADDED TO product_categories.csv: {sorted(new_products)}")
                print(f"\n⚠️  NEW PRODUCTS ADDED TO product_categories.csv:")
                for product in sorted(new_products):
                    print(f"  - {product}")
                print("Please edit product_categories.csv to add categories for these products.")
            
            if blank_categories:
                unique_blank = list(set(blank_categories))
                logger.warning(f"Found products with blank categories: {sorted(unique_blank)}")
                print(f"\n⚠️  WARNING: Found products with blank categories:")
                for product in sorted(unique_blank):
                    print(f"  - {product}")
                print("Please edit product_categories.csv to add categories for these products.")
            
            return final_df
        else:
            logger.warning("No data to combine")
            return pd.DataFrame(columns=['PRODUCT', 'QUANTITY', 'STATUS', 'CATEGORY'])
    
    def save_processed_data(self, output_dir: str = "processed_data"):
        """
        Save all processed data to CSV files.
        
        Args:
            output_dir (str): Directory to save the processed data
        """
        import os
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save delivery data
        for sheet_name, df in self.delivery_data.items():
            if not df.empty:
                output_file = os.path.join(output_dir, f"{sheet_name}.csv")
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {sheet_name} to {output_file}")
        
        # Save inventory data
        for sheet_name, df in self.inventory_data.items():
            if not df.empty:
                output_file = os.path.join(output_dir, "inventory.csv")
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {sheet_name} to {output_file}")
    
    def get_summary(self) -> Dict[str, dict]:
        """
        Get a summary of all processed data.
        
        Returns:
            Dict[str, dict]: Summary information for each table
        """
        summary = {}
        
        # Delivery data summary
        for sheet_name, df in self.delivery_data.items():
            if not df.empty:
                summary[sheet_name] = {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': list(df.columns),
                    'data_types': df.dtypes.to_dict(),
                    'missing_values': df.isnull().sum().to_dict()
                }
            else:
                summary[sheet_name] = {
                    'rows': 0,
                    'columns': 0,
                    'column_names': [],
                    'data_types': {},
                    'missing_values': {}
                }
        
        # Inventory data summary
        for sheet_name, df in self.inventory_data.items():
            if not df.empty:
                summary[sheet_name] = {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': list(df.columns),
                    'data_types': df.dtypes.to_dict(),
                    'missing_values': df.isnull().sum().to_dict()
                }
            else:
                summary[sheet_name] = {
                    'rows': 0,
                    'columns': 0,
                    'column_names': [],
                    'data_types': {},
                    'missing_values': {}
                }
        
        return summary

    def create_pivot_table(self, combined_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Create a pivot table with Category in rows, Status in columns, and sum of quantities as values.
        
        Args:
            combined_df (pd.DataFrame, optional): Combined dataframe. If None, will use self.combine_all_data()
            
        Returns:
            pd.DataFrame: Pivot table
        """
        if combined_df is None:
            combined_df = self.combine_all_data()
        
        if combined_df.empty:
            logger.warning("No data available for pivot table")
            return pd.DataFrame()
        
        # Ensure QUANTITY is numeric
        combined_df['QUANTITY'] = pd.to_numeric(combined_df['QUANTITY'], errors='coerce')
        
        # Fill NaN values in CATEGORY with 'Uncategorized'
        combined_df['CATEGORY'] = combined_df['CATEGORY'].fillna('Uncategorized')
        combined_df['CATEGORY'] = combined_df['CATEGORY'].replace('', 'Uncategorized')
        
        # Create pivot table
        try:
            pivot_table = pd.pivot_table(
                combined_df,
                values='QUANTITY',
                index='CATEGORY',
                columns='STATUS',
                aggfunc='sum',
                fill_value=0
            )
            
            # Add empty TOTAL column (will be filled with formulas in Excel)
            pivot_table['TOTAL'] = 0
            
            # Add empty TOTAL row (will be filled with formulas in Excel)
            pivot_table.loc['TOTAL'] = 0
            
            logger.info(f"Created pivot table with shape: {pivot_table.shape}")
            logger.info(f"Categories: {list(pivot_table.index[:-1])}")  # Exclude 'TOTAL' row
            logger.info(f"Statuses: {list(pivot_table.columns[:-1])}")  # Exclude 'TOTAL' column
            
            return pivot_table
            
        except Exception as e:
            logger.error(f"Error creating pivot table: {e}")
            return pd.DataFrame()
    
    def save_pivot_table(self, pivot_df: pd.DataFrame, output_dir: str = "processed_data"):
        """
        Save the pivot table to CSV and Excel files.
        
        Args:
            pivot_df (pd.DataFrame): Pivot table to save
            output_dir (str): Directory to save the pivot table
        """
        if pivot_df.empty:
            logger.warning("No pivot table to save")
            return
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to CSV
        csv_file = os.path.join(output_dir, "PIVOT_TABLE.csv")
        pivot_df.to_csv(csv_file)
        logger.info(f"Saved pivot table to {csv_file}")
        
        # Save to Excel with formatting
        excel_file = os.path.join(output_dir, "PIVOT_TABLE.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            pivot_df.to_excel(writer, sheet_name='Pivot Table')
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Pivot Table']
            
            # Format the worksheet
            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
            from openpyxl.utils import get_column_letter
            
            # Define styles
            header_font = Font(bold=True, color="FFFFFF")
            header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            total_font = Font(bold=True)
            total_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
            border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            
            # Format headers
            for col in range(1, worksheet.max_column + 1):
                cell = worksheet.cell(row=1, column=col)
                cell.font = header_font
                cell.fill = header_fill
                cell.border = border
                cell.alignment = Alignment(horizontal='center')
            
            # Format row headers (categories)
            for row in range(2, worksheet.max_row + 1):
                cell = worksheet.cell(row=row, column=1)
                cell.font = Font(bold=True)
                cell.border = border
                
                # Highlight total row
                if cell.value == 'TOTAL':
                    for col in range(1, worksheet.max_column + 1):
                        total_cell = worksheet.cell(row=row, column=col)
                        total_cell.font = total_font
                        total_cell.fill = total_fill
                        total_cell.border = border
            
            # Format all data cells and replace zeros with dashes
            for row in range(2, worksheet.max_row + 1):
                for col in range(2, worksheet.max_column + 1):
                    cell = worksheet.cell(row=row, column=col)
                    cell.border = border
                    cell.alignment = Alignment(horizontal='right')
                    
                    # Replace zero values with dash for display
                    if cell.value == 0 or cell.value == 0.0:
                        cell.value = '-'
                        cell.font = Font(color="808080")  # Gray color for dashes
                    elif isinstance(cell.value, (int, float)) and cell.value > 0:
                        # Format all numbers with comma separation and 0 decimal places
                        cell.number_format = '#,##0'
            
            # Add SUM formulas for totals
            # Row totals (sum across columns for each category)
            for row in range(2, worksheet.max_row):  # Exclude the TOTAL row
                # Calculate the range for this row (from column B to the second-to-last column)
                start_col = get_column_letter(2)  # Column B
                end_col = get_column_letter(worksheet.max_column - 1)  # Second-to-last column
                row_num = row
                
                # Create SUM formula for row total
                sum_formula = f"=SUM({start_col}{row_num}:{end_col}{row_num})"
                total_cell = worksheet.cell(row=row, column=worksheet.max_column)
                total_cell.value = sum_formula
                total_cell.number_format = '#,##0'
                total_cell.font = Font(bold=True)
                total_cell.border = border
                total_cell.alignment = Alignment(horizontal='right')
            
            # Column totals (sum down rows for each status)
            for col in range(2, worksheet.max_column):  # Exclude the TOTAL column
                # Calculate the range for this column (from row 2 to the second-to-last row)
                col_letter = get_column_letter(col)
                start_row = 2
                end_row = worksheet.max_row - 1  # Second-to-last row
                
                # Create SUM formula for column total
                sum_formula = f"=SUM({col_letter}{start_row}:{col_letter}{end_row})"
                total_cell = worksheet.cell(row=worksheet.max_row, column=col)
                total_cell.value = sum_formula
                total_cell.number_format = '#,##0'
                total_cell.font = total_font
                total_cell.fill = total_fill
                total_cell.border = border
                total_cell.alignment = Alignment(horizontal='right')
            
            # Grand total (sum of all data cells)
            start_col = get_column_letter(2)  # Column B
            end_col = get_column_letter(worksheet.max_column - 1)  # Second-to-last column
            start_row = 2
            end_row = worksheet.max_row - 1  # Second-to-last row
            
            grand_total_formula = f"=SUM({start_col}{start_row}:{end_col}{end_row})"
            grand_total_cell = worksheet.cell(row=worksheet.max_row, column=worksheet.max_column)
            grand_total_cell.value = grand_total_formula
            grand_total_cell.number_format = '#,##0'
            grand_total_cell.font = total_font
            grand_total_cell.fill = total_fill
            grand_total_cell.border = border
            grand_total_cell.alignment = Alignment(horizontal='right')
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"Saved formatted pivot table to {excel_file}")


def main():
    """
    Main function to demonstrate the Combined processor usage.
    """
    import os
    
    # Initialize the processor
    processor = CombinedProcessor('input/deliveries.xlsx', 'input/inventory.xlsx')
    
    # Process all data
    results = processor.process_all_data()
    
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
        combined_file = os.path.join("processed_data", "COMBINED_ALL_DATA.csv")
        combined_df.to_csv(combined_file, index=False)
        logger.info(f"Saved combined data to {combined_file}")
        print(f"\n=== COMBINED DATA SUMMARY ===")
        print(f"Total rows: {len(combined_df)}")
        print(f"Status breakdown: {combined_df['STATUS'].value_counts().to_dict()}")
        print(f"Category breakdown: {combined_df['CATEGORY'].value_counts().to_dict()}")
        print(f"Sample data:")
        print(combined_df.head(10).to_string(index=False))
        
        # Create and save pivot table
        print(f"\n=== CREATING PIVOT TABLE ===")
        pivot_table = processor.create_pivot_table(combined_df)
        
        if not pivot_table.empty:
            processor.save_pivot_table(pivot_table)
            print(f"✅ Pivot table created successfully!")
            print(f"Shape: {pivot_table.shape}")
            print(f"Categories: {len(pivot_table.index) - 1}")  # Exclude TOTAL row
            print(f"Statuses: {len(pivot_table.columns) - 1}")  # Exclude TOTAL column
            print(f"\nPivot Table Preview:")
            print(pivot_table.to_string())
        else:
            print("❌ Failed to create pivot table")
    
    print(f"\n=== INDIVIDUAL DATA SAMPLES ===")
    for sheet_name, df in results.items():
        if not df.empty:
            print(f"\n{sheet_name}:")
            print(f"Shape: {df.shape}")
            print("First 5 rows:")
            print(df.head().to_string(index=False))
            print("\n" + "="*50)


if __name__ == "__main__":
    main() 