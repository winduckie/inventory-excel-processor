import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional
import logging

# Set up logging
import os
from datetime import datetime

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Create a unique log filename with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'logs/excel_processor_{timestamp}.log'

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

class ExcelProcessor:
    """
    A class to process Excel files with multiple sheets and tables.
    """
    
    def __init__(self, file_path: str):
        """
        Initialize the Excel processor.
        
        Args:
            file_path (str): Path to the Excel file
        """
        self.file_path = file_path
        self.excel_file = None
        self.sheets_data = {}
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
        
    def load_excel_file(self) -> bool:
        """
        Load the Excel file and get sheet information.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.excel_file = pd.ExcelFile(self.file_path)
            logger.info(f"Successfully loaded Excel file: {self.file_path}")
            logger.info(f"Available sheets: {self.excel_file.sheet_names}")
            return True
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            return False
    
    def process_summary_sheet(self) -> pd.DataFrame:
        """
        Process the SUMMARY sheet.
        
        Returns:
            pd.DataFrame: Processed summary data
        """
        try:
            df = pd.read_excel(self.file_path, sheet_name='SUMMARY')
            logger.info(f"Processing SUMMARY sheet with {len(df)} rows")
            
            # Set proper headers
            df = self._set_table_headers(df)
            
            # Basic cleaning
            df = self._clean_table(df)
            
            # Store processed data
            self.sheets_data['SUMMARY'] = df
            return df
            
        except Exception as e:
            logger.error(f"Error processing SUMMARY sheet: {e}")
            return pd.DataFrame()
    
    def process_sailing_sheet(self) -> pd.DataFrame:
        """
        Process the SAILING sheet.
        
        Returns:
            pd.DataFrame: Processed sailing data
        """
        try:
            df = pd.read_excel(self.file_path, sheet_name='SAILING')
            logger.info(f"Processing SAILING sheet with {len(df)} rows")
            
            # Set proper headers
            df = self._set_table_headers(df)
            
            # Validate required columns
            self._validate_required_columns(df, 'SAILING')
            
            # Basic cleaning
            df = self._clean_table(df)
            
            # Store processed data
            self.sheets_data['SAILING'] = df
            return df
            
        except Exception as e:
            logger.error(f"Error processing SAILING sheet: {e}")
            return pd.DataFrame()
    
    def _validate_table_headers(self, df: pd.DataFrame, start_row: int) -> bool:
        """
        Validate that a row contains valid table headers (minimum 2 non-empty values).
        
        Args:
            df (pd.DataFrame): The dataframe
            start_row (int): The row index to check
            
        Returns:
            bool: True if the row contains valid headers, False otherwise
        """
        if start_row >= len(df):
            return False
            
        row = df.iloc[start_row]
        non_empty_count = row.notna().sum()
        
        # Check if we have at least 2 non-empty values in the row
        if non_empty_count >= 2:
            # Additional check: make sure the values look like headers (not all numbers)
            text_values = 0
            for value in row:
                if pd.notna(value):
                    str_value = str(value).strip()
                    if str_value and not str_value.replace('.', '').replace('-', '').isdigit():
                        text_values += 1
            
            return text_values >= 2
        
        return False
    
    def process_landed_pullout_sheet(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process the LANDED.PULL-OUT sheet which contains two separate tables.
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Two separate dataframes for the two tables
        """
        try:
            # Read the entire LANDED.PULL-OUT sheet
            df = pd.read_excel(self.file_path, sheet_name='LANDED.PULL-OUT')
            logger.info(f"Processing LANDED.PULL-OUT sheet with {len(df)} rows")
            
            # Find the separation points by looking for specific headers
            landed_start = None
            pullout_start = None
            
            # Look for "Landed" and "FOR PULL-OUT" in the first column
            for idx, row in df.iterrows():
                first_col_value = str(row.iloc[0]).strip().upper() if pd.notna(row.iloc[0]) else ""
                
                if "LANDED" in first_col_value and landed_start is None:
                    # Check if the next row has valid headers
                    if self._validate_table_headers(df, idx + 1):
                        landed_start = idx + 1  # Start BELOW the header
                        logger.info(f"Found 'Landed' header at row {idx}, table starts at row {landed_start}")
                    else:
                        logger.warning(f"Found 'Landed' header at row {idx}, but next row doesn't contain valid headers")
                
                if "FOR PULL-OUT" in first_col_value and pullout_start is None:
                    # Check if the next row has valid headers
                    if self._validate_table_headers(df, idx + 1):
                        pullout_start = idx + 1  # Start BELOW the header
                        logger.info(f"Found 'FOR PULL-OUT' header at row {idx}, table starts at row {pullout_start}")
                    else:
                        logger.warning(f"Found 'FOR PULL-OUT' header at row {idx}, but next row doesn't contain valid headers")
            
            # Split the data based on the found headers
            if landed_start is not None and pullout_start is not None:
                # Both headers found, split accordingly
                if landed_start < pullout_start:
                    # Landed table comes first
                    landed_end = pullout_start - 2  # End before the Pull-Out header
                    landed_table = df.iloc[landed_start:landed_end + 1].copy()
                    pullout_table = df.iloc[pullout_start:].copy()
                else:
                    # Pull-out table comes first
                    pullout_end = landed_start - 2  # End before the Landed header
                    pullout_table = df.iloc[pullout_start:pullout_end + 1].copy()
                    landed_table = df.iloc[landed_start:].copy()
                    
            elif landed_start is not None:
                # Only Landed header found
                landed_table = df.iloc[landed_start:].copy()
                pullout_table = pd.DataFrame()
                
            elif pullout_start is not None:
                # Only FOR PULL-OUT header found
                landed_table = pd.DataFrame()
                pullout_table = df.iloc[pullout_start:].copy()
                
            else:
                # No headers found, use fallback method
                logger.warning("No 'Landed' or 'FOR PULL-OUT' headers found, using fallback method")
                empty_rows = df.isna().all(axis=1)
                empty_row_indices = empty_rows[empty_rows].index.tolist()
                
                if len(empty_row_indices) > 0:
                    split_index = empty_row_indices[0]
                    landed_table = df.iloc[:split_index].copy()
                    pullout_table = df.iloc[split_index + 1:].copy()
                else:
                    landed_table = df.copy()
                    pullout_table = pd.DataFrame()
            
            # Set proper headers for the tables
            if not landed_table.empty:
                landed_table = self._set_table_headers(landed_table)
                # Validate required columns for landed table
                self._validate_required_columns(landed_table, 'LANDED')
            if not pullout_table.empty:
                pullout_table = self._set_table_headers(pullout_table)
                # Validate required columns for pullout table
                self._validate_required_columns(pullout_table, 'PULLOUT')
            
            # Clean the tables
            landed_table = self._clean_table(landed_table)
            pullout_table = self._clean_table(pullout_table)
            
            # Store processed data
            self.sheets_data['LANDED'] = landed_table
            self.sheets_data['PULLOUT'] = pullout_table
            
            return landed_table, pullout_table
            
        except Exception as e:
            logger.error(f"Error processing LANDED.PULL-OUT sheet: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def process_unserved_imported_sheet(self) -> pd.DataFrame:
        """
        Process the UNSERVED IMPORTED sheet.
        
        Returns:
            pd.DataFrame: Processed unserved imported data
        """
        try:
            df = pd.read_excel(self.file_path, sheet_name='UNSERVED IMPORTED')
            logger.info(f"Processing UNSERVED IMPORTED sheet with {len(df)} rows")
            
            # Set proper headers
            df = self._set_table_headers(df)
            
            # Validate required columns
            self._validate_required_columns(df, 'UNSERVED_IMPORTED')
            
            # Basic cleaning
            df = self._clean_table(df)
            
            # Store processed data
            self.sheets_data['UNSERVED_IMPORTED'] = df
            return df
            
        except Exception as e:
            logger.error(f"Error processing UNSERVED IMPORTED sheet: {e}")
            return pd.DataFrame()
    
    def process_unserved_local_sheet(self) -> pd.DataFrame:
        """
        Process the UNSERVED LOCAL sheet.
        
        Returns:
            pd.DataFrame: Processed unserved local data
        """
        try:
            df = pd.read_excel(self.file_path, sheet_name='UNSERVED LOCAL')
            logger.info(f"Processing UNSERVED LOCAL sheet with {len(df)} rows")
            
            # Set proper headers
            df = self._set_table_headers(df)
            
            # Validate required columns
            self._validate_required_columns(df, 'UNSERVED_LOCAL')
            
            # Basic cleaning
            df = self._clean_table(df)
            
            # Store processed data
            self.sheets_data['UNSERVED_LOCAL'] = df
            return df
            
        except Exception as e:
            logger.error(f"Error processing UNSERVED LOCAL sheet: {e}")
            return pd.DataFrame()
    
    def _handle_merged_cells(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle merged cells by propagating the merged cell text to all individual cells.
        Only fills empty cells that are adjacent to non-empty cells.
        
        Args:
            df (pd.DataFrame): Input dataframe
            
        Returns:
            pd.DataFrame: Dataframe with merged cell text propagated
        """
        if df.empty:
            return df
        
        # Create a copy to work with
        df_processed = df.copy()
        
        # Special handling for UNSERVED_LOCAL table - only forward fill RAW MATERIALS column
        if len(df_processed.columns) >= 8 and 'RAW MATERIALS' in df_processed.columns:
            # For UNSERVED_LOCAL, only forward fill the RAW MATERIALS column
            # Leave other columns as they are to preserve the original structure
            raw_materials_col = df_processed['RAW MATERIALS']
            
            # Forward fill RAW MATERIALS column, but preserve empty cells for summary rows
            # Only fill cells that are truly empty (not summary rows)
            filled_col = raw_materials_col.ffill()
            
            # Keep original empty cells for rows that appear to be summaries
            # (rows where supplier is empty but other data exists)
            for i in range(len(df_processed)):
                if pd.isna(df_processed.iloc[i]['SUPPLIER']) and not pd.isna(filled_col.iloc[i]):
                    # This looks like a summary row, keep RAW MATERIALS empty
                    filled_col.iloc[i] = ''
            
            df_processed['RAW MATERIALS'] = filled_col
            
            logger.info("Applied selective RAW MATERIALS forward fill for UNSERVED_LOCAL")
        else:
            # Standard merged cell handling for other tables
            for col in df_processed.columns:
                # Find consecutive empty cells and fill them with the previous value
                mask = df_processed[col].isna()
                if mask.any():
                    # Forward fill only for consecutive empty cells
                    df_processed[col] = df_processed[col].ffill()
            
            logger.info("Applied conservative merged cell handling")
        
        return df_processed
    
    def _clean_data_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean up data formatting to match expected output format.
        Removes unnecessary decimal points and formats data appropriately.
        
        Args:
            df (pd.DataFrame): Input dataframe
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        if df.empty:
            return df
        
        df_cleaned = df.copy()
        
        for col in df_cleaned.columns:
            # Convert numeric columns to remove unnecessary decimal points
            if df_cleaned[col].dtype in ['float64', 'float32']:
                # Check if all values are whole numbers
                if (df_cleaned[col] % 1 == 0).all():
                    # Convert to integer if all values are whole numbers
                    df_cleaned[col] = df_cleaned[col].astype(int)
                else:
                    # Keep as float but remove trailing .0 for whole numbers
                    df_cleaned[col] = df_cleaned[col].apply(lambda x: int(x) if x % 1 == 0 else x)
        
        logger.info("Applied data formatting cleanup")
        return df_cleaned
    
    def _set_table_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Set proper headers for a dataframe by finding the best header row.
        Scans through the dataframe to find the row with the most meaningful headers.
        
        Args:
            df (pd.DataFrame): Input dataframe
            
        Returns:
            pd.DataFrame: Dataframe with proper headers
        """
        if df.empty or len(df) < 2:
            return df
        
        # Scan through the first 10 rows to find the best header row
        best_header_row = 0
        max_meaningful_headers = 0
        
        for row_idx in range(min(10, len(df))):
            row = df.iloc[row_idx]
            meaningful_count = 0
            
            for value in row:
                if pd.notna(value):
                    str_value = str(value).strip()
                    # Check if it looks like a meaningful header (not just numbers, not empty)
                    if str_value and not str_value.replace('.', '').replace('-', '').replace(',', '').isdigit():
                        meaningful_count += 1
            
            if meaningful_count > max_meaningful_headers:
                max_meaningful_headers = meaningful_count
                best_header_row = row_idx
        
        # Only use the best header row if it has at least 2 meaningful headers
        if max_meaningful_headers >= 2:
            # Use the best header row as headers
            headers = df.iloc[best_header_row].values
            clean_headers = []
            
            for i, header in enumerate(headers):
                if pd.isna(header) or str(header).strip() == '':
                    clean_headers.append(f'Column_{i+1}')
                else:
                    clean_headers.append(str(header).strip())
            
            # Remove all rows up to and including the header row, then set the columns
            df = df.iloc[best_header_row + 1:].copy()
            df.columns = clean_headers
            
            # Now handle merged cells in the data portion only
            df = self._handle_merged_cells(df)
            
            # Clean up data formatting to match expected output
            df = self._clean_data_formatting(df)
            
            logger.info(f"Found best header row at index {best_header_row} with {max_meaningful_headers} meaningful headers, removed {best_header_row + 1} rows above")
        else:
            logger.warning(f"Best header row only has {max_meaningful_headers} meaningful headers, keeping original column names")
        
        return df
    
    def _clean_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a dataframe by removing empty rows and handling missing values.
        
        Args:
            df (pd.DataFrame): Input dataframe
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        if df.empty:
            return df
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')
        
        # Fill NaN values with appropriate defaults
        for col in df.columns:
            if hasattr(df[col], 'dtype'):
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
                elif df[col].dtype in ['int64', 'float64']:
                    df[col] = df[col].fillna(0)
            else:
                # Handle case where column might not have dtype attribute
                df[col] = df[col].fillna('')
        
        return df
    
    def process_all_sheets(self) -> Dict[str, pd.DataFrame]:
        """
        Process all sheets in the Excel file.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary containing all processed dataframes
        """
        if not self.load_excel_file():
            return {}
        
        results = {}
        
        # Process each sheet based on available sheets
        available_sheets = self.excel_file.sheet_names
        
        if 'SUMMARY' in available_sheets:
            results['SUMMARY'] = self.process_summary_sheet()
        
        if 'SAILING' in available_sheets:
            results['SAILING'] = self.process_sailing_sheet()
        
        if 'LANDED.PULL-OUT' in available_sheets:
            table1, table2 = self.process_landed_pullout_sheet()
            results['LANDED_PULLOUT_table1'] = table1
            results['LANDED_PULLOUT_table2'] = table2
        
        if 'UNSERVED IMPORTED' in available_sheets:
            results['UNSERVED_IMPORTED'] = self.process_unserved_imported_sheet()
        
        if 'UNSERVED LOCAL' in available_sheets:
            results['UNSERVED_LOCAL'] = self.process_unserved_local_sheet()
        
        # Process any other sheets that might exist
        for sheet_name in available_sheets:
            if sheet_name not in ['SUMMARY', 'SAILING', 'LANDED.PULL-OUT', 'UNSERVED IMPORTED', 'UNSERVED LOCAL']:
                try:
                    df = pd.read_excel(self.file_path, sheet_name=sheet_name)
                    df = self._clean_table(df)
                    results[sheet_name] = df
                    logger.info(f"Processed additional sheet: {sheet_name}")
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_name}: {e}")
        
        return results
    
    def save_processed_data(self, output_dir: str = "processed_data"):
        """
        Save all processed data to CSV files.
        
        Args:
            output_dir (str): Directory to save the processed data
        """
        import os
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        for sheet_name, df in self.sheets_data.items():
            if not df.empty:
                output_file = os.path.join(output_dir, f"{sheet_name}.csv")
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {sheet_name} to {output_file}")
    
    def get_summary(self) -> Dict[str, dict]:
        """
        Get a summary of all processed data.
        
        Returns:
            Dict[str, dict]: Summary information for each sheet
        """
        summary = {}
        
        for sheet_name, df in self.sheets_data.items():
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
    
    def combine_product_quantity_data(self) -> pd.DataFrame:
        """
        Combine all tables (except SUMMARY) by extracting product and quantity columns.
        UNSERVED_LOCAL quantity is multiplied by 1000 before combining.
        
        Returns:
            pd.DataFrame: Combined dataframe with product and quantity data
        """
        combined_data = []
        
        # Load product categories
        categories = self._load_product_categories()
        new_products = set()
        blank_categories = []
        
        for table_name, df in self.sheets_data.items():
            # Skip SUMMARY table
            if table_name == 'SUMMARY':
                continue
                
            if df.empty:
                continue
                
            # Get the required columns for this table
            if table_name not in self.column_mapping:
                logger.warning(f"No column mapping found for table: {table_name}")
                continue
                
            required_columns = self.column_mapping[table_name]
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
            
            # Multiply UNSERVED_LOCAL quantity by 1000
            if table_name == 'UNSERVED_LOCAL':
                # Convert to numeric, handle errors, then multiply
                table_data[quantity_col] = pd.to_numeric(table_data[quantity_col], errors='coerce')
                table_data[quantity_col] = table_data[quantity_col] * 1000
                logger.info(f"Multiplied UNSERVED_LOCAL quantities by 1000")
            
            # Rename columns to standard names
            table_data = table_data.rename(columns={
                product_col: 'PRODUCT',
                quantity_col: 'QUANTITY'
            })
            
            # Remove rows where product is empty
            table_data = table_data.dropna(subset=['PRODUCT'])
            table_data = table_data[table_data['PRODUCT'] != '']
            
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


def main():
    """
    Main function to demonstrate the Excel processor usage.
    """
    import os
    
    # Initialize the processor
    processor = ExcelProcessor('input/deliveries.xlsx')
    
    # Process all sheets
    results = processor.process_all_sheets()
    
    # Print summary
    summary = processor.get_summary()
    print("\n=== PROCESSING SUMMARY ===")
    for sheet_name, info in summary.items():
        print(f"\n{sheet_name.upper()}:")
        print(f"  Rows: {info['rows']}")
        print(f"  Columns: {info['columns']}")
        if info['column_names']:
            print(f"  Columns: {', '.join(info['column_names'])}")
    
    # Save processed data
    processor.save_processed_data()
    
    # Combine product and quantity data
    combined_df = processor.combine_product_quantity_data()
    
    # Save combined data
    if not combined_df.empty:
        combined_file = os.path.join("processed_data", "COMBINED_PRODUCT_QUANTITY.csv")
        combined_df.to_csv(combined_file, index=False)
        logger.info(f"Saved combined data to {combined_file}")
        print(f"\n=== COMBINED DATA SUMMARY ===")
        print(f"Total rows: {len(combined_df)}")
        print(f"Status breakdown: {combined_df['STATUS'].value_counts().to_dict()}")
        print(f"Category breakdown: {combined_df['CATEGORY'].value_counts().to_dict()}")
        print(f"Sample data:")
        print(combined_df.head(10).to_string(index=False))
    
    # Return the results for further use
    return results


if __name__ == "__main__":
    main() 