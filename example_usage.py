#!/usr/bin/env python3
"""
Example usage of the Excel processor for deliveries.xlsx
"""

from excel_processor import ExcelProcessor
from advanced_excel_processor import AdvancedExcelProcessor

def basic_usage():
    """
    Basic usage example using the standard Excel processor.
    """
    print("=== BASIC EXCEL PROCESSOR ===")
    
    # Initialize the processor
    processor = ExcelProcessor('input/deliveries.xlsx')
    
    # Process all sheets
    results = processor.process_all_sheets()
    
    # Print results
    for sheet_name, df in results.items():
        print(f"\n{sheet_name}:")
        print(f"  Shape: {df.shape}")
        if not df.empty:
            print(f"  Columns: {list(df.columns)}")
            print(f"  First few rows:")
            print(df.head())
    
    # Save to CSV files
    processor.save_processed_data("basic_output")
    print("\nData saved to 'basic_output' directory")

def advanced_usage():
    """
    Advanced usage example using the advanced Excel processor.
    """
    print("\n=== ADVANCED EXCEL PROCESSOR ===")
    
    # Initialize the advanced processor
    processor = AdvancedExcelProcessor('input/deliveries.xlsx')
    
    # Process all sheets
    results = processor.process_all_sheets()
    
    # Print results
    for sheet_name, df in results.items():
        print(f"\n{sheet_name}:")
        print(f"  Shape: {df.shape}")
        if not df.empty:
            print(f"  Columns: {list(df.columns)}")
            print(f"  First few rows:")
            print(df.head())
    
    # Save to CSV files
    processor.save_processed_data("advanced_output")
    print("\nData saved to 'advanced_output' directory")

def process_specific_sheets():
    """
    Example of processing specific sheets individually.
    """
    print("\n=== PROCESSING SPECIFIC SHEETS ===")
    
    processor = AdvancedExcelProcessor('input/deliveries.xlsx')
    
    # Process SUMMARY sheet
    summary_df = processor.process_summary_sheet()
    print(f"SUMMARY sheet: {summary_df.shape}")
    
    # Process SAILING sheet
    sailing_df = processor.process_sailing_sheet()
    print(f"SAILING sheet: {sailing_df.shape}")
    
    # Process LANDED.PULL-OUT sheet (with two tables)
    landed_table1, landed_table2 = processor.process_landed_pullout_sheet()
    print(f"LANDED.PULL-OUT table 1: {landed_table1.shape}")
    print(f"LANDED.PULL-OUT table 2: {landed_table2.shape}")
    
    # Process UNSERVED IMPORTED sheet
    unserved_imported_df = processor.process_unserved_imported_sheet()
    print(f"UNSERVED IMPORTED sheet: {unserved_imported_df.shape}")
    
    # Process UNSERVED LOCAL sheet
    unserved_local_df = processor.process_unserved_local_sheet()
    print(f"UNSERVED LOCAL sheet: {unserved_local_df.shape}")
    
    # Get summary
    summary = processor.get_summary()
    print("\nSummary:")
    for sheet_name, info in summary.items():
        print(f"  {sheet_name}: {info['rows']} rows, {info['columns']} columns")

if __name__ == "__main__":
    # Run all examples
    basic_usage()
    advanced_usage()
    process_specific_sheets() 