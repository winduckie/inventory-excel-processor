#!/usr/bin/env python3
"""
Excel Processor Runner - Split Files Version (imports.xlsx + local.xlsx)

This script processes Excel data from split files:
- imports.xlsx: Contains imported data (Sailling, Landed, For Pull out Return, UNSERVED)
- local.xlsx: Contains local unserved data

Simply change the DATE variable below to process different date folders.
"""

from combined_processor_split import CombinedProcessorSplit
import os
import sys
from datetime import datetime
import pandas as pd

# =============================================================================
# CONFIGURATION - CHANGE THIS DATE TO PROCESS DIFFERENT FOLDERS
# =============================================================================
DATE = "20250924"  # Change this to your desired date (YYYYMMDD format)
# =============================================================================

def run_processor_split(date_str):
    """
    Run the Excel processor for a specific date folder with split files.
    
    Args:
        date_str (str): Date in YYYYMMDD format
    """
    print("🚀 Excel Processor Runner - Split Files Version")
    print("=" * 60)
    print(f"📅 Processing date: {date_str}")
    print(f"📁 Input folder: input/{date_str}/")
    print(f"📁 Output folder: processed_data/{date_str}/")
    print()
    
    # Check if input folder exists
    input_dir = f"input/{date_str}"
    if not os.path.exists(input_dir):
        print(f"❌ Input folder not found: {input_dir}")
        print("Please create the folder or check the date.")
        return False
    
    # Check if required files exist
    imports_file = os.path.join(input_dir, "imports.xlsx")
    local_file = os.path.join(input_dir, "local.xlsx")
    inventory_file = os.path.join(input_dir, "inventory.xlsx")
    global_usage_file = os.path.join(input_dir, "global_usage.csv")
    
    if not os.path.exists(imports_file):
        print(f"❌ Imports file not found: {imports_file}")
        return False
    
    if not os.path.exists(local_file):
        print(f"❌ Local file not found: {local_file}")
        return False
    
    if not os.path.exists(inventory_file):
        print(f"❌ Inventory file not found: {inventory_file}")
        return False
    
    # Check if global usage file exists
    if os.path.exists(global_usage_file):
        print(f"🌍 Global usage file found: {global_usage_file}")
        print("   Will create enhanced pivot table with GLOBAL USAGE column")
    else:
        print(f"📊 No global usage file found: {global_usage_file}")
        print("   Running in standard mode")
        global_usage_file = None
    
    print("✅ Input files found:")
    print(f"  📊 Imports: {imports_file}")
    print(f"  🏠 Local: {local_file}")
    print(f"  📦 Inventory: {inventory_file}")
    if global_usage_file:
        print(f"  🌍 Global Usage: {global_usage_file}")
    print()
    
    try:
        # Initialize the processor
        print("🔄 Initializing split processor...")
        processor = CombinedProcessorSplit(imports_file, local_file, inventory_file, global_usage_file)
        
        # Show detected date and output directory
        print(f"📅 Detected date: {processor.input_date}")
        print(f"📁 Output directory: {processor.output_dir}")
        print()
        
        # Process all data
        print("🔄 Processing Excel data...")
        results = processor.process_all_data()
        
        # Ensure global usage data is loaded if available
        if global_usage_file:
            processor._load_global_usage_data()
            print(f"🌍 Global usage data loaded: {len(processor.global_usage_data.get('global_usage', pd.DataFrame()))} ingredients")
        
        # Print summary
        summary = processor.get_summary()
        print("\n=== PROCESSING SUMMARY ===")
        for sheet_name, info in summary.items():
            print(f"\n{sheet_name.upper()}:")
            print(f"  Rows: {info['rows']}")
            print(f"  Columns: {info['columns']}")
            if info['column_names']:
                # Convert all column names to strings to avoid TypeError
                column_names = [str(col) for col in info['column_names']]
                print(f"  Columns: {', '.join(column_names)}")
        
        # Save processed data
        print(f"\n💾 Saving processed data to {processor.output_dir}...")
        processor.save_processed_data()
        
        # Combine all data
        print("\n🔗 Combining all data...")
        combined_df = processor.combine_all_data()
        
        # Save combined data
        if not combined_df.empty:
            combined_file = os.path.join(processor.output_dir, "COMBINED_ALL_DATA.csv")
            combined_df.to_csv(combined_file, index=False)
            print(f"✅ Saved combined data to {combined_file}")
            print(f"📊 Total rows: {len(combined_df)}")
            print(f"📈 Status breakdown: {combined_df['STATUS'].value_counts().to_dict()}")
            # Check if CATEGORY column exists before trying to access it
            if 'CATEGORY' in combined_df.columns:
                print(f"🏷️  Category breakdown: {combined_df['CATEGORY'].value_counts().to_dict()}")
            else:
                print("🏷️  No CATEGORY column found in combined data")
            
            # Create and save pivot table
            print(f"\n📊 Creating pivot table...")
            
            # Check for N/A categories before creating pivot table (if CATEGORY column exists)
            if 'CATEGORY' in combined_df.columns:
                na_count = len(combined_df[combined_df['CATEGORY'] == 'N/A'])
                if na_count > 0:
                    na_products = combined_df[combined_df['CATEGORY'] == 'N/A']['PRODUCT'].unique()
                    print(f"⚠️  Found {na_count} products with 'N/A' category - these will be excluded from pivot table")
                    print(f"📋 N/A products: {', '.join(sorted(na_products))}")
            
            pivot_table = processor.create_pivot_table(combined_df)
            
            if not pivot_table.empty:
                processor.save_pivot_table(pivot_table)
                print(f"✅ Pivot table created successfully!")
                print(f"📐 Shape: {pivot_table.shape}")
                print(f"📁 Categories: {len(pivot_table.index) - 1}")  # Exclude TOTAL row
                # Count statuses excluding TOTAL and GLOBAL USAGE columns
                status_columns = [col for col in pivot_table.columns if col not in ['TOTAL', 'GLOBAL USAGE']]
                print(f"📊 Statuses: {len(status_columns)}")
                print(f"📋 Column order: {list(pivot_table.columns)}")
                
                # Show enhanced pivot table if global usage is available
                if global_usage_file:
                    print(f"\n🌍 Creating enhanced pivot table with GLOBAL USAGE...")
                    enhanced_pivot_table = processor.create_enhanced_pivot_table(combined_df)
                    
                    if not enhanced_pivot_table.empty:
                        processor.save_enhanced_pivot_table(enhanced_pivot_table)
                        print(f"✅ Enhanced pivot table created successfully!")
                        print(f"📐 Shape: {enhanced_pivot_table.shape}")
                        print(f"📁 Categories: {len(enhanced_pivot_table.index) - 1}")  # Exclude TOTAL row
                        print(f"📋 Column order: {list(enhanced_pivot_table.columns)}")
                        print(f"🌍 GLOBAL USAGE column (right of TOTAL): ✅")
                        print(f"📊 Monthly Usage column: ✅")
                        print(f"🔮 1-6 Month Projections: ✅")
                    else:
                        print("❌ Failed to create enhanced pivot table")
            else:
                print("❌ Failed to create pivot table")
            
            # Print final summary at the end for visibility
            if not combined_df.empty:
                try:
                    statuses_of_interest = ['SAILING', 'LANDED', 'PULLOUT', 'UNSERVED IMPORTED', 'UNSERVED LOCAL']
                    interest_counts = {}
                    for status in statuses_of_interest:
                        matching_rows = combined_df[combined_df['STATUS'].str.upper() == status.upper()]
                        interest_counts[status] = int(matching_rows.shape[0])
                    
                    # Count uncategorized entries
                    # Only count blank/empty categories, not 'N/A' (N/A means acknowledged as not needing a category)
                    uncategorized_count = 0
                    try:
                        import csv
                        with open('config/product_categories.csv', 'r', newline='', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                category_val = (row.get('Category') or '').strip()
                                if category_val == '':  # Only count blank, not N/A
                                    uncategorized_count += 1
                    except Exception:
                        pass
                    
                    print('\n' + '=' * 60)
                    print('=== FINAL DATA SUMMARY ===')
                    print(f"Entries by status:")
                    print(f"  SAILING: {interest_counts.get('SAILING', 0)}")
                    print(f"  LANDED: {interest_counts.get('LANDED', 0)}")
                    print(f"  PULLOUT: {interest_counts.get('PULLOUT', 0)}")
                    print(f"  UNSERVED IMPORTED: {interest_counts.get('UNSERVED IMPORTED', 0)}")
                    print(f"  UNSERVED LOCAL: {interest_counts.get('UNSERVED LOCAL', 0)}")
                    print(f"Uncategorized entries in product_categories.csv: {uncategorized_count}")
                    print('=' * 60)
                except Exception as e:
                    print(f"⚠️  Could not generate final summary: {e}")
        
        print(f"\n🎉 Processing complete!")
        print(f"📁 Check your results in: {processor.output_dir}")
        return True
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_available_dates():
    """Show available date folders."""
    if not os.path.exists("input"):
        print("❌ No input folder found.")
        return
    
    dates = [d for d in os.listdir("input") if os.path.isdir(os.path.join("input", d)) and d.isdigit()]
    
    if not dates:
        print("❌ No date folders found in input/")
        return
    
    print("📂 Available date folders:")
    for date in sorted(dates):
        # Check if files exist
        imports_exists = os.path.exists(f"input/{date}/imports.xlsx")
        local_exists = os.path.exists(f"input/{date}/local.xlsx")
        inventory_exists = os.path.exists(f"input/{date}/inventory.xlsx")
        
        status = "✅" if imports_exists and local_exists and inventory_exists else "⚠️"
        print(f"  {status} {date} - {imports_exists and local_exists and inventory_exists and 'Ready' or 'Missing files'}")

def main():
    """Main function."""
    global DATE
    
    # Check if date was provided as command line argument
    if len(sys.argv) > 1:
        DATE = sys.argv[1]
    
    # Validate date format
    if not DATE.isdigit() or len(DATE) != 8:
        print("❌ Invalid date format. Please use YYYYMMDD format (e.g., 20250811)")
        print(f"Current DATE value: {DATE}")
        return
    
    # Show available dates
    print()
    show_available_dates()
    print()
    
    # Run the processor
    success = run_processor_split(DATE)
    
    if success:
        print("\n🎯 To process a different date, either:")
        print(f"1. Change the DATE variable in this script (currently: {DATE})")
        print(f"2. Run: python run_processor_split.py YYYYMMDD")
        print(f"3. Run: python run_processor_split.py {DATE}")

if __name__ == "__main__":
    main()
