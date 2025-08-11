#!/usr/bin/env python3
"""
Excel Processor Runner - Easy Date Configuration

This script makes it easy to run the Excel processor for a specific date folder.
Simply change the DATE variable below to process different date folders.
"""

from combined_processor import CombinedProcessor
import os
import sys
from datetime import datetime

# =============================================================================
# CONFIGURATION - CHANGE THIS DATE TO PROCESS DIFFERENT FOLDERS
# =============================================================================
DATE = "20250811"  # Change this to your desired date (YYYYMMDD format)
# =============================================================================

def run_processor(date_str):
    """
    Run the Excel processor for a specific date folder.
    
    Args:
        date_str (str): Date in YYYYMMDD format
    """
    print("🚀 Excel Processor Runner")
    print("=" * 50)
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
    delivery_file = os.path.join(input_dir, "deliveries.xlsx")
    inventory_file = os.path.join(input_dir, "inventory.xlsx")
    
    if not os.path.exists(delivery_file):
        print(f"❌ Delivery file not found: {delivery_file}")
        return False
    
    if not os.path.exists(inventory_file):
        print(f"❌ Inventory file not found: {inventory_file}")
        return False
    
    print("✅ Input files found:")
    print(f"  📊 Deliveries: {delivery_file}")
    print(f"  📦 Inventory: {inventory_file}")
    print()
    
    try:
        # Initialize the processor
        print("🔄 Initializing processor...")
        processor = CombinedProcessor(delivery_file, inventory_file)
        
        # Show detected date and output directory
        print(f"📅 Detected date: {processor.input_date}")
        print(f"📁 Output directory: {processor.output_dir}")
        print()
        
        # Process all data
        print("🔄 Processing Excel data...")
        results = processor.process_all_data()
        
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
            print(f"🏷️  Category breakdown: {combined_df['CATEGORY'].value_counts().to_dict()}")
            
            # Create and save pivot table
            print(f"\n📊 Creating pivot table...")
            
            # Check for N/A categories before creating pivot table
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
                print(f"📊 Statuses: {len(pivot_table.columns) - 1}")  # Exclude TOTAL column
            else:
                print("❌ Failed to create pivot table")
        
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
        delivery_exists = os.path.exists(f"input/{date}/deliveries.xlsx")
        inventory_exists = os.path.exists(f"input/{date}/inventory.xlsx")
        
        status = "✅" if delivery_exists and inventory_exists else "⚠️"
        print(f"  {status} {date} - {delivery_exists and inventory_exists and 'Ready' or 'Missing files'}")

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
    success = run_processor(DATE)
    
    if success:
        print("\n🎯 To process a different date, either:")
        print(f"1. Change the DATE variable in this script (currently: {DATE})")
        print(f"2. Run: python run_processor.py YYYYMMDD")
        print(f"3. Run: python run_processor.py {DATE}")

if __name__ == "__main__":
    main()
