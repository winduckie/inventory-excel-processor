#!/usr/bin/env python3
"""
Example usage of the Combined Excel Processor with date-based folder structure.

This script demonstrates how to use the processor with input files organized in date-based folders.
"""

import os
from datetime import datetime
from combined_processor import CombinedProcessor

def create_sample_input_structure():
    """
    Create sample input folder structure for demonstration.
    """
    # Create today's date folder
    today = datetime.now().strftime('%Y%m%d')
    input_dir = f"input/{today}"
    
    # Create the directory structure
    os.makedirs(input_dir, exist_ok=True)
    
    print(f"Created input directory: {input_dir}")
    print("Please place your Excel files in this directory:")
    print(f"  - {input_dir}/deliveries.xlsx")
    print(f"  - {input_dir}/inventory.xlsx")
    
    return input_dir

def process_with_date_folders():
    """
    Process Excel files using the date-based folder structure.
    """
    # Create sample input structure
    input_dir = create_sample_input_structure()
    
    # Check if files exist
    delivery_file = os.path.join(input_dir, "deliveries.xlsx")
    inventory_file = os.path.join(input_dir, "inventory.xlsx")
    
    if not os.path.exists(delivery_file):
        print(f"❌ Delivery file not found: {delivery_file}")
        print("Please place your deliveries.xlsx file in the input directory.")
        return
    
    if not os.path.exists(inventory_file):
        print(f"❌ Inventory file not found: {inventory_file}")
        print("Please place your inventory.xlsx file in the input directory.")
        return
    
    print(f"✅ Found input files:")
    print(f"  - Deliveries: {delivery_file}")
    print(f"  - Inventory: {inventory_file}")
    
    # Initialize the processor
    print("\n🚀 Initializing processor...")
    processor = CombinedProcessor(delivery_file, inventory_file)
    
    # Show detected date and output directory
    print(f"📅 Detected date: {processor.input_date}")
    print(f"📁 Output directory: {processor.output_dir}")
    
    # Process all data
    print("\n🔄 Processing data...")
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
        pivot_table = processor.create_pivot_table(combined_df)
        
        if not pivot_table.empty:
            processor.save_pivot_table(pivot_table)
            print(f"✅ Pivot table created successfully!")
            print(f"📐 Shape: {pivot_table.shape}")
            print(f"📁 Categories: {len(pivot_table.index) - 1}")  # Exclude TOTAL row
            print(f"📊 Statuses: {len(pivot_table.columns) - 1}")  # Exclude TOTAL column
        else:
            print("❌ Failed to create pivot table")
    
    print(f"\n🎉 Processing complete! Check the output in: {processor.output_dir}")

def process_specific_date_folder(date_str):
    """
    Process files from a specific date folder.
    
    Args:
        date_str (str): Date string in YYYYMMDD format (e.g., '20250811')
    """
    input_dir = f"input/{date_str}"
    delivery_file = os.path.join(input_dir, "deliveries.xlsx")
    inventory_file = os.path.join(input_dir, "inventory.xlsx")
    
    if not os.path.exists(delivery_file) or not os.path.exists(inventory_file):
        print(f"❌ Files not found in {input_dir}")
        print("Please ensure both deliveries.xlsx and inventory.xlsx exist in the directory.")
        return
    
    print(f"🔄 Processing files from {input_dir}...")
    
    # Initialize the processor
    processor = CombinedProcessor(delivery_file, inventory_file)
    
    # Process all data
    results = processor.process_all_data()
    
    # Save processed data
    processor.save_processed_data()
    
    # Combine and save
    combined_df = processor.combine_all_data()
    if not combined_df.empty:
        combined_file = os.path.join(processor.output_dir, "COMBINED_ALL_DATA.csv")
        combined_df.to_csv(combined_file, index=False)
        
        # Create pivot table
        pivot_table = processor.create_pivot_table(combined_df)
        if not pivot_table.empty:
            processor.save_pivot_table(pivot_table)
    
    print(f"✅ Processing complete! Output saved to: {processor.output_dir}")

if __name__ == "__main__":
    print("🚀 Excel Processor with Date-Based Folders")
    print("=" * 50)
    
    # Check if a specific date was provided as command line argument
    import sys
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
        if len(date_arg) == 8 and date_arg.isdigit():
            print(f"📅 Processing specific date: {date_arg}")
            process_specific_date_folder(date_arg)
        else:
            print(f"❌ Invalid date format: {date_arg}")
            print("Please use YYYYMMDD format (e.g., 20250811)")
    else:
        # Process current date folder
        process_with_date_folders() 