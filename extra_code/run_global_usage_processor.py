#!/usr/bin/env python3
"""
Runner script for the Global Usage Processor

This script provides a simple way to run the global usage processor
with the required input files.
"""

import os
import sys
from global_usage_processor import GlobalUsageProcessor

def main():
    """
    Main function to run the global usage processor.
    """
    print("🚀 Starting Global Usage Processor...")
    
    # Check if input files exist
    required_files = [
        'input/deliveries.xlsx',
        'input/inventory.xlsx', 
        'input/global_usage.csv'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing required input files:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        print("\nPlease ensure all required files are present in the input/ directory.")
        sys.exit(1)
    
    print("✅ All required input files found")
    
    try:
        # Initialize the processor
        processor = GlobalUsageProcessor(
            'input/deliveries.xlsx',
            'input/inventory.xlsx',
            'input/global_usage.csv'
        )
        
        print("📊 Processing all data...")
        
        # Process all data
        results = processor.process_all_data()
        
        print("💾 Saving processed data...")
        
        # Save processed data
        processor.save_processed_data()
        
        print("🔗 Combining all data...")
        
        # Combine all data
        combined_df = processor.combine_all_data()
        
        if not combined_df.empty:
            # Save combined data
            combined_file = os.path.join(processor.output_dir, "COMBINED_ALL_DATA.csv")
            combined_df.to_csv(combined_file, index=False)
            print(f"✅ Combined data saved to: {combined_file}")
            
            print("📈 Creating enhanced pivot table...")
            
            # Create and save enhanced pivot table
            enhanced_pivot_table = processor.create_enhanced_pivot_table(combined_df)
            
            if not enhanced_pivot_table.empty:
                processor.save_enhanced_pivot_table(enhanced_pivot_table)
                print("✅ Enhanced pivot table created successfully!")
                print(f"📊 Shape: {enhanced_pivot_table.shape}")
                print(f"📁 Output directory: {processor.output_dir}")
                
                # Show summary of the enhanced pivot table
                print("\n📋 Enhanced Pivot Table Summary:")
                print(f"   Categories: {len(enhanced_pivot_table.index) - 1}")  # Exclude TOTAL row
                print(f"   Columns: {len(enhanced_pivot_table.columns)}")
                print(f"   Monthly Usage column: ✅")
                print(f"   1-6 Month Projections: ✅")
                print(f"   Negative values colored red: ✅")
                
            else:
                print("❌ Failed to create enhanced pivot table")
        else:
            print("❌ No combined data available")
        
        print(f"\n🎉 Processing completed successfully!")
        print(f"📁 Check the output directory: {processor.output_dir}")
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
