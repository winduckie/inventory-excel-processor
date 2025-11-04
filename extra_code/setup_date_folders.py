#!/usr/bin/env python3
"""
Setup script for date-based folder structure.

This script helps you create the proper folder structure for organizing
your Excel files by date.
"""

import os
from datetime import datetime, timedelta

def create_date_folders(start_date=None, num_days=7):
    """
    Create date-based input and output folders.
    
    Args:
        start_date (str): Start date in YYYYMMDD format. If None, uses today.
        num_days (int): Number of days to create folders for.
    """
    if start_date is None:
        start_date = datetime.now()
    else:
        start_date = datetime.strptime(start_date, '%Y%m%d')
    
    print(f"📅 Creating folders starting from {start_date.strftime('%Y-%m-%d')}")
    print(f"📁 Creating {num_days} days of folders...")
    print()
    
    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        date_str = current_date.strftime('%Y%m%d')
        
        # Create input folder
        input_dir = f"input/{date_str}"
        os.makedirs(input_dir, exist_ok=True)
        
        # Create output folder
        output_dir = f"processed_data/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a README file in each input folder
        readme_content = f"""# Input Folder for {date_str}

This folder is for Excel files to be processed on {current_date.strftime('%Y-%m-%d')}.

## Required Files:
- `deliveries.xlsx` - Delivery data
- `inventory.xlsx` - Inventory data

## Usage:
1. Place your Excel files in this folder
2. Run: `python example_usage.py {date_str}`
3. Check results in: `processed_data/{date_str}/`

## Notes:
- Files will be automatically processed and saved to the corresponding output folder
- The processor will detect the date from the folder name
- Output will be organized by date for easy tracking
"""
        
        readme_file = os.path.join(input_dir, "README.md")
        with open(readme_file, 'w') as f:
            f.write(readme_content)
        
        print(f"✅ Created: {input_dir}/")
        print(f"✅ Created: {output_dir}/")
        print(f"📝 Added README.md to {input_dir}/")
        print()
    
    print("🎉 Date-based folder structure created successfully!")
    print()
    print("📋 Next steps:")
    print("1. Place your Excel files in the appropriate date folder")
    print("2. Run the processor: python example_usage.py")
    print("3. Or process a specific date: python example_usage.py 20250811")

def create_today_folder():
    """Create folder for today's date."""
    today = datetime.now().strftime('%Y%m%d')
    create_date_folders(today, 1)

def show_folder_structure():
    """Display the current folder structure."""
    print("📂 Current folder structure:")
    print()
    
    if os.path.exists("input"):
        print("input/")
        for item in sorted(os.listdir("input")):
            if os.path.isdir(os.path.join("input", item)):
                print(f"  └── {item}/")
                subdir = os.path.join("input", item)
                for subitem in sorted(os.listdir(subdir)):
                    if subitem.endswith(('.xlsx', '.xls')):
                        print(f"      └── {subitem}")
                    elif subitem == "README.md":
                        print(f"      └── {subitem}")
    else:
        print("input/ (not created yet)")
    
    print()
    
    if os.path.exists("processed_data"):
        print("processed_data/")
        for item in sorted(os.listdir("processed_data")):
            if os.path.isdir(os.path.join("processed_data", item)):
                print(f"  └── {item}/")
                subdir = os.path.join("processed_data", item)
                for subitem in sorted(os.listdir(subdir)):
                    print(f"      └── {subitem}")
    else:
        print("processed_data/ (not created yet)")

if __name__ == "__main__":
    import sys
    
    print("🚀 Date-Based Folder Setup Script")
    print("=" * 40)
    print()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "today":
            create_today_folder()
        elif command == "show":
            show_folder_structure()
        elif command == "create" and len(sys.argv) > 2:
            start_date = sys.argv[2]
            num_days = int(sys.argv[3]) if len(sys.argv) > 3 else 7
            create_date_folders(start_date, num_days)
        else:
            print("❌ Invalid command or missing arguments")
            print()
            print("Usage:")
            print("  python setup_date_folders.py today                    # Create today's folder")
            print("  python setup_date_folders.py show                    # Show current structure")
            print("  python setup_date_folders.py create 20250811         # Create folders starting from 20250811")
            print("  python setup_date_folders.py create 20250811 14      # Create 14 days of folders")
    else:
        # Default: create today's folder
        create_today_folder()
        print()
        show_folder_structure()
