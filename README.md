# Excel Processor with Date-Based Organization

This project provides code to process Excel files (deliveries.xlsx and inventory.xlsx) with automatic date-based folder organization. Input files are organized by date (e.g., `input/20250811/`) and output is automatically saved to corresponding date-based folders (e.g., `processed_data/20250811/`).

## Features

- **Date-Based Organization**: Automatic detection of date from input folder names
- **Multiple Sheet Processing**: Each sheet in the Excel file is processed separately
- **Special Sailing Sheet Handling**: The sailing sheet contains two tables that are processed separately
- **Data Cleaning**: Automatic removal of empty rows/columns and handling of missing values
- **CSV Export**: Processed data can be saved as CSV files
- **Comprehensive Logging**: Detailed logging of processing steps
- **Error Handling**: Robust error handling for various Excel file issues
- **Automatic Output Organization**: Output files are automatically organized by date

## Files

- `combined_processor.py`: Main processor that combines delivery and inventory data
- `excel_processor.py`: Basic Excel processor with standard functionality
- `advanced_excel_processor.py`: Advanced processor with better table detection
- `example_usage.py`: Example scripts showing how to use the processors
- `setup_date_folders.py`: Helper script to create date-based folder structure
- `requirements.txt`: Required Python packages

## Installation

1. Install the required packages:
```bash
pip install -r requirements.txt
```

2. Set up the date-based folder structure:
```bash
python setup_date_folders.py today
```

## Usage

### Date-Based Organization

The processor automatically detects the date from your input folder structure:

```
input/
├── 20250811/
│   ├── deliveries.xlsx
│   └── inventory.xlsx
├── 20250812/
│   ├── deliveries.xlsx
│   └── inventory.xlsx
└── ...
```

Output is automatically organized in corresponding folders:

```
processed_data/
├── 20250811/
│   ├── COMBINED_ALL_DATA.csv
│   ├── PIVOT_TABLE.csv
│   ├── PIVOT_TABLE.xlsx
│   └── ...
├── 20250812/
│   └── ...
└── ...
```

### Basic Usage

```python
from combined_processor import CombinedProcessor

# Initialize the processor (automatically detects date from folder)
processor = CombinedProcessor('input/20250811/deliveries.xlsx', 'input/20250811/inventory.xlsx')

# Process all data
results = processor.process_all_data()

# Save to CSV files (automatically uses date-based output folder)
processor.save_processed_data()
```

### Advanced Usage

```python
from combined_processor import CombinedProcessor

# Initialize the processor
processor = CombinedProcessor('input/20250811/deliveries.xlsx', 'input/20250811/inventory.xlsx')

# Process specific sheets
deliveries_df = processor.process_delivery_sheet('SUMMARY')
landed_df, pullout_df = processor.process_landed_pullout_sheet('LANDED.PULL OUT')
inventory_df = processor.process_inventory_sheet()

# Get summary
summary = processor.get_summary()
```

### Command Line Usage

```bash
# Process current date folder
python example_usage.py

# Process specific date folder
python example_usage.py 20250811

# Create date-based folders
python setup_date_folders.py today
python setup_date_folders.py create 20250811 7
```

### Running Examples

```bash
python example_usage.py
```

## Sheet Processing

### Deliveries Sheet
- Function: `process_deliveries_sheet()`
- Returns: Single DataFrame with deliveries data

### Sailing Sheet
- Function: `process_sailing_sheet()`
- Returns: Tuple of two DataFrames (table1, table2)
- Special handling for detecting and separating the two tables

### Orders Sheet
- Function: `process_orders_sheet()`
- Returns: Single DataFrame with orders data

### Inventory Sheet
- Function: `process_inventory_sheet()`
- Returns: Single DataFrame with inventory data

## Output

The processors will:
1. Load the Excel file
2. Process each sheet according to its specific function
3. Clean the data (remove empty rows/columns, handle missing values)
4. Save processed data as CSV files in the specified output directory
5. Provide a summary of the processed data

## Advanced Features

The `AdvancedExcelProcessor` includes:
- **Table Boundary Detection**: Automatically detects where tables are separated
- **Pattern Recognition**: Identifies table separation based on data patterns
- **Alternative Splitting Methods**: Multiple fallback methods for table separation
- **Enhanced Logging**: More detailed processing information

## Error Handling

Both processors include comprehensive error handling for:
- File not found
- Invalid Excel file format
- Missing sheets
- Data processing errors

## Example Output Structure

```
processed_data/
├── 20250811/
│   ├── COMBINED_ALL_DATA.csv
│   ├── PIVOT_TABLE.csv
│   ├── PIVOT_TABLE.xlsx
│   ├── SUMMARY.csv
│   ├── SAILING.csv
│   ├── LANDED.csv
│   ├── PULLOUT.csv
│   └── inventory.csv
├── 20250812/
│   └── ...
└── ...
```

## Setup and Organization

### Creating Date-Based Folders

Use the setup script to create organized folder structures:

```bash
# Create today's folder
python setup_date_folders.py today

# Create folders for a specific date range
python setup_date_folders.py create 20250811 7

# Show current folder structure
python setup_date_folders.py show
```

### Folder Structure

Each date folder contains:
- `README.md` - Instructions and usage notes
- `deliveries.xlsx` - Your delivery data
- `inventory.xlsx` - Your inventory data

## Logging

The processors provide detailed logging including:
- File loading status
- Sheet processing progress
- Table detection results
- Data cleaning operations
- File saving operations
- Date detection and output folder creation

## Requirements

- Python 3.7+
- pandas >= 1.5.0
- numpy >= 1.21.0
- openpyxl >= 3.0.0
- xlrd >= 2.0.0 