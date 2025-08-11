# Excel Processor for deliveries.xlsx

This project provides code to process the `deliveries.xlsx` file into separate tables. Each sheet has its own processing function, and the sailing sheet is handled specially to separate its two tables.

## Features

- **Multiple Sheet Processing**: Each sheet in the Excel file is processed separately
- **Special Sailing Sheet Handling**: The sailing sheet contains two tables that are processed separately
- **Data Cleaning**: Automatic removal of empty rows/columns and handling of missing values
- **CSV Export**: Processed data can be saved as CSV files
- **Comprehensive Logging**: Detailed logging of processing steps
- **Error Handling**: Robust error handling for various Excel file issues

## Files

- `excel_processor.py`: Basic Excel processor with standard functionality
- `advanced_excel_processor.py`: Advanced processor with better table detection
- `example_usage.py`: Example scripts showing how to use the processors
- `requirements.txt`: Required Python packages

## Installation

1. Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from excel_processor import ExcelProcessor

# Initialize the processor
processor = ExcelProcessor('deliveries.xlsx')

# Process all sheets
results = processor.process_all_sheets()

# Save to CSV files
processor.save_processed_data("output_directory")
```

### Advanced Usage

```python
from advanced_excel_processor import AdvancedExcelProcessor

# Initialize the advanced processor
processor = AdvancedExcelProcessor('deliveries.xlsx')

# Process specific sheets
deliveries_df = processor.process_deliveries_sheet()
sailing_table1, sailing_table2 = processor.process_sailing_sheet()

# Get summary
summary = processor.get_summary()
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
├── deliveries.csv
├── sailing_table1.csv
├── sailing_table2.csv
├── orders.csv
└── inventory.csv
```

## Logging

The processors provide detailed logging including:
- File loading status
- Sheet processing progress
- Table detection results
- Data cleaning operations
- File saving operations

## Requirements

- Python 3.7+
- pandas >= 1.5.0
- numpy >= 1.21.0
- openpyxl >= 3.0.0
- xlrd >= 2.0.0 