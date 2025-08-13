# Global Usage Processor

The Global Usage Processor is an extension of the existing Combined Excel Processor that integrates global usage data from `global_usage.csv` to provide enhanced inventory projections and monthly usage analysis.

## Features

### 🔄 Enhanced Pivot Table
- **Monthly Usage Column**: Shows the monthly consumption rate for each product category
- **1-6 Month Projections**: Calculates projected inventory levels for the next 6 months
- **Negative Value Highlighting**: Automatically colors negative projected values in red
- **Smart Ingredient Mapping**: Automatically matches ingredient names from global usage to product categories

### 📊 Data Integration
- Processes `global_usage.csv` alongside existing delivery and inventory data
- Automatically calculates monthly usage rates (annual usage ÷ 12)
- Projects future inventory levels by subtracting cumulative usage from current totals
- Maintains all existing functionality from the base combined processor

### 🎨 Enhanced Formatting
- **Monthly Usage**: Highlighted in yellow with bold formatting
- **Projection Columns**: Highlighted in blue with bold formatting
- **Negative Values**: Highlighted in red with bold formatting
- **Professional Excel Output**: Clean borders, proper number formatting, and auto-adjusted column widths

## File Structure

```
excel_processor/
├── global_usage_processor.py          # Main processor class
├── run_global_usage_processor.py      # Simple runner script
├── combined_processor.py              # Base processor (inherited from)
├── config/
│   ├── table_column_mapping.json     # Updated with global_usage mapping
│   └── product_categories.csv        # Product categorization
├── input/
│   ├── deliveries.xlsx               # Delivery data
│   ├── inventory.xlsx                # Inventory data
│   └── global_usage.csv              # Global usage data
└── processed_data/                    # Output directory
```

## Usage

### Quick Start

1. **Ensure all input files are present:**
   ```bash
   input/
   ├── deliveries.xlsx
   ├── inventory.xlsx
   └── global_usage.csv
   ```

2. **Run the processor:**
   ```bash
   python run_global_usage_processor.py
   ```

### Programmatic Usage

```python
from global_usage_processor import GlobalUsageProcessor

# Initialize the processor
processor = GlobalUsageProcessor(
    'input/deliveries.xlsx',
    'input/inventory.xlsx',
    'input/global_usage.csv'
)

# Process all data
results = processor.process_all_data()

# Create enhanced pivot table
enhanced_pivot = processor.create_enhanced_pivot_table()

# Save with formatting
processor.save_enhanced_pivot_table(enhanced_pivot)
```

## Output Files

### Enhanced Pivot Table
- **ENHANCED_PIVOT_TABLE.csv**: Raw data in CSV format
- **ENHANCED_PIVOT_TABLE.xlsx**: Formatted Excel file with:
  - Color-coded columns
  - Negative value highlighting
  - Professional formatting
  - Auto-adjusted column widths

### Additional Outputs
- **COMBINED_ALL_DATA.csv**: All processed data combined
- **Individual sheet files**: Processed data for each input sheet
- **Log files**: Detailed processing logs in `logs/` directory

## Column Structure

The enhanced pivot table includes:

| Column | Description | Formatting |
|--------|-------------|------------|
| **Category** | Product categories | Bold, left-aligned |
| **Status Columns** | Delivery statuses (LANDED, SAILING, etc.) | Standard |
| **TOTAL** | Sum of all statuses | Bold, blue background |
| **Monthly Usage** | Monthly consumption rate | Bold, yellow background |
| **1 Month Projection** | Inventory after 1 month | Bold, blue background |
| **2 Month Projection** | Inventory after 2 months | Bold, blue background |
| **3 Month Projection** | Inventory after 3 months | Bold, blue background |
| **4 Month Projection** | Inventory after 4 months | Bold, blue background |
| **5 Month Projection** | Inventory after 5 months | Bold, blue background |
| **6 Month Projection** | Inventory after 6 months | Bold, blue background |

## Calculation Logic

### Monthly Usage
```
Monthly Usage = Annual Usage (from global_usage.csv) ÷ 12
```

### Projected Inventory
```
Projected Inventory = Current Total - (Monthly Usage × Number of Months)
```

### Negative Value Detection
- Values < 0 are automatically colored red
- Indicates potential stockout risk
- Helps identify critical inventory planning needs

## Ingredient Mapping

The processor automatically creates mappings between:
- **Ingredient Names** from `global_usage.csv`
- **Product Names** from `product_categories.csv`

### Mapping Strategy
1. **Exact Match**: Direct name comparison
2. **Partial Match**: Substring matching (case-insensitive)
3. **Fallback**: Use ingredient name as-is if no match found

## Error Handling

- **Missing Files**: Graceful error messages with file path details
- **Data Validation**: Automatic handling of malformed data
- **Logging**: Comprehensive logging to both file and console
- **Exception Recovery**: Continues processing even if individual steps fail

## Requirements

- Python 3.7+
- pandas
- numpy
- openpyxl
- All dependencies from `requirements.txt`

## Troubleshooting

### Common Issues

1. **Missing Input Files**
   - Ensure all three input files are in the `input/` directory
   - Check file permissions and paths

2. **Ingredient Mapping Issues**
   - Review `product_categories.csv` for product name consistency
   - Check ingredient names in `global_usage.csv` for typos

3. **Negative Projections**
   - High monthly usage relative to current inventory
   - Consider reviewing usage data accuracy
   - Check if annual vs. monthly usage assumptions are correct

### Log Files
Check `logs/global_usage_processor_YYYYMMDD_HHMMSS.log` for detailed processing information and error details.

## Future Enhancements

- **Custom Time Periods**: Configurable projection periods (3, 6, 12 months)
- **Usage Trends**: Historical usage pattern analysis
- **Alert System**: Automated notifications for critical inventory levels
- **Export Options**: Additional output formats (PDF, JSON, etc.)
- **Web Interface**: Browser-based processing and visualization

## Support

For issues or questions:
1. Check the log files for detailed error information
2. Verify input file formats and data quality
3. Review the ingredient mapping logic
4. Ensure all dependencies are properly installed
