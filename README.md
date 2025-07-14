# Healthcare Facilities Map Generator

## 📖 Overview
This project generates an interactive map of healthcare facilities by connecting to Metabase, fetching facility data, processing coordinates, and creating a beautiful HTML map with filtering capabilities.

## 🏗️ Project Structure

The project is now split into two main components:

### 📥 `data.py` - Data Fetching
- **Purpose**: Connects to Metabase API and fetches raw data
- **Functionality**:
  - Secure authentication with Metabase (supports both username/password and API key)
  - Fetches facility data from configured questions/cards
  - Saves raw data to CSV files in the `data/` directory
  - Handles error logging and session management

### 🗺️ `map.py` - Data Processing & Map Generation
- **Purpose**: Processes raw data and generates interactive HTML map
- **Functionality**:
  - Loads data from CSV files created by `data.py`
  - Applies encoding fixes for special characters
  - Processes and validates coordinates
  - Applies coordinate corrections (if available)
  - Generates interactive HTML map with filtering capabilities

## 🚀 Getting Started

### Prerequisites

1. **Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Configuration**
   Create a `.env` file in the project root with your Metabase credentials:

   **Option 1 - API Key Authentication (Recommended):**
   ```env
   METABASE_URL=https://your-metabase-url.com
   METABASE_API_KEY=your_api_key
   ```

   **Option 2 - Username/Password Authentication:**
   ```env
   METABASE_URL=https://your-metabase-url.com
   METABASE_USERNAME=your_username
   METABASE_PASSWORD=your_password
   ```

### Usage Workflow

#### Step 1: Fetch Data from Metabase
```bash
python data.py
```
**What it does:**
- Connects to Metabase using your credentials (API key or username/password)
- Fetches facility data from question ID 4843 (configurable)
- Optionally fetches shifts data from question ID 4659
- Saves raw data to `data/raw_facilities.csv` and `data/raw_shifts.csv`

#### Step 2: Generate Interactive Map
```bash
python map.py
```
**What it does:**
- Loads the raw data from CSV files
- Processes and validates facility coordinates
- Applies encoding fixes for special characters
- Filters facilities to Spain boundaries
- Generates `facilities_map.html` - the interactive map
- Saves processed data to `data/processed_facilities.csv`

## 📂 Directory Structure

```
facility_map/
├── data.py                              # Metabase data fetcher
├── map.py                               # Map generator and data processor
├── requirements.txt                     # Python dependencies
├── .env                                 # Metabase credentials (create this)
├── facilities_map.html                  # Generated interactive map
├── data/
│   ├── raw_facilities.csv              # Raw data from Metabase
│   ├── raw_shifts.csv                  # Optional shifts data
│   ├── processed_facilities.csv        # Processed facility data
│   ├── facilities_corrected_coords.csv # Optional coordinate corrections
│   └── question_4846.csv              # Additional data files
└── README.md
```

## 🗺️ Map Features

The generated `facilities_map.html` includes:

- **Interactive Map**: Pan, zoom, and explore facilities across Spain
- **Smart Grouping**: Facilities at the same location are automatically grouped
- **Advanced Filtering**:
  - Filter by cities (multi-select)
  - Filter by medical specializations (multi-select)
  - Quick select/clear all options
- **Real-time Statistics**: Live updates of visible facilities, cities, and specializations
- **Detailed Popups**: Click markers to see facility details
- **Responsive Design**: Works on desktop and mobile devices

## ⚙️ Configuration

### Metabase Question IDs
Edit the question IDs in `data.py` to match your Metabase setup:

```python
# Configuration - Change these question IDs according to your Metabase setup
FACILITY_QUESTION_ID = 4843  # Your facility question ID
SHIFTS_QUESTION_ID = 4659    # Optional: Your shifts question ID
```

### Authentication Methods
The system automatically detects which authentication method to use:

**API Key Authentication** (detected when `METABASE_API_KEY` is present):
- More secure and recommended for production
- No session management required
- Simply add `X-API-KEY` header to requests

**Username/Password Authentication** (detected when both `METABASE_USERNAME` and `METABASE_PASSWORD` are present):
- Creates a session token
- Requires login/logout cycle
- Good for development environments

### Coordinate Corrections
If you have coordinate corrections, place them in `data/facilities_corrected_coords.csv` with format:
- Separator: `;` (semicolon)
- Columns: `Nombre_Original`, `Latitud_Corregida`, `Longitud_Corregida`

## 🔧 Data Processing Pipeline

The map generator follows a comprehensive processing pipeline:

1. **Column Standardization**: Normalizes column names and data structure
2. **Encoding Fixes**: Resolves UTF-8 encoding issues for Spanish characters
3. **Coordinate Corrections**: Applies manual coordinate overrides (if available)
4. **Validation & Filtering**: 
   - Removes facilities without coordinates
   - Filters to Spain geographical bounds (35-44°N, -10-5°E)
   - Provides detailed statistics on removed records

## 🔍 Expected Data Format

### Facility Data Columns
- `id` or `facility_id`: Unique facility identifier
- `name`, `facility_name`, or `public_name`: Facility name
- `latitude`, `address_latitude`: Latitude coordinate
- `longitude`, `address_longitude`: Longitude coordinate
- `address`: Facility address
- `city`: City name
- `specialization`: Comma-separated specializations
- `capacity`: Facility capacity
- `phone`: Contact phone
- `type`: Facility type

## 🚨 Troubleshooting

### Common Issues

1. **Missing `.env` file**
   - Create `.env` file with your Metabase credentials
   - Choose either API key or username/password authentication
   - Ensure `METABASE_URL` is always included

2. **Authentication failures**
   - **API Key**: Verify your API key is valid and has proper permissions
   - **Username/Password**: Check credentials and ensure user has access to the questions
   - Check that `METABASE_URL` is correct and accessible

3. **No data files found**
   - Run `python data.py` first to fetch data from Metabase
   - Check that `data/raw_facilities.csv` was created
   - Verify your question IDs exist and contain data

4. **Empty map**
   - Verify your facility data has valid coordinates
   - Check that coordinates are within Spain bounds
   - Review the processing logs for filtering statistics

5. **Encoding issues**
   - The system automatically fixes most UTF-8 encoding problems
   - Check logs for any encoding warnings

### Logs
Both scripts provide detailed logging:
- ✅ Success messages (green)
- ⚠️ Warning messages (yellow)  
- ❌ Error messages (red)
- ℹ️ Information messages (blue)

## 📊 Output Files

After successful execution:

1. **`facilities_map.html`**: Open in any web browser to view the interactive map
2. **`data/processed_facilities.csv`**: Processed facility data for analysis
3. **`data/raw_facilities.csv`**: Original data from Metabase (backup)

## 🎯 Next Steps

1. Configure your `.env` file with Metabase credentials
2. Run `python data.py` to fetch your data
3. Run `python map.py` to generate the map
4. Open `facilities_map.html` in your browser
5. Explore facilities with the interactive filters!

---

For questions or issues, check the logs for detailed error messages and processing statistics.