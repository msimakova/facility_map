# 🗺️ Coordinate Checker

A comprehensive tool to check and fix facility coordinates using Google Maps API.

## 📋 What It Does

1. **Analyzes** all facility coordinates in `data/raw_facilities.csv`
2. **Identifies** problematic coordinates (missing, zero, extreme values)
3. **Skips** facilities already corrected in `data/facilities_corrected_coords.csv`
4. **Geocodes** problematic facilities using Google Maps API
5. **Updates** the corrections file automatically

## 🚀 Usage

### Basic Usage (Analysis Only)
```bash
python coordinate_checker.py
```
This will analyze coordinates and show you what needs fixing.

### With Google Maps API (Full Geocoding)
1. Get a Google Maps API key from [Google Cloud Console](https://console.cloud.google.com/)
2. Add to your `.env` file:
   ```
   GOOGLE_MAPS_API_KEY=your_api_key_here
   ```
3. Run the checker:
   ```bash
   python coordinate_checker.py
   ```

## 📊 What It Checks

- **Missing coordinates**: `NaN` values
- **Zero coordinates**: `(0.0, 0.0)`
- **Extreme values**: Like `4044270000000000.0`
- **Default coordinates**: `(1.0, 1.0)`
- **Outside Spain bounds**: Not within `(35-44, -10-5)`

## 🔄 Workflow

1. **Run data.py** - Fetch data from Metabase
2. **Run coordinate_checker.py** - Check and fix coordinates
3. **Run map.py** - Generate the updated map

## 💰 Cost

- **Google Maps API**: ~$0.20 for 40 facilities
- **Free tier**: 200 requests/day

## 📁 Files

- `coordinate_checker.py` - Main coordinate checking tool
- `data.py` - Fetch data from Metabase
- `map.py` - Generate interactive map
- `data/facilities_corrected_coords.csv` - Coordinate corrections 