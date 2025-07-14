"""
Healthcare Facilities Map Generator
Processes facility data and generates an interactive HTML map showing healthcare facilities.
"""

import pandas as pd
import numpy as np
import os
import logging
import json
import re
from datetime import datetime
import pytz
from data import MetabaseDataFetcher

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_encoding_issues(text):
    """Robustly fix UTF-8 encoding issues by detecting and re-encoding text"""
    if pd.isna(text):
        return text
    
    text = str(text)
    
    # Skip if text doesn't contain problematic characters
    if not any(char in text for char in ['Ã', 'â', 'Â', 'Ñ', 'ñ']):
        return text
    
    try:
        # Try to detect if text was incorrectly encoded
        # Common pattern: UTF-8 text read as Latin-1/Windows-1252
        
        # Method 1: Try to encode as Latin-1 then decode as UTF-8
        try:
            corrected = text.encode('latin-1').decode('utf-8')
            return corrected
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        
        # Method 2: Try to encode as Windows-1252 then decode as UTF-8
        try:
            corrected = text.encode('windows-1252').decode('utf-8')
            return corrected
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        
        # Method 3: Manual byte-level correction for common UTF-8 sequences
        try:
            bytes_text = text.encode('latin-1')
            corrected = bytes_text.decode('utf-8')
            return corrected
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
            
    except Exception:
        pass
    
    # If all automatic methods fail, return original text
    return text

def apply_encoding_fix_to_dataframe(df):
    """Apply encoding fix to all string columns in a DataFrame"""
    if df is None:
        return None
        
    df_fixed = df.copy()
    
    # Get all string/object columns
    string_columns = df_fixed.select_dtypes(include=['object']).columns
    
    for col in string_columns:
        # Skip columns that are likely numeric IDs or codes
        if col.lower() in ['id', 'facility_id', 'professional_id', 'shift_id', 'postal_code', 'phone_number']:
            continue
            
        # Apply fix to all text values in the column
        try:
            df_fixed[col] = df_fixed[col].apply(fix_encoding_issues)
        except Exception as e:
            logger.warning(f"Warning: Could not fix encoding for column {col}: {e}")
            continue
    
    return df_fixed

def update_coords(row, coords_map):
    """Update coordinates from correction file"""
    name = None
    possible_name_cols = ['facility_name', 'name', 'Name', 'public_name']
    
    for col in possible_name_cols:
        if col in row and pd.notna(row[col]):
            name = row[col]
            break
    
    if name and name in coords_map:
        row['latitude'] = coords_map[name]['Latitud_Corregida']
        row['longitude'] = coords_map[name]['Longitud_Corregida']
    return row

def standardize_dataframes(*dataframes):
    """Standardize column names for all dataframes"""
    standardized = []
    
    for df in dataframes:
        if df is not None:
            # Standardize column names
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            standardized.append(df)
        else:
            standardized.append(None)
    
    return standardized

def process_facilities(facility_data, coordinate_corrections=None):
    """
    Process facility data following the complete coordinate processing pipeline
    """
    logger.info("=== 🏥 PROCESSING FACILITIES ===")
    
    if facility_data is None or facility_data.empty:
        logger.error("❌ No facility data provided")
        return None
    
    facility = facility_data.copy()
    
    # STEP 1: Column Standardization
    logger.info("🔧 Step 1: Standardizing columns...")
    facility.columns = facility.columns.str.lower().str.replace(' ', '_')
    
    # Rename specific columns to standardize
    if 'id' in facility.columns and 'facility_id' not in facility.columns:
        facility = facility.rename(columns={'id': 'facility_id'})
    if 'name' in facility.columns and 'facility_name' not in facility.columns:
        facility = facility.rename(columns={'name': 'facility_name'})
    if 'public_name' in facility.columns and 'facility_name' not in facility.columns:
        facility = facility.rename(columns={'public_name': 'facility_name'})
    if 'address_latitude' in facility.columns:
        facility = facility.rename(columns={'address_latitude': 'latitude'})
    if 'address_longitude' in facility.columns:
        facility = facility.rename(columns={'address_longitude': 'longitude'})
    
    # STEP 2: Apply robust encoding fix
    logger.info("🔧 Step 2: Applying encoding fixes...")
    facility = apply_encoding_fix_to_dataframe(facility)
    
    # STEP 3: Apply coordinate corrections (Manual Override)
    if coordinate_corrections is not None:
        logger.info("📍 Step 3: Applying coordinate corrections...")
        try:
            coords_map = coordinate_corrections.set_index('Nombre_Original')[
                ['Latitud_Corregida', 'Longitud_Corregida']
            ].to_dict('index')
            facility = facility.apply(lambda row: update_coords(row, coords_map), axis=1)
            logger.info(f"✅ Coordinate corrections applied")
        except Exception as e:
            logger.warning(f"⚠️ Error applying coordinate corrections: {e}")
    
    # STEP 4: Ensure coordinate columns exist
    if 'latitude' not in facility.columns:
        facility['latitude'] = None
    if 'longitude' not in facility.columns:
        facility['longitude'] = None
    
    # STEP 5: Validation & Filtering
    logger.info("✅ Step 4: Validating and filtering coordinates...")
    
    # Remove facilities without coordinates
    initial_count = len(facility)
    facility = facility.dropna(subset=['latitude', 'longitude'])
    removed_no_coords = initial_count - len(facility)
    
    # Filter to Spain bounds
    facility = facility[
        (facility['latitude'].between(35, 44)) &      # Spain latitude range
        (facility['longitude'].between(-10, 5))       # Spain longitude range
    ]
    removed_outside_spain = len(facility.dropna(subset=['latitude', 'longitude'])) - len(facility)
    
    logger.info(f"📊 Processing results:")
    logger.info(f"   • Initial facilities: {initial_count}")
    logger.info(f"   • Removed (no coordinates): {removed_no_coords}")
    logger.info(f"   • Removed (outside Spain): {removed_outside_spain}")
    logger.info(f"   • Final facilities: {len(facility)}")
    
    return facility

def format_datetime_madrid(utc_datetime_str):
    """Convert UTC datetime to Madrid timezone and format user-friendly"""
    try:
        if pd.isna(utc_datetime_str):
            return "N/A"
        
        # Parse UTC datetime
        utc_dt = pd.to_datetime(utc_datetime_str, utc=True)
        
        # Convert to Madrid timezone
        madrid_tz = pytz.timezone('Europe/Madrid')
        madrid_dt = utc_dt.tz_convert(madrid_tz)
        
        # Format user-friendly
        formatted = madrid_dt.strftime("%d %b %Y, %H:%M")
        timezone_name = madrid_dt.strftime("%Z")
        
        return f"{formatted} ({timezone_name})"
    except Exception as e:
        return str(utc_datetime_str)

def extract_skills(skills_str):
    """Extract skills from comma-separated string"""
    if pd.isna(skills_str):
        return []
    return [s.strip() for s in str(skills_str).split(",") if s.strip()]

def load_data_from_files(data_dir='data'):
    """Load processed data from CSV files"""
    logger.info("=== 📂 LOADING DATA FROM FILES ===")
    
    # Load raw facility data
    facility_file = os.path.join(data_dir, 'raw_facilities.csv')
    if not os.path.exists(facility_file):
        logger.error(f"❌ Facility data file not found: {facility_file}")
        logger.error("Please run data.py first to fetch data from Metabase")
        return None, None
    
    facility_data = pd.read_csv(facility_file)
    logger.info(f"✅ Loaded facility data: {len(facility_data)} rows")
    
    # Load coordinate corrections (optional)
    corrections_file = os.path.join(data_dir, 'facilities_corrected_coords.csv')
    coordinate_corrections = None
    if os.path.exists(corrections_file):
        try:
            coordinate_corrections = pd.read_csv(corrections_file, sep=';')
            logger.info(f"✅ Loaded coordinate corrections: {len(coordinate_corrections)} entries")
        except Exception as e:
            logger.warning(f"⚠️ Could not load coordinate corrections: {e}")
    else:
        logger.info("ℹ️ No coordinate corrections file found (optional)")
    
    return facility_data, coordinate_corrections

def create_facilities_map(facilities_df):
    """Create HTML map showing only facilities"""
    
    logger.info("=== 🗺️ CREATING FACILITIES MAP ===")
    
    if facilities_df is None or facilities_df.empty:
        logger.error("❌ No facility data to create map")
        return None
    
    # Prepare facilities data for JavaScript
    facilities_data = []
    for _, row in facilities_df.iterrows():
        try:
            facility = {
                'id': str(row.get('facility_id', 'N/A')),
                'name': str(row.get('facility_name', 'N/A')),
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'address': str(row.get('address', 'N/A')),
                'city': str(row.get('city', 'N/A')),
                'specialization': extract_skills(row.get('specialization', '')) if pd.notna(row.get('specialization')) else [],
                'capacity': str(row.get('capacity', 'N/A')),
                'phone': str(row.get('phone', 'N/A')),
                'type': str(row.get('type', 'Healthcare Facility'))
            }
            facilities_data.append(facility)
        except Exception as e:
            logger.warning(f"⚠️ Error processing facility row: {e}")
            continue
    
    # Create HTML map
    html_content = f'''
<!DOCTYPE html>
<html>
<head>
    <title>Healthcare Facilities Map</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
        }}
        #map {{
            height: 100vh;
            width: 100%;
        }}
        .facility-marker {{
            font-size: 24px;
            text-align: center;
            border-radius: 50%;
            background: white;
            border: 2px solid white;
            width: 32px;
            height: 32px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        }}
        .facility-popup {{
            font-size: 14px;
            max-width: 400px;
        }}
        .facility-header {{
            margin: 0 0 10px 0;
            color: #2c3e50;
            text-align: center;
            font-size: 16px;
            font-weight: bold;
        }}
        .facility-details {{
            max-height: 300px;
            overflow-y: auto;
        }}
        .facility-item {{
            padding: 12px;
            margin-bottom: 8px;
            background: #f9f9f9;
            border-left: 4px solid #007bff;
            border-radius: 4px;
        }}
        .facility-name {{
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        }}
        .facility-info {{
            font-size: 12px;
            color: #666;
            line-height: 1.4;
        }}
        .company-title {{
            position: fixed;
            top: 20px;
            left: 20px;
            background: white;
            padding: 15px 25px;
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            z-index: 1000;
            font-size: 18px;
            font-weight: bold;
            color: #2c3e50;
            border: 2px solid #007bff;
        }}
        .stats-overlay {{
            position: fixed;
            bottom: 20px;
            left: 20px;
            background: white;
            padding: 15px 20px;
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            z-index: 1000;
            font-size: 14px;
            color: #2c3e50;
            border: 2px solid #007bff;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    
    <!-- Company Title -->
    <div class="company-title">
        🏥 Facilities Map
    </div>
    
    <!-- Stats Overlay -->
    <div class="stats-overlay">
        <strong>📊 Overview</strong><br>
        • Total Facilities: <strong>{len(facilities_data):,}</strong><br>
        • Locations Mapped: <strong>{len(facilities_data):,}</strong>
    </div>
    
    <script>
        // Embedded data
        const facilitiesData = {json.dumps(facilities_data, ensure_ascii=False)};
        
        // Global variables
        let map;
        let allMarkers = [];
        
        // Initialize map
        function initMap() {{
            map = L.map('map').setView([40.4, -3.7], 6);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '© OpenStreetMap contributors'
            }}).addTo(map);
            
            // Load all facilities
            loadAllFacilities();
        }}
        
        function loadAllFacilities() {{
            // Group facilities by location to handle multiple facilities at same coordinates
            const facilitiesGrouped = {{}};
            facilitiesData.forEach(fac => {{
                const key = fac.latitude.toFixed(4) + ',' + fac.longitude.toFixed(4);
                if (!facilitiesGrouped[key]) facilitiesGrouped[key] = [];
                facilitiesGrouped[key].push(fac);
            }});
            
            // Add facility markers
            Object.entries(facilitiesGrouped).forEach(([key, facs]) => {{
                const [lat, lon] = key.split(',').map(Number);
                
                // Create hospital emoji marker or company logo marker
                const hospitalIcon = L.divIcon({{
                    className: 'facility-marker',
                    html: '<img src="logo.png" style="width:22px;height:22px;" alt="Company Logo"/>',
                    iconSize: [32, 32],
                    iconAnchor: [16, 16],
                    popupAnchor: [0, -20]
                }});
                
                // Create popup content
                let popupContent = '<div class="facility-popup">';
                if (facs.length === 1) {{
                    const fac = facs[0];
                    popupContent += '<h4 class="facility-header">' + fac.name + '</h4>';
                    popupContent += '<div class="facility-item">';
                    popupContent += '<div class="facility-info">';
                    popupContent += '<strong>ID:</strong> ' + fac.id + '<br>';
                    popupContent += '<strong>City:</strong> ' + fac.city + '<br>';
                    if (fac.address !== 'N/A') {{
                        popupContent += '<strong>Address:</strong> ' + fac.address + '<br>';
                    }}
                    if (fac.phone !== 'N/A') {{
                        popupContent += '<strong>Phone:</strong> ' + fac.phone + '<br>';
                    }}
                    if (fac.capacity !== 'N/A') {{
                        popupContent += '<strong>Capacity:</strong> ' + fac.capacity + '<br>';
                    }}
                    if (fac.specialization.length > 0) {{
                        popupContent += '<strong>Specializations:</strong> ' + fac.specialization.join(', ');
                    }}
                    popupContent += '</div></div>';
                }} else {{
                    popupContent += '<h4 class="facility-header">' + facs.length + ' Facilities at this Location</h4>';
                    popupContent += '<div class="facility-details">';
                    
                    facs.forEach((fac, i) => {{
                        popupContent += '<div class="facility-item">';
                        popupContent += '<div class="facility-name">' + fac.name + '</div>';
                        popupContent += '<div class="facility-info">';
                        popupContent += '<strong>ID:</strong> ' + fac.id + ' | <strong>City:</strong> ' + fac.city + '<br>';
                        if (fac.address !== 'N/A') {{
                            popupContent += '<strong>Address:</strong> ' + fac.address + '<br>';
                        }}
                        if (fac.phone !== 'N/A') {{
                            popupContent += '<strong>Phone:</strong> ' + fac.phone + '<br>';
                        }}
                        if (fac.specialization.length > 0) {{
                            popupContent += '<strong>Specializations:</strong> ' + fac.specialization.join(', ');
                        }}
                        popupContent += '</div></div>';
                    }});
                    
                    popupContent += '</div>';
                }}
                popupContent += '</div>';
                
                // Create marker with hospital icon
                const marker = L.marker([lat, lon], {{
                    icon: hospitalIcon
                }}).bindPopup(popupContent);
                
                marker.addTo(map);
                allMarkers.push(marker);
            }});
            
            // Fit map to show all facilities
            if (allMarkers.length > 0) {{
                const group = new L.featureGroup(allMarkers);
                map.fitBounds(group.getBounds().pad(0.1));
            }}
        }}
        
        // Initialize map when page loads
        document.addEventListener('DOMContentLoaded', function() {{
            initMap();
        }});
    </script>
</body>
</html>
    '''
    
    return html_content

def main():
    """Main function to process data and generate map"""
    try:
        # Load data from files
        facility_data, coordinate_corrections = load_data_from_files()
        
        if facility_data is None:
            return False
        
        # Standardize dataframes
        facility_data, coordinate_corrections = standardize_dataframes(facility_data, coordinate_corrections)
        
        # Process facilities using the complete pipeline
        processed_facilities = process_facilities(facility_data, coordinate_corrections)
        
        if processed_facilities is None or processed_facilities.empty:
            logger.error("❌ No valid facilities after processing")
            return False
        
        # Generate the HTML map
        logger.info("🗺️ Generating HTML map...")
        html_content = create_facilities_map(processed_facilities)
        
        if html_content is None:
            logger.error("❌ Failed to generate HTML map")
            return False
        
        # Save the map
        map_filename = 'facilities_map.html'
        with open(map_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"✅ Facilities map saved as: {map_filename}")
        logger.info(f"📊 Final map contains {len(processed_facilities)} facilities")
        
        # Save processed data for reference
        data_dir = 'data'
        os.makedirs(data_dir, exist_ok=True)
        processed_facilities.to_csv(os.path.join(data_dir, 'processed_facilities.csv'), index=False)
        logger.info(f"✅ Processed data saved to: {data_dir}/processed_facilities.csv")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting Healthcare Facilities Map Generator")
    logger.info("=" * 50)
    
    success = main()
    
    if success:
        logger.info("🎉 Map generation completed successfully!")
        logger.info("📂 Generated files:")
        logger.info("   • facilities_map.html - Interactive map")
        logger.info("   • data/processed_facilities.csv - Processed data")
        logger.info("🌐 Open facilities_map.html in your browser to view the map")
    else:
        logger.error("💥 Map generation failed!")
        logger.error("Make sure you have run data.py first to fetch the data from Metabase") 