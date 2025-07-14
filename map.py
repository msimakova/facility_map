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
        row['latitude'] = coords_map[name]['latitud_corregida']
        row['longitude'] = coords_map[name]['longitud_corregida']
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
            # After standardization, column names are lowercase
            coords_map = coordinate_corrections.set_index('nombre_original')[
                ['latitud_corregida', 'longitud_corregida']
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

def clean_facility_id(val):
    """Convierte el facility_id a string sin decimales sobrantes."""
    try:
        if pd.isna(val):
            return ''
        val_str = str(val)
        if val_str.endswith('.0'):
            return val_str[:-2]
        return val_str
    except Exception:
        return str(val)

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

def load_facilities_and_shifts(data_dir='data'):
    """Carga instalaciones corregidas, shifts disponibles y ofertas, y asocia todo por facility_id sin filtrar instalaciones."""
    facilities_file = os.path.join(data_dir, 'all_corrected_facilities.csv')
    shifts_file = os.path.join(data_dir, 'available_shifts.csv')
    offers_file = os.path.join(data_dir, 'available_offers.csv')
    if not os.path.exists(facilities_file):
        logger.error(f"❌ Required file not found: {facilities_file}")
        return None, None, None
    facilities = pd.read_csv(facilities_file, sep=';')
    facilities.columns = facilities.columns.str.lower().str.replace(' ', '_')
    facilities['facility_id'] = facilities['facility_id'].apply(clean_facility_id)
    if os.path.exists(shifts_file):
        shifts = pd.read_csv(shifts_file)
        shifts.columns = shifts.columns.str.lower().str.replace(' ', '_')
        shifts['facility_id'] = shifts['facility_id'].apply(clean_facility_id)
    else:
        shifts = pd.DataFrame()
    if os.path.exists(offers_file):
        offers = pd.read_csv(offers_file)
        offers.columns = offers.columns.str.lower().str.replace(' ', '_')
        offers['facility_id'] = offers['facility_id'].apply(clean_facility_id)
        # Filtrar solo ofertas PUBLISHED
        if 'status' in offers.columns:
            offers = offers[offers['status'] == 'PUBLISHED']
        logger.info(f"✅ Loaded {len(offers)} published offers")
    else:
        offers = pd.DataFrame()
        logger.info("ℹ️ No offers file found")
    return facilities, shifts, offers

def create_facilities_map_with_shifts(facilities_df, shifts_df, offers_df=None):
    """Crea el HTML del mapa mostrando instalaciones, shifts y ofertas si existen"""
    logger.info("=== 🗺️ CREATING FACILITIES MAP WITH SHIFTS ===")
    if facilities_df is None or facilities_df.empty:
        logger.error("❌ No facilities to create map")
        return None
    # Agrupar shifts por facility_id si existen
    shifts_by_fac = shifts_df.groupby('facility_id') if shifts_df is not None and not shifts_df.empty else {}
    # Agrupar ofertas por facility_id si existen
    offers_by_fac = offers_df.groupby('facility_id') if offers_df is not None and not offers_df.empty else {}
    facilities_data = []
    for _, row in facilities_df.iterrows():
        fac_id = clean_facility_id(row.get('facility_id', row.get('nombre_original', '')))
        fac_shifts = shifts_by_fac.get_group(fac_id) if fac_id in getattr(shifts_by_fac, 'groups', {}) else None
        fac_offers = offers_by_fac.get_group(fac_id) if fac_id in getattr(offers_by_fac, 'groups', {}) else None
        try:
            # Calcular estadísticas de shifts
            shift_stats = {'total': 0, 'enf': 0, 'tcae': 0, 'offers': 0}
            shifts_list = []
            offers_list = []
            if fac_shifts is not None and not fac_shifts.empty:
                shift_stats['total'] = len(fac_shifts)
                shift_stats['enf'] = len(fac_shifts[fac_shifts['category'] == 'ENF'])
                shift_stats['tcae'] = len(fac_shifts[fac_shifts['category'] == 'TCAE'])
                shifts_list = [
                    {
                        'shift_id': str(s.get('id', '')),
                        'start_time': format_datetime_madrid(s.get('start_time_utc', '')),
                        'finish_time': format_datetime_madrid(s.get('finish_time_utc', '')),
                        'specialization': s.get('specialization_display_text', s.get('specialization', '')),
                        'category': s.get('category', ''),
                        'capacity': s.get('capacity', ''),
                    }
                    for _, s in fac_shifts.iterrows()
                ]
            # Calcular estadísticas de ofertas
            if fac_offers is not None and not fac_offers.empty:
                shift_stats['offers'] = len(fac_offers)
                offers_list = [
                    {
                        'offer_id': str(o.get('id', '')),
                        'external_id': str(o.get('external_id', '')),
                        'category': o.get('category', ''),
                        'skill': o.get('skill', ''),
                        'contract_type': o.get('contract_type', ''),
                        'salary_min': o.get('salary_min', ''),
                        'salary_max': o.get('salary_max', ''),
                        'salary_period': o.get('salary_period', ''),
                        'start_date': o.get('start_date', ''),
                        'status': o.get('status', ''),
                        'job_description': str(o.get('job_description', ''))[:100] + ('...' if len(str(o.get('job_description', ''))) > 100 else ''),
                    }
                    for _, o in fac_offers.iterrows()
                ]
            facility = {
                'id': fac_id,
                'name': str(row.get('nombre_correcto', row.get('facility_name', 'N/A'))),
                'city': str(row.get('ciudad', row.get('city', 'N/A'))),
                'address': str(row.get('direccion', row.get('address', 'N/A'))),
                'latitude': float(row.get('latitud_corregida', row.get('latitude', 0))),
                'longitude': float(row.get('longitud_corregida', row.get('longitude', 0))),
                'shift_stats': shift_stats,
                'shifts': shifts_list,
                'offers': offers_list
            }
            facilities_data.append(facility)
        except Exception as e:
            logger.warning(f"⚠️ Error processing facility row: {e}")
            continue
    # Crear HTML
    total_hospitals = len(facilities_data)
    html_content = f'''
<!DOCTYPE html>
<html>
<head>
    <title>Mapa de Centros Sanitarios</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        body {{ margin: 0; padding: 0; font-family: Arial, sans-serif; }}
        #map {{ height: 100vh; width: 100%; }}
        .facility-marker {{ font-size: 24px; text-align: center; border-radius: 50%; background: white; border: 2px solid white; width: 32px; height: 32px; display: flex; align-items: center; justify-content: center; box-shadow: 0 2px 8px rgba(0,0,0,0.3); }}
        .facility-popup {{ font-size: 14px; max-width: 400px; }}
        .facility-header {{ margin: 0 0 10px 0; color: #2c3e50; text-align: center; font-size: 16px; font-weight: bold; }}
        .shift-list {{ margin-top: 10px; max-height: 250px; overflow-y: auto; }}
        .shift-item {{ background: #f9f9f9; border-left: 4px solid #007bff; border-radius: 4px; padding: 8px; margin-bottom: 8px; }}
        .shift-title {{ font-weight: bold; }}
        .offer-item {{ background: #f0f8ff; border-left: 4px solid #28a745; border-radius: 4px; padding: 8px; margin-bottom: 8px; }}
        .offer-title {{ font-weight: bold; color: #28a745; }}
        .shift-stats {{ background: #e9f4ff; border: 1px solid #007bff; border-radius: 8px; padding: 12px; margin: 10px 0; }}
        .stat-row {{ display: flex; justify-content: space-between; margin: 4px 0; }}
        .stat-label {{ font-weight: bold; color: #2c3e50; }}
        .stat-value {{ color: #007bff; font-weight: bold; }}
        .company-title {{ position: absolute; top: 20px; left: 20px; background: white; border-radius: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.15); padding: 16px 32px; font-size: 24px; font-weight: bold; color: #2c3e50; z-index: 1000; border: 2px solid #007bff; }}
        .hospital-count {{ font-size: 16px; color: #007bff; margin-top: 8px; text-align: center; }}
        .filters {{ position: absolute; top: 20px; right: 20px; background: white; border-radius: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.15); padding: 16px; z-index: 1000; border: 2px solid #007bff; min-width: 250px; }}
        .filter-title {{ font-weight: bold; color: #2c3e50; margin-bottom: 10px; text-align: center; }}
        .filter-group {{ margin-bottom: 12px; }}
        .filter-label {{ font-weight: bold; color: #2c3e50; margin-bottom: 5px; display: block; }}
        .filter-checkbox {{ margin: 3px 0; }}
    </style>
</head>
<body>
    <div class="company-title">🏥 Mapa de Centros Sanitarios
        <div class="hospital-count">Mostrando <span id="visible-count">{total_hospitals}</span> centros</div>
    </div>
    <div class="filters">
        <div class="filter-title">🔍 Filtros</div>
        <div class="filter-group">
            <label class="filter-label">Mostrar solo:</label>
            <div class="filter-checkbox"><input type="checkbox" id="filter-with-shifts"> Centros con turnos</div>
            <div class="filter-checkbox"><input type="checkbox" id="filter-with-offers"> Centros con ofertas</div>
        </div>
        <div class="filter-group">
            <label class="filter-label">Categorías:</label>
            <div class="filter-checkbox"><input type="checkbox" id="filter-enf" checked> ENF (Enfermería)</div>
            <div class="filter-checkbox"><input type="checkbox" id="filter-tcae" checked> TCAE (Auxiliares)</div>
        </div>
    </div>
    <div id="map"></div>
    <script>
        const facilitiesData = {json.dumps(facilities_data, ensure_ascii=False)}
        let map;
        let allMarkers = [];
        let visibleMarkers = [];
        function getColorBySpecialization(especialidad) {{
            const colorMap = {{
                'Consulta de enfermería': '#007bff',
                'Médico': '#28a745',
                'Pediatría': '#e67e22',
                'Urgencias': '#e74c3c',
                'Farmacia': '#8e44ad',
                '': '#007bff',
            }};
            return colorMap[especialidad] || '#007bff';
        }}
        function initMap() {{
            map = L.map('map').setView([40.4, -3.7], 6);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ attribution: '© OpenStreetMap contributors' }}).addTo(map);
            loadAllFacilities();
            setupFilters();
        }}
        function isInSpain(lat, lon) {{
            return lat >= 35 && lat <= 44 && lon >= -10 && lon <= 5;
        }}
        function updateVisibleCount() {{
            document.getElementById('visible-count').textContent = visibleMarkers.length;
        }}
        function applyFilters() {{
            const filterWithShifts = document.getElementById('filter-with-shifts').checked;
            const filterWithOffers = document.getElementById('filter-with-offers').checked;
            const filterENF = document.getElementById('filter-enf').checked;
            const filterTCAE = document.getElementById('filter-tcae').checked;
            visibleMarkers = [];
            allMarkers.forEach(marker => {{
                const fac = marker.facilityData;
                let show = true;
                if (filterWithShifts && fac.shift_stats.total === 0) show = false;
                if (filterWithOffers && fac.shift_stats.offers === 0) show = false;
                if (!filterENF && !filterTCAE) show = false;
                else if (!filterENF && fac.shift_stats.enf > 0 && fac.shift_stats.tcae === 0) show = false;
                else if (!filterTCAE && fac.shift_stats.tcae > 0 && fac.shift_stats.enf === 0) show = false;
                if (show) {{
                    if (!map.hasLayer(marker)) map.addLayer(marker);
                    visibleMarkers.push(marker);
                }} else {{
                    if (map.hasLayer(marker)) map.removeLayer(marker);
                }}
            }});
            updateVisibleCount();
        }}
        function setupFilters() {{
            document.getElementById('filter-with-shifts').addEventListener('change', applyFilters);
            document.getElementById('filter-with-offers').addEventListener('change', applyFilters);
            document.getElementById('filter-enf').addEventListener('change', applyFilters);
            document.getElementById('filter-tcae').addEventListener('change', applyFilters);
        }}
        function loadAllFacilities() {{
            let spainMarkers = [];
            facilitiesData.forEach(fac => {{
                const lat = fac.latitude;
                const lon = fac.longitude;
                const hospitalIcon = L.divIcon({{ className: 'facility-marker', html: '<img src="logo.png" style="width:22px;height:22px;" alt="Logo"/>', iconSize: [32, 32], iconAnchor: [16, 16], popupAnchor: [0, -20] }});
                let popupContent = '<div class="facility-popup">';
                popupContent += '<h4 class="facility-header">' + fac.name + '</h4>';
                // popupContent += '<div><strong>ID:</strong> ' + fac.id + '</div>'; // REMOVED ID FIELD
                popupContent += '<div><strong>Ciudad:</strong> ' + fac.city + '</div>';
                popupContent += '<div><strong>Dirección:</strong> ' + fac.address + '</div>';
                popupContent += '<div class="shift-stats">';
                popupContent += '<div class="stat-row"><span class="stat-label">Total turnos:</span><span class="stat-value">' + fac.shift_stats.total + '</span></div>';
                popupContent += '<div class="stat-row"><span class="stat-label">ENF (Enfermería):</span><span class="stat-value">' + fac.shift_stats.enf + '</span></div>';
                popupContent += '<div class="stat-row"><span class="stat-label">TCAE (Auxiliares):</span><span class="stat-value">' + fac.shift_stats.tcae + '</span></div>';
                popupContent += '<div class="stat-row"><span class="stat-label">Ofertas:</span><span class="stat-value">' + fac.shift_stats.offers + '</span></div>';
                popupContent += '</div>';
                if (fac.shifts && fac.shifts.length > 0) {{
                    popupContent += '<div class="shift-list">';
                    popupContent += '<div style="font-weight:bold; margin-bottom:5px; color:#007bff;">Detalles de turnos:</div>';
                    fac.shifts.forEach(shift => {{
                        const color = getColorBySpecialization(shift.specialization);
                        popupContent += '<div class="shift-item">';
                        popupContent += `<div class="shift-title" style="color:${{color}}">` + (shift.specialization || 'Turno') + '</div>';
                        popupContent += '<div><strong>Inicio:</strong> ' + shift.start_time + '</div>';
                        popupContent += '<div><strong>Fin:</strong> ' + shift.finish_time + '</div>';
                        popupContent += '<div><strong>Categoría:</strong> ' + shift.category + '</div>';
                        popupContent += '<div><strong>Plazas:</strong> ' + shift.capacity + '</div>';
                        popupContent += '</div>';
                    }});
                    popupContent += '</div>';
                }}
                if (fac.offers && fac.offers.length > 0) {{
                    popupContent += '<div style="font-weight:bold; margin-top:15px; color:#28a745;">Detalles de ofertas:</div>';
                    fac.offers.forEach(offer => {{
                        popupContent += '<div class="offer-item">';
                        popupContent += `<div class="offer-title" style="color:#28a745;">` + (offer.external_id || 'Oferta') + '</div>';
                        // Traducción de tipo de contrato
                        let contrato = offer.contract_type;
                        if (contrato === 'PERMANENT') contrato = 'Indefinido';
                        else if (contrato === 'TEMPORAL') contrato = 'Temporal';
                        else if (contrato === 'PART_TIME') contrato = 'Parcial';
                        else if (contrato === 'INTERIM') contrato = 'Interino';
                        else if (contrato === 'FIXED_TERM') contrato = 'Temporal';
                        else if (contrato === 'INDEFINITE') contrato = 'Indefinido';
                        // Traducción de categoría
                        let categoria = offer.category;
                        if (categoria === 'ENF') categoria = 'Enfermería';
                        else if (categoria === 'TCAE') categoria = 'Auxiliar/TCAE';
                        if (categoria && categoria !== 'NaN' && categoria !== '') {{
                            popupContent += '<div><strong>Categoría:</strong> ' + categoria + '</div>';
                        }}
                        if (offer.skill && offer.skill !== 'NaN' && offer.skill !== '') {{
                            popupContent += '<div><strong>Habilidad:</strong> ' + offer.skill + '</div>';
                        }}
                        if (contrato && contrato !== 'NaN' && contrato !== '') {{
                            popupContent += '<div><strong>Tipo de Contrato:</strong> ' + contrato + '</div>';
                        }}
                        if ((offer.salary_min && offer.salary_min !== 'NaN' && offer.salary_min !== '') || 
                            (offer.salary_max && offer.salary_max !== 'NaN' && offer.salary_max !== '')) {{
                            let salaryText = '';
                            if (offer.salary_min && offer.salary_min !== 'NaN' && offer.salary_min !== '') {{
                                let formattedMin = parseFloat(offer.salary_min).toLocaleString('es-ES');
                                salaryText += formattedMin;
                            }}
                            if (offer.salary_max && offer.salary_max !== 'NaN' && offer.salary_max !== '') {{
                                if (salaryText) salaryText += ' - ';
                                let formattedMax = parseFloat(offer.salary_max).toLocaleString('es-ES');
                                salaryText += formattedMax;
                            }}
                            if (offer.salary_period && offer.salary_period !== 'NaN' && offer.salary_period !== '') {{
                                let periodText = offer.salary_period;
                                if (periodText === 'YEAR') periodText = 'año';
                                else if (periodText === 'MONTH') periodText = 'mes';
                                salaryText += ' ' + periodText;
                            }}
                            popupContent += '<div><strong>Salario:</strong> ' + salaryText + '</div>';
                        }}
                        if (offer.start_date && offer.start_date !== 'NaN' && offer.start_date !== '' && offer.start_date !== 'NaT') {{
                            popupContent += '<div><strong>Fecha de Inicio:</strong> ' + offer.start_date + '</div>';
                        }}
                        if (offer.job_description && offer.job_description !== 'NaN' && offer.job_description !== '' && offer.job_description !== 'nan') {{
                            popupContent += '<div><strong>Descripción:</strong> ' + offer.job_description + '</div>';
                        }}
                        popupContent += '</div>';
                    }});
                    popupContent += '</div>';
                }}
                popupContent += '</div>';
                const marker = L.marker([lat, lon], {{ icon: hospitalIcon }}).bindPopup(popupContent);
                marker.facilityData = fac;
                marker.addTo(map);
                allMarkers.push(marker);
                visibleMarkers.push(marker);
                if (isInSpain(lat, lon)) {{
                    spainMarkers.push(marker);
                }}
            }});
            updateVisibleCount();
            if (spainMarkers.length > 0) {{
                const group = new L.featureGroup(spainMarkers);
                map.fitBounds(group.getBounds().pad(0.1));
            }}
        }}
        document.addEventListener('DOMContentLoaded', function() {{ initMap(); }});
    </script>
</body>
</html>
    '''
    return html_content

def main():
    try:
        facilities_df, shifts_df, offers_df = load_facilities_and_shifts()
        if facilities_df is None or shifts_df is None:
            logger.error("❌ No valid facilities or shifts after processing")
            return False
        logger.info("🗺️ Generating HTML map with available shifts...")
        html_content = create_facilities_map_with_shifts(facilities_df, shifts_df, offers_df)
        if html_content is None:
            logger.error("❌ Failed to generate HTML map")
            return False
        map_filename = 'index.html'
        with open(map_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"✅ Facilities map saved as: {map_filename}")
        logger.info(f"📊 Final map contains {len(facilities_df)} facilities with available shifts")
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
        logger.info("   • index.html - Interactive map")
        logger.info("🌐 Open index.html in your browser to view the map")
    else:
        logger.error("💥 Map generation failed!")
        logger.error("Make sure you have run data.py first to fetch the data from Metabase") 