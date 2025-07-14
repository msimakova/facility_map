#!/usr/bin/env python3
"""
Comprehensive Coordinate Checker and Geocoder
Checks facility coordinates, geocodes problematic ones, and updates corrections file.
"""

import pandas as pd
import requests
import time
import logging
import os
from dotenv import load_dotenv

# Try to import geocoding libraries
try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoordinateChecker:
    """Comprehensive coordinate checking and geocoding system"""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        
        # Data directories
        self.data_dir = 'data'
        self.raw_file = os.path.join(self.data_dir, 'raw_facilities.csv')
        self.corrections_file = os.path.join(self.data_dir, 'facilities_corrected_coords.csv')
        self.new_geocoded_file = os.path.join(self.data_dir, 'newly_geocoded_facilities.csv')
        
    def load_data(self):
        """Load raw facility data"""
        if not os.path.exists(self.raw_file):
            logger.error(f"❌ Raw facilities file not found: {self.raw_file}")
            logger.error("Please run data.py first to fetch data from Metabase")
            return None
        
        raw_data = pd.read_csv(self.raw_file)
        logger.info(f"✅ Loaded {len(raw_data)} raw facilities")
        return raw_data
    
    def load_corrections(self):
        """Load existing coordinate corrections"""
        corrections = None
        if os.path.exists(self.corrections_file):
            try:
                corrections = pd.read_csv(self.corrections_file, sep=';')
                logger.info(f"✅ Loaded {len(corrections)} existing corrections")
            except Exception as e:
                logger.warning(f"⚠️ Could not load corrections: {e}")
        else:
            logger.info("ℹ️ No corrections file found - will create new one")
        
        return corrections
    
    def analyze_coordinates(self, raw_data):
        """Analyze coordinate quality and identify problems"""
        logger.info("🔍 Analyzing coordinate quality...")
        
        analysis = {
            'total': len(raw_data),
            'good_coordinates': 0,
            'missing_coordinates': 0,
            'zero_coordinates': 0,
            'extreme_coordinates': 0,
            'default_coordinates': 0,
            'outside_spain': 0,
            'already_corrected': 0,
            'needs_geocoding': [],
            'corrected_coordinates': []
        }
        
        corrections = self.load_corrections()
        corrected_names = set()
        if corrections is not None:
            corrected_names = set(corrections['Nombre_Original'].str.lower())
        
        for _, facility in raw_data.iterrows():
            name = facility['name'].lower()
            lat = facility['address_latitude']
            lon = facility['address_longitude']
            
            # Check if already corrected
            if name in corrected_names:
                analysis['already_corrected'] += 1
                # Get the corrected coordinates
                correction = corrections[corrections['Nombre_Original'].str.lower() == name].iloc[0]
                analysis['corrected_coordinates'].append({
                    'name': facility['name'],
                    'original_lat': lat,
                    'original_lon': lon,
                    'corrected_lat': correction['Latitud_Corregida'],
                    'corrected_lon': correction['Longitud_Corregida'],
                    'reason': correction['Fuente_Problema']
                })
                continue
            
            # Analyze coordinate quality
            if pd.isna(lat) or pd.isna(lon):
                analysis['missing_coordinates'] += 1
                analysis['needs_geocoding'].append({
                    'name': facility['name'],
                    'address': facility['address'],
                    'city': facility['address_city'],
                    'reason': 'Missing coordinates',
                    'original_lat': lat,
                    'original_lon': lon
                })
            elif lat == 0.0 and lon == 0.0:
                analysis['zero_coordinates'] += 1
                analysis['needs_geocoding'].append({
                    'name': facility['name'],
                    'address': facility['address'],
                    'city': facility['address_city'],
                    'reason': 'Zero coordinates',
                    'original_lat': lat,
                    'original_lon': lon
                })
            elif abs(lat) > 100 or abs(lon) > 100:
                analysis['extreme_coordinates'] += 1
                analysis['needs_geocoding'].append({
                    'name': facility['name'],
                    'address': facility['address'],
                    'city': facility['address_city'],
                    'reason': 'Extreme coordinate values',
                    'original_lat': lat,
                    'original_lon': lon
                })
            elif lat == 1.0 and lon == 1.0:
                analysis['default_coordinates'] += 1
                analysis['needs_geocoding'].append({
                    'name': facility['name'],
                    'address': facility['address'],
                    'city': facility['address_city'],
                    'reason': 'Default coordinates',
                    'original_lat': lat,
                    'original_lon': lon
                })
            elif not (35 <= lat <= 44) or not (-10 <= lon <= 5):
                analysis['outside_spain'] += 1
                analysis['needs_geocoding'].append({
                    'name': facility['name'],
                    'address': facility['address'],
                    'city': facility['address_city'],
                    'reason': 'Outside Spain bounds',
                    'original_lat': lat,
                    'original_lon': lon
                })
            else:
                analysis['good_coordinates'] += 1
        
        return analysis
    
    def print_analysis(self, analysis):
        """Print coordinate analysis results"""
        logger.info("📊 COORDINATE ANALYSIS RESULTS:")
        logger.info("=" * 40)
        logger.info(f"📈 Total facilities: {analysis['total']}")
        logger.info(f"✅ Good coordinates: {analysis['good_coordinates']}")
        logger.info(f"⏭️ Already corrected: {analysis['already_corrected']}")
        logger.info(f"❌ Missing coordinates: {analysis['missing_coordinates']}")
        logger.info(f"❌ Zero coordinates: {analysis['zero_coordinates']}")
        logger.info(f"❌ Extreme coordinates: {analysis['extreme_coordinates']}")
        logger.info(f"❌ Default coordinates: {analysis['default_coordinates']}")
        logger.info(f"❌ Outside Spain: {analysis['outside_spain']}")
        logger.info(f"🔄 Need geocoding: {len(analysis['needs_geocoding'])}")
        logger.info("=" * 40)
        
        if analysis['corrected_coordinates']:
            logger.info("🔧 Already corrected facilities:")
            for facility in analysis['corrected_coordinates']:
                logger.info(f"   • {facility['name']}")
                logger.info(f"     Original: ({facility['original_lat']}, {facility['original_lon']})")
                logger.info(f"     Corrected: ({facility['corrected_lat']}, {facility['corrected_lon']})")
                logger.info(f"     Reason: {facility['reason']}")
                logger.info()
        
        if analysis['needs_geocoding']:
            logger.info("📍 Facilities needing geocoding:")
            for facility in analysis['needs_geocoding']:
                logger.info(f"   • {facility['name']} ({facility['reason']})")
                logger.info(f"     Current: ({facility['original_lat']}, {facility['original_lon']})")
                logger.info()
    
    def geocode_facility(self, facility):
        """Geocode a single facility using multiple methods"""
        
        # Try Google Maps API first (if available)
        if self.api_key:
            result = self._geocode_google_maps(facility)
            if result:
                return result
        
        # Try OpenStreetMap Nominatim (free)
        if GEOPY_AVAILABLE:
            result = self._geocode_nominatim(facility)
            if result:
                return result
        
        # Try simple address lookup
        result = self._geocode_simple_lookup(facility)
        if result:
            return result
        
        logger.warning(f"⚠️ Could not geocode: {facility['name']}")
        return None
    
    def _geocode_google_maps(self, facility):
        """Geocode using Google Maps API"""
        try:
            # Construct address
            address = facility['address']
            city = facility['city']
            full_address = f"{address}, {city}, Spain"
            
            # Prepare request
            params = {
                'address': full_address,
                'key': self.api_key
            }
            
            # Make request
            response = requests.get(self.base_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data['status'] == 'OK' and data['results']:
                    result = data['results'][0]
                    location = result['geometry']['location']
                    
                    lat = location['lat']
                    lng = location['lng']
                    
                    logger.info(f"✅ Geocoded (Google Maps): {facility['name']} -> ({lat}, {lng})")
                    
                    return {
                        'Nombre_Original': facility['name'],
                        'Nombre_Correcto': facility['name'],
                        'Ciudad': facility['city'],
                        'Tipo': 'TO_BE_DETERMINED',
                        'Direccion': facility['address'],
                        'Latitud_Corregida': lat,
                        'Longitud_Corregida': lng,
                        'Fuente_Problema': f"Geocoded via Google Maps API - {result['formatted_address']}"
                    }
                else:
                    logger.warning(f"⚠️ No Google Maps results for: {facility['name']} ({data['status']})")
                    return None
            else:
                logger.error(f"❌ Google Maps API request failed: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error geocoding with Google Maps {facility['name']}: {e}")
            return None
    
    def _geocode_nominatim(self, facility):
        """Geocode using OpenStreetMap Nominatim (free)"""
        try:
            geolocator = Nominatim(user_agent="facility_map_geocoder")
            
            # Construct address
            address = facility['address']
            city = facility['city']
            full_address = f"{address}, {city}, Spain"
            
            # Geocode
            location = geolocator.geocode(full_address, timeout=10)
            
            if location:
                lat = location.latitude
                lng = location.longitude
                
                logger.info(f"✅ Geocoded (Nominatim): {facility['name']} -> ({lat}, {lng})")
                
                return {
                    'Nombre_Original': facility['name'],
                    'Nombre_Correcto': facility['name'],
                    'Ciudad': facility['city'],
                    'Tipo': 'TO_BE_DETERMINED',
                    'Direccion': facility['address'],
                    'Latitud_Corregida': lat,
                    'Longitud_Corregida': lng,
                    'Fuente_Problema': f"Geocoded via OpenStreetMap Nominatim - {location.address}"
                }
            else:
                logger.warning(f"⚠️ No Nominatim results for: {facility['name']}")
                return None
                
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            logger.warning(f"⚠️ Nominatim timeout/unavailable for {facility['name']}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error geocoding with Nominatim {facility['name']}: {e}")
            return None
    
    def _geocode_simple_lookup(self, facility):
        """Simple geocoding using city-based lookup"""
        try:
            # Simple city-based coordinates for Spain
            city_coords = {
                'barcelona': (41.3851, 2.1734),
                'madrid': (40.4168, -3.7038),
                'valencia': (39.4699, -0.3763),
                'sevilla': (37.3891, -5.9845),
                'zaragoza': (41.6488, -0.8891),
                'málaga': (36.7213, -4.4217),
                'murcia': (37.9922, -1.1307),
                'palma': (39.5696, 2.6502),
                'las palmas': (28.1235, -15.4366),
                'bilbao': (43.2627, -2.9253),
                'alicante': (38.3452, -0.4815),
                'cordoba': (37.8882, -4.7794),
                'valladolid': (41.6523, -4.7245),
                'vigo': (42.2406, -8.7207),
                'gijón': (43.5453, -5.6619),
                'granada': (37.1765, -3.5976),
                'oviedo': (43.3623, -5.8493),
                'santander': (43.4623, -3.8099),
                'tarrasa': (41.5606, 2.0104),
                'sabadell': (41.5463, 2.1074),
                'alcorcón': (40.3494, -3.8313),
                'móstoles': (40.3233, -3.8644),
                'fuenlabrada': (40.2842, -3.7942),
                'badalona': (41.4500, 2.2474),
                'hospitalet': (41.3597, 2.0998),
                'alcalá de henares': (40.4820, -3.3635),
                'terrassa': (41.5606, 2.0104),
                'jerez de la frontera': (36.6866, -6.1372),
                'marbella': (36.5097, -4.8860),
                'león': (42.5987, -5.5671),
                'tarragona': (41.1187, 1.2453),
                'lleida': (41.6148, 0.6268),
                'castellón': (39.9864, -0.0513),
                'burgos': (42.3408, -3.6997),
                'salamanca': (40.9645, -5.6630),
                'albacete': (38.9952, -1.8557),
                'huelva': (37.2614, -6.9447),
                'logroño': (42.4627, -2.4449),
                'cádiz': (36.5297, -6.2926),
                'lucena': (37.4088, -4.4852),
                'jaén': (37.7796, -3.7849),
                'orense': (42.3355, -7.8639),
                'girona': (41.9794, 2.8214),
                'lugo': (43.0097, -7.5560),
                'cáceres': (39.4765, -6.3722),
                'talavera de la reina': (39.9603, -4.8303),
                'santiago de compostela': (42.8805, -8.5456),
                'lérida': (41.6148, 0.6268),
                'cartagena': (37.6057, -0.9913),
                'toledo': (39.8584, -4.0226),
                'elche': (38.2672, -0.6987),
                'oviedo': (43.3623, -5.8493),
                'guadalajara': (40.6296, -3.1665),
                'tudela': (42.0644, -1.6044),
                'ceuta': (35.8894, -5.3213),
                'melilla': (35.2923, -2.9381)
            }
            
            city = facility['city'].lower()
            
            # Try exact city match
            if city in city_coords:
                lat, lng = city_coords[city]
                logger.info(f"✅ Geocoded (City lookup): {facility['name']} -> ({lat}, {lng})")
                
                return {
                    'Nombre_Original': facility['name'],
                    'Nombre_Correcto': facility['name'],
                    'Ciudad': facility['city'],
                    'Tipo': 'TO_BE_DETERMINED',
                    'Direccion': facility['address'],
                    'Latitud_Corregida': lat,
                    'Longitud_Corregida': lng,
                    'Fuente_Problema': f"Geocoded via city lookup - {facility['city']}"
                }
            
            # Try partial city match
            for known_city, coords in city_coords.items():
                if known_city in city or city in known_city:
                    lat, lng = coords
                    logger.info(f"✅ Geocoded (City lookup): {facility['name']} -> ({lat}, {lng})")
                    
                    return {
                        'Nombre_Original': facility['name'],
                        'Nombre_Correcto': facility['name'],
                        'Ciudad': facility['city'],
                        'Tipo': 'TO_BE_DETERMINED',
                        'Direccion': facility['address'],
                        'Latitud_Corregida': lat,
                        'Longitud_Corregida': lng,
                        'Fuente_Problema': f"Geocoded via city lookup - {facility['city']}"
                    }
            
            logger.warning(f"⚠️ No city match found for: {facility['name']} ({facility['city']})")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error in simple lookup for {facility['name']}: {e}")
            return None
    
    def geocode_facilities(self, facilities_to_geocode):
        """Geocode multiple facilities"""
        if not facilities_to_geocode:
            logger.info("✅ No facilities need geocoding")
            return []
        
        logger.info(f"🔄 Starting geocoding for {len(facilities_to_geocode)} facilities...")
        
        geocoded_results = []
        
        for i, facility in enumerate(facilities_to_geocode, 1):
            logger.info(f"📍 [{i}/{len(facilities_to_geocode)}] Geocoding: {facility['name']}")
            
            result = self.geocode_facility(facility)
            
            if result:
                geocoded_results.append(result)
            
            # Rate limiting
            time.sleep(0.1)  # 100ms delay
        
        logger.info(f"✅ Geocoding completed: {len(geocoded_results)} successful")
        return geocoded_results
    
    def update_corrections_file(self, new_corrections):
        """Save new geocoded results to a separate file (don't modify original corrections)"""
        # Save new geocoded results to separate file
        new_corrections_df = pd.DataFrame(new_corrections)
        new_corrections_df.to_csv(self.new_geocoded_file, sep=';', index=False)
        logger.info(f"✅ Saved new geocoded facilities to: {self.new_geocoded_file}")
        logger.info(f"📊 New geocoded facilities: {len(new_corrections_df)}")
        
        # Also show how to combine them manually
        logger.info("💡 To combine with original corrections:")
        logger.info(f"   • Original corrections: {self.corrections_file}")
        logger.info(f"   • New geocoded: {self.new_geocoded_file}")
        logger.info("   • You can manually merge them if needed")
        
        return new_corrections_df
    
    def save_final_combined(self, geocoded_results):
        """Combina correcciones manuales y nuevas geocodificadas en un solo archivo final, sin duplicados y con facility_id."""
        # Cargar correcciones manuales
        corrections = None
        if os.path.exists(self.corrections_file):
            corrections = pd.read_csv(self.corrections_file, sep=';')
        else:
            corrections = pd.DataFrame()
        # Convertir resultados nuevos a DataFrame
        df_geocoded = pd.DataFrame(geocoded_results)
        # Cargar raw para obtener ids
        raw = pd.read_csv(self.raw_file)
        name_to_id = dict(zip(raw['name'].str.lower(), raw['id']))
        # Añadir facility_id a ambos dataframes
        if not corrections.empty:
            corrections['facility_id'] = corrections['Nombre_Original'].str.lower().map(name_to_id)
        if not df_geocoded.empty:
            df_geocoded['facility_id'] = df_geocoded['Nombre_Original'].str.lower().map(name_to_id)
        # Unificar columnas
        if not corrections.empty:
            corrections.columns = [c.strip() for c in corrections.columns]
        if not df_geocoded.empty:
            df_geocoded.columns = [c.strip() for c in df_geocoded.columns]
        # Concatenar y eliminar duplicados por 'Nombre_Original' (prioridad a correcciones manuales)
        df_all = pd.concat([corrections, df_geocoded], ignore_index=True)
        df_all = df_all.drop_duplicates(subset=['Nombre_Original'], keep='first')
        # Guardar archivo final
        output_file = os.path.join(self.data_dir, 'all_corrected_facilities.csv')
        df_all.to_csv(output_file, sep=';', index=False, encoding='utf-8')
        logger.info(f"✅ Archivo combinado generado: {output_file} ({len(df_all)} instalaciones)")
        return output_file
    
    def run_full_check(self):
        """Run the complete coordinate checking and geocoding process (geocode all except already-corrected)"""
        logger.info("🚀 Starting comprehensive coordinate check")
        logger.info("=" * 50)
        
        # Load data
        raw_data = self.load_data()
        if raw_data is None:
            return False
        
        # Load corrections
        corrections = self.load_corrections()
        corrected_names = set()
        if corrections is not None:
            corrected_names = set(corrections['Nombre_Original'].str.lower())
        
        # Build list of facilities to geocode: all except already-corrected
        facilities_to_geocode = []
        already_corrected = []
        for _, facility in raw_data.iterrows():
            name = facility['name'].lower()
            if name in corrected_names:
                already_corrected.append(facility['name'])
                continue
            # Build dict for geocoding
            facilities_to_geocode.append({
                'name': facility['name'],
                'address': facility.get('address', ''),
                'city': facility.get('address_city', '') or facility.get('city', ''),
            })
        
        logger.info(f"⏭️ Already corrected: {len(already_corrected)}")
        logger.info(f"🔄 Will geocode: {len(facilities_to_geocode)} facilities")
        if already_corrected:
            logger.info("🔧 Already corrected facilities:")
            for n in already_corrected:
                logger.info(f"   • {n}")
        
        # Check if any geocoding method is available
        if not self.api_key and not GEOPY_AVAILABLE:
            logger.warning("⚠️ No geocoding methods available")
            logger.warning("Options:")
            logger.warning("  1. Add GOOGLE_MAPS_API_KEY to your .env file for Google Maps")
            logger.warning("  2. Install geopy: pip install geopy for OpenStreetMap")
            logger.warning("  3. Use city-based lookup (already available)")
            logger.warning("You can still view the analysis above")
            return True
        
        # Geocode all facilities except already-corrected
        geocoded_results = self.geocode_facilities(facilities_to_geocode)
        # Guardar archivo combinado final
        self.save_final_combined(geocoded_results)
        logger.info("🎉 Coordinate checking y combinación completadas!")
        logger.info("💡 Next steps:")
        logger.info("   1. Revisa el archivo combinado all_corrected_facilities.csv")
        logger.info("   2. Ejecuta 'python map.py' para generar el mapa")
        return True

def main():
    """Main function"""
    try:
        checker = CoordinateChecker()
        success = checker.run_full_check()
        
        if success:
            logger.info("✅ Process completed successfully!")
        else:
            logger.error("❌ Process failed!")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        return False

if __name__ == "__main__":
    main() 