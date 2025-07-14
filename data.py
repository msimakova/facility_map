"""
Metabase Data Fetcher
Connects to Metabase via API and fetches data from questions/cards.
Supports both username/password and API key authentication.
"""

import pandas as pd
import os
import requests
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetabaseDataFetcher:
    """Enhanced class to fetch data from Metabase API with flexible authentication"""
    
    def __init__(self):
        # Check if .env file exists
        if not os.path.exists('.env'):
            logger.error("❌ .env file not found in the current directory")
            logger.error("Please create a .env file with the following variables:")
            logger.error("  Option 1 - Username/Password:")
            logger.error("    METABASE_URL=https://your-metabase-url.com")
            logger.error("    METABASE_USERNAME=your_username")
            logger.error("    METABASE_PASSWORD=your_password")
            logger.error("  Option 2 - API Key:")
            logger.error("    METABASE_URL=https://your-metabase-url.com")
            logger.error("    METABASE_API_KEY=your_api_key")
            raise FileNotFoundError(".env file is required but was not found")
        
        # Load variables from .env file
        load_dotenv(dotenv_path='.env')
        
        # Get credentials from .env
        self.metabase_url = os.getenv('METABASE_URL')
        self.username = os.getenv('METABASE_USERNAME')
        self.password = os.getenv('METABASE_PASSWORD')
        self.api_key = os.getenv('METABASE_API_KEY')
        
        # Validate URL
        if not self.metabase_url:
            logger.error("❌ Missing required variable in .env file: METABASE_URL")
            raise ValueError("METABASE_URL is required in .env file")
        
        # Determine authentication method
        self.auth_method = None
        if self.api_key:
            self.auth_method = 'api_key'
            logger.info("🔑 Using API Key authentication")
        elif self.username and self.password:
            self.auth_method = 'username_password'
            logger.info("👤 Using Username/Password authentication")
        else:
            logger.error("❌ Missing authentication credentials in .env file")
            logger.error("Please provide either:")
            logger.error("  - METABASE_API_KEY for API key authentication, OR")
            logger.error("  - Both METABASE_USERNAME and METABASE_PASSWORD for username/password authentication")
            raise ValueError("Missing authentication credentials in .env file")
        
        self.metabase_url = self.metabase_url.rstrip('/')
        self.session = requests.Session()
        self.session_token = None
        
        # Setup authentication
        if not self.setup_authentication():
            raise Exception("Failed to authenticate with Metabase")
    
    def setup_authentication(self) -> bool:
        """Setup authentication based on available credentials"""
        if self.auth_method == 'api_key':
            return self.setup_api_key_auth()
        elif self.auth_method == 'username_password':
            return self.login()
        return False
    
    def setup_api_key_auth(self) -> bool:
        """Setup API key authentication"""
        try:
            # For API key authentication, we add the key to headers
            self.session.headers.update({'X-API-KEY': self.api_key})
            logger.info("✅ API Key authentication configured")
            return True
        except Exception as e:
            logger.error(f"❌ API Key authentication setup failed: {str(e)}")
            return False
    
    def login(self) -> bool:
        """Login to Metabase using username/password and get session token"""
        try:
            base_url = self.metabase_url.rstrip('/').split('/')[0] + '//' + self.metabase_url.rstrip('/').split('/')[2]
            login_url = f"{base_url}/api/session"
            
            logger.info(f"Attempting to login to Metabase at: {base_url}")
            logger.info(f"Using username: {self.username}")
            
            request_data = {
                "username": self.username,
                "password": self.password
            }
            
            response = self.session.post(login_url, json=request_data, timeout=10)
            
            if response.status_code == 200:
                response_data = response.json()
                if not response_data or 'id' not in response_data:
                    logger.error("❌ Login response missing session ID")
                    return False
                    
                self.session_token = response_data['id']
                self.session.headers.update({'X-Metabase-Session': self.session_token})
                logger.info("✅ Successfully logged into Metabase")
                return True
            else:
                logger.error(f"❌ Login failed with status code: {response.status_code}")
                logger.error(f"Response content: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Login error: {str(e)}")
            return False
    
    def list_available_questions(self, limit: int = 20) -> pd.DataFrame:
        """List available questions/cards that the API key has access to"""
        try:
            # Try to get cards/questions
            cards_url = f"{self.metabase_url}/api/card"
            
            logger.info(f"🔍 Fetching available questions from Metabase...")
            
            response = self.session.get(cards_url, timeout=30)
            
            if response.status_code == 200:
                cards_data = response.json()
                
                if isinstance(cards_data, list) and cards_data:
                    # Create a DataFrame with useful info
                    cards_info = []
                    for card in cards_data[:limit]:  # Limit to avoid too much output
                        cards_info.append({
                            'id': card.get('id'),
                            'name': card.get('name'),
                            'description': card.get('description', 'N/A'),
                            'collection_name': card.get('collection', {}).get('name', 'N/A'),
                            'created_at': card.get('created_at', 'N/A'),
                            'updated_at': card.get('updated_at', 'N/A')
                        })
                    
                    df = pd.DataFrame(cards_info)
                    logger.info(f"✅ Found {len(df)} available questions")
                    return df
                else:
                    logger.warning(f"⚠️ No questions found or unexpected format")
                    return pd.DataFrame()
                    
            else:
                logger.error(f"❌ Failed to fetch questions: {response.status_code} - {response.text}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error fetching questions: {e}")
            return pd.DataFrame()

    def fetch_question_data(self, question_id: int, description: str = "") -> pd.DataFrame:
        """Fetch data from a Metabase question/card"""
        try:
            query_url = f"{self.metabase_url}/api/card/{question_id}/query/json"
            
            logger.info(f"Fetching {description} from Metabase question ID: {question_id}")
            
            response = self.session.post(query_url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Handle direct list format (new format)
                if isinstance(data, list):
                    if not data:
                        logger.warning(f"⚠️ No data returned for question {question_id}")
                        return pd.DataFrame()
                    
                    df = pd.DataFrame(data)
                    logger.info(f"✅ Successfully fetched {len(df)} rows from question {question_id}")
                    return df
                
                # Handle legacy format (data/rows/cols structure)
                elif isinstance(data, dict):
                    if 'data' in data:
                        data_obj = data['data']
                        if isinstance(data_obj, dict) and 'rows' in data_obj and 'cols' in data_obj:
                            rows = data_obj['rows']
                            columns = [col['display_name'] for col in data_obj['cols']]
                            
                            if not rows:
                                logger.warning(f"⚠️ No data returned for question {question_id}")
                                return pd.DataFrame(columns=columns)
                            
                            logger.info(f"✅ Successfully fetched {len(rows)} rows from question {question_id}")
                            return pd.DataFrame(rows, columns=columns)
                
                logger.error(f"❌ Unexpected response format for question {question_id}")
                return None
                
            else:
                logger.error(f"❌ Failed to fetch question {question_id}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching question {question_id}: {e}")
            return None
    
    def load_static_file(self, filename: str, separator: str = ',') -> pd.DataFrame:
        """Load static files from local data directory"""
        try:
            data_dir = 'data'
            filepath = os.path.join(data_dir, filename)
            
            if not os.path.exists(filepath):
                logger.warning(f"⚠️ Static file not found: {filepath}")
                return None
            
            df = pd.read_csv(filepath, sep=separator, encoding='utf-8')
            logger.info(f"✅ Loaded static file: {filename} ({len(df)} rows)")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading static file {filename}: {e}")
            return None
    
    def save_data_to_csv(self, dataframe: pd.DataFrame, filename: str, data_dir: str = 'data') -> bool:
        """Save DataFrame to CSV file in data directory"""
        try:
            # Create data directory if it doesn't exist
            os.makedirs(data_dir, exist_ok=True)
            
            filepath = os.path.join(data_dir, filename)
            dataframe.to_csv(filepath, index=False, encoding='utf-8')
            logger.info(f"✅ Data saved to: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving data to {filename}: {e}")
            return False
    
    def logout(self):
        """Logout from Metabase (only applicable for username/password authentication)"""
        if self.auth_method == 'username_password' and self.session_token:
            try:
                logout_url = f"{self.metabase_url}/api/session"
                self.session.delete(logout_url)
                logger.info("✅ Logged out from Metabase")
            except Exception as e:
                logger.warning(f"⚠️ Error during logout: {e}")
            finally:
                self.session_token = None
        elif self.auth_method == 'api_key':
            logger.info("ℹ️ API Key authentication - no logout required")

def main():
    """Main function to fetch data from Metabase and save to files"""
    try:
        # Create data directory if it doesn't exist
        data_dir = 'data'
        os.makedirs(data_dir, exist_ok=True)
        logger.info(f"✅ Data directory ready: {data_dir}")
        
        # Initialize data fetcher and connect to Metabase
        fetcher = MetabaseDataFetcher()
        
        # Configuration - Change these question IDs according to your Metabase setup
        FACILITY_QUESTION_ID = 4846  # Change this to your facility question ID
        SHIFTS_QUESTION_ID = 4659    # Optional: for shifts data (if needed)
        
        # Fetch facility data from Metabase
        logger.info("🔄 Fetching facility data from Metabase...")
        facility_data = fetcher.fetch_question_data(FACILITY_QUESTION_ID, "Facility Data")
        
        if facility_data is None or facility_data.empty:
            logger.error("❌ No facility data fetched")
            return False
        
        # Save raw facility data
        if not fetcher.save_data_to_csv(facility_data, 'raw_facilities.csv', data_dir):
            logger.error("❌ Failed to save facility data")
            return False
        
        # Optionally fetch shifts data
        logger.info("🔄 Fetching shifts data from Metabase...")
        shifts_data = fetcher.fetch_question_data(SHIFTS_QUESTION_ID, "Shifts Data")
        
        if shifts_data is not None and not shifts_data.empty:
            if not fetcher.save_data_to_csv(shifts_data, 'raw_shifts.csv', data_dir):
                logger.warning("⚠️ Failed to save shifts data")
        else:
            logger.warning("⚠️ No shifts data fetched")
        
        logger.info(f"✅ Data fetching completed successfully!")
        logger.info(f"📊 Facility data: {len(facility_data)} rows")
        if shifts_data is not None:
            logger.info(f"📊 Shifts data: {len(shifts_data)} rows")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        return False
    finally:
        # Always logout
        if 'fetcher' in locals():
            fetcher.logout()

if __name__ == "__main__":
    logger.info("🚀 Starting Metabase Data Fetcher")
    logger.info("=" * 50)
    
    success = main()
    
    if success:
        logger.info("🎉 Data fetching completed successfully!")
        logger.info("📂 Generated files in data/ directory:")
        logger.info("   • raw_facilities.csv - Raw facility data from Metabase")
        logger.info("   • raw_shifts.csv - Raw shifts data from Metabase (if available)")
        logger.info("🔄 Next step: Run map.py to process data and generate the interactive map")
    else:
        logger.error("💥 Data fetching failed!")