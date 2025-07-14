#!/usr/bin/env python3
"""
List Available Metabase Questions
Helper script to find question IDs you have access to.
"""

import pandas as pd
import logging
import json
from data import MetabaseDataFetcher

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_basic_access(fetcher):
    """Test basic API access with different endpoints"""
    endpoints_to_test = [
        ("/api/user/current", "Current User Info"),
        ("/api/database", "Available Databases"),
        ("/api/collection", "Available Collections"),
        ("/api/card", "Available Cards/Questions")
    ]
    
    logger.info("🔍 Testing API access...")
    
    for endpoint, description in endpoints_to_test:
        try:
            url = f"{fetcher.metabase_url}{endpoint}"
            response = fetcher.session.get(url, timeout=10)
            
            logger.info(f"📡 {description} ({endpoint}): {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list):
                        logger.info(f"   ✅ Success - Found {len(data)} items")
                        if len(data) > 0:
                            logger.info(f"   📝 First item structure: {list(data[0].keys()) if isinstance(data[0], dict) else 'Not a dict'}")
                    elif isinstance(data, dict):
                        logger.info(f"   ✅ Success - Dict with keys: {list(data.keys())}")
                    else:
                        logger.info(f"   ✅ Success - Data type: {type(data)}")
                except json.JSONDecodeError:
                    logger.info(f"   ✅ Success - Non-JSON response")
            else:
                logger.error(f"   ❌ Error: {response.text[:200]}")
                
        except Exception as e:
            logger.error(f"   💥 Exception: {str(e)}")
    
    return True

def list_questions_improved(fetcher):
    """Improved question listing with better error handling"""
    try:
        cards_url = f"{fetcher.metabase_url}/api/card"
        logger.info(f"🔍 Fetching questions from: {cards_url}")
        
        response = fetcher.session.get(cards_url, timeout=30)
        logger.info(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                cards_data = response.json()
                logger.info(f"📊 Response type: {type(cards_data)}")
                
                if isinstance(cards_data, list):
                    logger.info(f"📋 Found {len(cards_data)} cards")
                    
                    if len(cards_data) > 0:
                        # Debug first item
                        first_card = cards_data[0]
                        logger.info(f"🔍 First card structure: {list(first_card.keys()) if isinstance(first_card, dict) else 'Not a dict'}")
                        
                        # Process cards
                        cards_info = []
                        for i, card in enumerate(cards_data[:20]):  # Limit to first 20
                            if isinstance(card, dict):
                                cards_info.append({
                                    'id': card.get('id', f'unknown_{i}'),
                                    'name': card.get('name', 'Unknown'),
                                    'description': str(card.get('description', 'N/A'))[:100],
                                    'collection_name': str(card.get('collection', {}).get('name', 'N/A')) if isinstance(card.get('collection'), dict) else 'N/A',
                                    'database_id': card.get('database_id', 'N/A'),
                                    'table_id': card.get('table_id', 'N/A')
                                })
                            else:
                                logger.warning(f"⚠️ Card {i} is not a dict: {type(card)}")
                        
                        if cards_info:
                            df = pd.DataFrame(cards_info)
                            return df
                        else:
                            logger.error("❌ No valid cards found")
                            return pd.DataFrame()
                    else:
                        logger.warning("⚠️ Empty cards list")
                        return pd.DataFrame()
                        
                elif isinstance(cards_data, dict):
                    logger.info(f"📊 Dict response with keys: {list(cards_data.keys())}")
                    # Maybe it's paginated or wrapped
                    if 'data' in cards_data:
                        return list_questions_improved_dict(cards_data['data'])
                    else:
                        logger.error(f"❌ Unexpected dict structure: {cards_data}")
                        return pd.DataFrame()
                else:
                    logger.error(f"❌ Unexpected response type: {type(cards_data)}")
                    return pd.DataFrame()
                    
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")
                logger.error(f"❌ Raw response: {response.text[:500]}")
                return pd.DataFrame()
        else:
            logger.error(f"❌ HTTP Error {response.status_code}: {response.text}")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"❌ Exception in list_questions_improved: {e}")
        return pd.DataFrame()

def main():
    """List all available questions from Metabase"""
    try:
        logger.info("🚀 Starting Metabase Questions Explorer")
        logger.info("=" * 50)
        
        # Initialize data fetcher
        fetcher = MetabaseDataFetcher()
        
        # Test basic access first
        test_basic_access(fetcher)
        
        logger.info("\n" + "=" * 50)
        
        # Get available questions with improved method
        questions_df = list_questions_improved(fetcher)
        
        if questions_df.empty:
            logger.error("❌ No questions found or access denied")
            logger.info("💡 Possible solutions:")
            logger.info("   1. Check if your API key has the correct permissions")
            logger.info("   2. Verify the question ID exists and you have access to it")
            logger.info("   3. Try using a different question ID that you know exists")
            return False
        
        # Display results
        logger.info("📋 Available Questions:")
        logger.info("=" * 80)
        
        # Print in a nice format
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        print(questions_df.to_string(index=False))
        
        logger.info("=" * 80)
        logger.info(f"📊 Found {len(questions_df)} questions total")
        
        # Look for facility-related questions
        facility_keywords = ['facility', 'hospital', 'clinic', 'centro', 'instalacion', 'healthcare', 'health']
        facility_questions = questions_df[
            questions_df['name'].str.lower().str.contains('|'.join(facility_keywords), na=False)
        ]
        
        if not facility_questions.empty:
            logger.info("🏥 Facility-related questions found:")
            print("\n" + facility_questions[['id', 'name', 'description']].to_string(index=False))
            logger.info("💡 Try using one of these IDs in your data.py configuration!")
        else:
            logger.info("ℹ️ No obvious facility-related questions found by name")
            logger.info("💡 Look through the list above for questions containing your facility data")
        
        # Save to file
        questions_df.to_csv('data/available_questions.csv', index=False)
        logger.info("✅ Questions list saved to: data/available_questions.csv")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False
    finally:
        if 'fetcher' in locals():
            fetcher.logout()

if __name__ == "__main__":
    success = main()
    
    if success:
        print("\n🎯 Next Steps:")
        print("1. Look at the questions list above")
        print("2. Find a question that contains your facility data")
        print("3. Update FACILITY_QUESTION_ID in data.py with the correct ID")
        print("4. Run 'python data.py' again")
    else:
        print("\n💥 Failed to list questions. Check your API key permissions.")
        print("\n🔧 Alternative solutions:")
        print("1. Ask your Metabase admin which question IDs you have access to")
        print("2. Try using question ID 4846 (since you have question_4846.csv in your data)")
        print("3. Check the Metabase web interface for question IDs in the URL") 