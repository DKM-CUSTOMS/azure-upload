"""
Test script for TARIC Quota Scraper
Usage: python test_taric_scraper.py
"""

from TaricQuotaScraper.scraper import scrape_taric_quota
import json

def test_scraper():
    """Test the TARIC quota scraper with sample data"""
    
    print("=" * 60)
    print("Testing TARIC Quota Scraper")
    print("=" * 60)
    
    # Test case 1: China with order number 091100
    print("\nTest 1: Origin=CN (China), Order Number=091100")
    print("-" * 60)
    try:
        result = scrape_taric_quota(origin="CN", order_number="090521")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Test case 2: Multiple origins with a different order number
    print("\n\nTest 2: Origin=1011 (ERGA OMNES), Order Number=091100")
    print("-" * 60)
    try:
        result = scrape_taric_quota(origin="1011", order_number="090521")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Test case 3: United States
    print("\n\nTest 3: Origin=US (United States), Order Number=091100")
    print("-" * 60)
    try:
        result = scrape_taric_quota(origin="US", order_number="090521")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")
    
    print("\n" + "=" * 60)
    print("Testing complete")
    print("=" * 60)

if __name__ == "__main__":
    test_scraper()
