"""
Test script for TARIC Quota Scraper - Using discovered URL pattern
Usage: python test_taric_direct.py
"""

from TaricQuotaScraper.scraper import scrape_taric_quota
import json

def test_scraper():
    """Test the TARIC quota scraper with the URL pattern discovered"""
    
    print("=" * 60)
    print("Testing TARIC Quota Scraper - Direct URL Method")
    print("=" * 60)
    
    # Test case from the user's discovered URL
    # https://ec.europa.eu/taxation_customs/dds2/taric/quota_consultation.jsp?Lang=en&Origin=MA&Code=091100&Year=2026&Expand=false
    print("\nTest 1: Origin=MA (Morocco), Order Number=091100, Year=2026")
    print("-" * 60)
    try:
        result = scrape_taric_quota(origin="MA", order_number="091100", year=2026)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Test case 2: Without specifying year (should use current year)
    print("\n\nTest 2: Origin=MA, Order Number=091100 (no year specified)")
    print("-" * 60)
    try:
        result = scrape_taric_quota(origin="MA", order_number="091100")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Test case 3: ERGA OMNES
    print("\n\nTest 3: Origin=1011 (ERGA OMNES), Order Number=091100, Year=2026")
    print("-" * 60)
    try:
        result = scrape_taric_quota(origin="1011", order_number="091100", year=2026)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("Testing complete")
    print("=" * 60)

if __name__ == "__main__":
    test_scraper()
