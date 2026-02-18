"""
Test script for TARIC Quota Scraper - Testing all search combinations
Usage: python test_all_combinations.py
"""

from TaricQuotaScraper.scraper import scrape_taric_quota
import json

def test_all_combinations():
    """Test the TARIC quota scraper with different parameter combinations"""
    
    print("=" * 70)
    print("Testing TARIC Quota Scraper - All Search Combinations")
    print("=" * 70)
    
    # Test case 1: Search by origin only
    print("\n📍 Test 1: Search by ORIGIN only (MA - Morocco)")
    print("-" * 70)
    try:
        result = scrape_taric_quota(origin="MA", year=2026)
        print(f"✓ Success! Found {result['results_count']} results")
        if result['results_count'] > 0:
            print(f"  First result: Order {result['results'][0]['order_number']}")
            # Show only first 3 results
            for i, res in enumerate(result['results'][:3]):
                print(f"  {i+1}. Order: {res['order_number']}, Balance: {res['balance']['quantity']} {res['balance']['unit']}")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Test case 2: Search by order number only
    print("\n\n📋 Test 2: Search by ORDER NUMBER only (091100)")
    print("-" * 70)
    try:
        result = scrape_taric_quota(order_number="091100", year=2026)
        print(f"✓ Success! Found {result['results_count']} results")
        if result['results_count'] > 0:
            print(f"  First result: Origins - {result['results'][0]['origins']}")
            # Show only first 3 results
            for i, res in enumerate(result['results'][:3]):
                print(f"  {i+1}. Origins: {res['origins']}, Balance: {res['balance']['quantity']} {res['balance']['unit']}")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Test case 3: Search by both origin AND order number
    print("\n\n🔍 Test 3: Search by BOTH Origin + Order Number (MA + 091100)")
    print("-" * 70)
    try:
        result = scrape_taric_quota(origin="MA", order_number="091100", year=2026)
        print(f"✓ Success! Found {result['results_count']} results")
        if result['results_count'] > 0:
            for res in result['results']:
                print(f"  Order: {res['order_number']}")
                print(f"  Origins: {res['origins']}")
                print(f"  Period: {res['start_date']} to {res['end_date']}")
                print(f"  Balance: {res['balance']['quantity']} {res['balance']['unit']}")
        print("\n  Full JSON:")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Test case 4: Search with year defaulting to current year
    print("\n\n📅 Test 4: Search without specifying year (defaults to current year)")
    print("-" * 70)
    try:
        result = scrape_taric_quota(origin="MA", order_number="091100")
        print(f"✓ Success! Year defaulted to: {result['year']}")
        print(f"  Found {result['results_count']} results")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Test case 5: Search that returns no results
    print("\n\n❌ Test 5: Search with no results (invalid combination)")
    print("-" * 70)
    try:
        result = scrape_taric_quota(origin="XX", order_number="999999", year=2026)
        print(f"✓ Handled gracefully")
        print(f"  Message: {result.get('message', 'No message')}")
        print(f"  Results: {result['results_count']}")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    print("\n" + "=" * 70)
    print("✅ All tests complete!")
    print("=" * 70)

if __name__ == "__main__":
    test_all_combinations()
