"""
Debug script to analyze the TARIC website response
"""

import requests
from bs4 import BeautifulSoup

def debug_taric():
    base_url = "https://ec.europa.eu/taxation_customs/dds2/taric/quota_consultation.jsp"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })
    
    print("Step 1: Getting initial page...")
    response = session.get(f"{base_url}?Lang=en")
    print(f"Status: {response.status_code}")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the form
    form = soup.find('form')
    if form:
        print(f"\nForm found: action='{form.get('action')}', method='{form.get('method')}'")
        
        # Find all form inputs
        inputs = form.find_all(['input', 'select', 'button'])
        print("\nForm fields:")
        for inp in inputs:
            name = inp.get('name', '')
            input_type = inp.get('type', inp.name)
            value = inp.get('value', '')
            print(f"  - {name}: type={input_type}, value={value}")
    
    # Try different form submission
    print("\n\nStep 2: Submitting form...")
    
    # Check if there's a searchBtn onClick behavior
    search_btn = soup.find('button', {'id': 'searchBtN'})
    if search_btn:
        onclick = search_btn.get('onclick', '')
        print(f"Search button onclick: {onclick}")
    
    # Try to extract form data
    form_data = {
        'Lang': 'en',
        'Origin': '1011',  # ERGA OMNES
        'Code': '091100',
        'Simulation_Date': '',
    }
    
    print(f"Submitting with data: {form_data}")
    response = session.post(base_url, data=form_data)
    print(f"Response status: {response.status_code}")
    
    # Save response to file for inspection
    with open('taric_response.html', 'w', encoding='utf-8') as f:
        f.write(response.text)
    print("\nResponse saved to taric_response.html")
    
    # Check for table
    soup = BeautifulSoup(response.content, 'html.parser')
    quota_table = soup.find('table', {'id': 'quotaTable'})
    
    if quota_table:
        print("\n✓ Quota table found!")
        rows = quota_table.find_all('tr')
        print(f"Number of rows: {len(rows)}")
    else:
        print("\n✗ No quota table found")
        
        # Check what tables exist
        all_tables = soup.find_all('table')
        print(f"Found {len(all_tables)} total tables")
        for i, table in enumerate(all_tables):
            table_id = table.get('id', 'no-id')
            table_class = table.get('class', 'no-class')
            print(f"  Table {i+1}: id={table_id}, class={table_class}")
    
    # Check for any error messages
    error_div = soup.find('div', class_='ecl-message')
    if error_div:
        print(f"\nError message found: {error_div.get_text(strip=True)}")

if __name__ == "__main__":
    debug_taric()
