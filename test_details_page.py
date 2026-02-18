import requests
from bs4 import BeautifulSoup

# Test the details page
url = "https://ec.europa.eu/taxation_customs/dds2/taric/quota_tariff_details.jsp?Lang=en&StartDate=2026-01-01&Code=091100"

print(f"Testing details page: {url}\n")

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
})

response = session.get(url)
print(f"Status Code: {response.status_code}")
print(f"Content Length: {len(response.content)}\n")

# Save to file
with open('taric_details_response.html', 'w', encoding='utf-8') as f:
    f.write(response.text)
print("Saved to taric_details_response.html\n")

# Parse and find interesting data
soup = BeautifulSoup(response.content, 'html.parser')

# Look for tables
tables = soup.find_all('table')
print(f"Found {len(tables)} tables\n")

for i, table in enumerate(tables):
    table_id = table.get('id', 'no-id')
    table_class = table.get('class', [])
    print(f"Table {i+1}: id='{table_id}', class={table_class}")
    
    # Get first few rows
    rows = table.find_all('tr')[:5]
    if rows:
        print(f"  First few rows:")
        for row in rows[:3]:
            cells = row.find_all(['td', 'th'])
            if cells:
                text = ' | '.join([cell.get_text(strip=True)[:50] for cell in cells])
                print(f"    {text}")
    print()

# Look for specific data sections
print("\n" + "="*70)
print("Looking for specific data...")
print("="*70)

# Check for any divs with specific classes
data_sections = soup.find_all('div', class_=['ecl-table', 'data-container', 'quota-details'])
print(f"Found {len(data_sections)} data sections")
