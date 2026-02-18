import requests
from bs4 import BeautifulSoup

# Try the quota_list.jsp endpoint that's called via AJAX
base_url = "https://ec.europa.eu/taxation_customs/dds2/taric/quota_list.jsp"

params = {
    'Lang': 'en',
    'Origin': 'MA',
    'Code': '091100',
    'Year': '2026',
    'Status': '',
    'Critical': '',
    'Expand': 'false',
    'Offset': '0'
}

print(f"Testing AJAX endpoint...")
print(f"URL: {base_url}")
print(f"Params: {params}")

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': '*/*',
    'X-Requested-With': 'XMLHttpRequest',
})

response = session.get(base_url, params=params)
print(f"\nStatus Code: {response.status_code}")
print(f"Content Length: {len(response.content)}")

# Save to file
with open('taric_ajax_response.html', 'w', encoding='utf-8') as f:
    f.write(response.text)

print("Saved to taric_ajax_response.html")

# Parse and look for the table
soup =BeautifulSoup(response.content, 'html.parser')

# Look for quotaTable
quota_table = soup.find('table', {'id': 'quotaTable'})
if quota_table:
    print("\n✓ quotaTable found in AJAX response!")
    tbody = quota_table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
        print(f"  Rows found: {len(rows)}")
        if rows:
            print("\nFirst row data:")
            cells = rows[0].find_all('td')
            for i, cell in enumerate(cells):
                print(f"  Cell {i}: {cell.get_text(strip=True)}")
else:
    print("\n✗ quotaTable NOT found in AJAX response")
