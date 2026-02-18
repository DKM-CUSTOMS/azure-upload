import requests
from bs4 import BeautifulSoup

# Test the exact URL from the user
url = "https://ec.europa.eu/taxation_customs/dds2/taric/quota_consultation.jsp?Lang=en&Origin=MA&Code=091100&Year=2026&Expand=false"

print(f"Testing URL: {url}")

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
})

response = session.get(url)
print(f"Status Code: {response.status_code}")
print(f"Content Length: {len(response.content)}")

# Save to file
with open('taric_ma_response.html', 'w', encoding='utf-8') as f:
    f.write(response.text)

print("Saved to taric_ma_response.html")

# Parse and look for tables
soup = BeautifulSoup(response.content, 'html.parser')

# Find all tables
all_tables = soup.find_all('table')
print(f"\nFound {len(all_tables)} tables total")

# Look specifically for quotaTable
quota_table = soup.find('table', {'id': 'quotaTable'})
if quota_table:
    print("✓ quotaTable found!")
    rows = quota_table.find_all('tr')
    print(f"  Rows: {len(rows)}")
else:
    print("✗ quotaTable NOT found")
    
# Check for any divs with class ecl-table
ecl_tables = soup.find_all(class_='ecl-table')
print(f"\nFound {len(ecl_tables)} elements with class 'ecl-table'")

# Check body length
print(f"\nBody content length: {len(soup.find('body').get_text() if soup.find('body') else 'No body')}")

# Check for script tags that might load data dynamically
scripts = soup.find_all('script')
print(f"Found {len(scripts)} script tags")
