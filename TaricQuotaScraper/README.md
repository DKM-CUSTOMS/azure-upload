# TARIC Quota Scraper

Azure Function that scrapes tariff quota data from the European Commission's TARIC database.

## Description

This function retrieves quota information from the EU's TARIC (Integrated Tariff of the European Communities) database by origin country and order number. The data includes:
- Order number
- Origin countries  
- Start and end dates
- Balance (quantity and unit)
- Link to more detailed information

## Features

- ✅ Simple HTTP POST API
- ✅ No browser automation needed (uses direct AJAX endpoint)
- ✅ Clean JSON response format
- ✅ Handles multiple result rows
- ✅ Automatic year defaulting to current year
- ✅ Proper error handling

## API Endpoint

**Method:** POST  
**URL:** `/api/TaricQuotaScraper`

## Request Body

You can search by:
- **Origin only** - Get all quotas for a specific country
- **Order number only** - Get quota details for a specific order
- **Both** - Get specific quota for a country

### Search by Origin Only

```json
{
  "origin": "MA",
  "year": 2026
}
```

### Search by Order Number Only

```json
{
  "order_number": "091100",
  "year": 2026
}
```

### Search by Both Origin and Order Number

```json
{
  "origin": "MA",
  "order_number": "091100",
  "year": 2026
}
```

### Parameters

- `origin` (optional*): Origin country code
  - Examples: `"MA"` (Morocco), `"CN"` (China), `"US"` (United States), `"1011"` (ERGA OMNES)
  - See [full list of origin codes](https://ec.europa.eu/taxation_customs/dds2/taric/quota_consultation.jsp?Lang=en)

- `order_number` (optional*): Quota order number
  - Format: 6-digit string
  - Example: `"091100"`

- `year` (optional): Year to search
  - Format: Integer (e.g. `2026`)
  - Defaults to current year if not provided

**Note:** *At least one of `origin` or `order_number` must be provided.

## Response Format

### Successful Response

```json
{
  "success": true,
  "origin": "MA",
  "order_number": "091100",
  "year": 2026,
  "results_count": 1,
  "results": [
    {
      "order_number": "091100",
      "origins": "Multiple origins",
      "start_date": "01-01-2026",
      "end_date": "31-12-2026",
      "balance": {
        "quantity": "1500000",
        "unit": "Kilogram",
        "raw": "1500000 Kilogram"
      },
      "more_info_url": "https://ec.europa.eu/taxation_customs/dds2/taric/quota_tariff_details.jsp?Lang=en&StartDate=2026-01-01&Code=091100"
    }
  ],
  "request_url": "https://ec.europa.eu/taxation_customs/dds2/taric/quota_list.jsp?Lang=en&Origin=MA&Code=091100&Year=2026&Status=&Critical=&Expand=false&Offset=0"
}
```

### No Results Response

```json
{
  "success": true,
  "origin": "XX",
  "order_number": "999999",
  "year": 2026,
  "results": [],
  "message": "No results found for the given criteria"
}
```

### Error Response

```json
{
  "error": "At least one of 'origin' or 'order_number' must be provided",
  "examples": {
    "search_by_origin": {"origin": "MA"},
    "search_by_order": {"order_number": "091100"},
    "search_by_both": {"origin": "MA", "order_number": "091100"}
  }
}
```

## Testing Locally

Run the test script:

```bash
python test_taric_direct.py
```

Or test with curl:

```bash
curl -X POST http://localhost:7071/api/TaricQuotaScraper \
  -H "Content-Type: application/json" \
  -d '{"origin": "MA", "order_number": "091100", "year": 2026}'
```

## How It Works

1. The function receives a POST request with origin and order number
2. It makes a GET request to the TARIC AJAX endpoint: `quota_list.jsp`
3. Parses the HTML table response using BeautifulSoup
4. Extracts data from the `quotaTable` element
5. Returns structured JSON data

### Technical Details

- **Discovery**: The main page loads data dynamically via JavaScript. The actual data comes from an AJAX call to `quota_list.jsp`.
- **No Selenium needed**: Since we found the direct AJAX endpoint, we can use simple HTTP requests instead of browser automation.
- **Parsing**: Uses BeautifulSoup to extract data from the HTML table structure.

## Example Usage

### Python

```python
import requests

url = "https://your-function-app.azurewebsites.net/api/TaricQuotaScraper"

# Example 1: Search by origin only (all quotas for Morocco)
payload = {"origin": "MA", "year": 2026}
response = requests.post(url, json=payload)
data = response.json()

print(f"Found {data['results_count']} quotas for Morocco")
for result in data['results']:
    print(f"  Order: {result['order_number']}, Balance: {result['balance']['quantity']} {result['balance']['unit']}")

# Example 2: Search by order number only (specific quota across all origins)
payload = {"order_number": "091100", "year": 2026}
response = requests.post(url, json=payload)
data = response.json()

print(f"\nQuota 091100 details:")
for result in data['results']:
    print(f"  Origins: {result['origins']}")
    print(f"  Period: {result['start_date']} to {result['end_date']}")
    print(f"  Balance: {result['balance']['quantity']} {result['balance']['unit']}")

# Example 3: Search by both (specific quota for a specific country)
payload = {"origin": "MA", "order_number": "091100", "year": 2026}
response = requests.post(url, json=payload)
data = response.json()

if data['results_count'] > 0:
    result = data['results'][0]
    print(f"\nMorocco quota 091100:")
    print(f"  Balance: {result['balance']['quantity']} {result['balance']['unit']}")
```

### JavaScript

```javascript
// Example 1: Search by origin only
const response1 = await fetch('https://your-function-app.azurewebsites.net/api/TaricQuotaScraper', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({origin: 'MA', year: 2026})
});
const data1 = await response1.json();
console.log(`Found ${data1.results_count} quotas for Morocco`);

// Example 2: Search by order number only
const response2 = await fetch('https://your-function-app.azurewebsites.net/api/TaricQuotaScraper', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({order_number: '091100', year: 2026})
});
const data2 = await response2.json();
console.log('Quota 091100:', data2.results);

// Example 3: Search by both
const response3 = await fetch('https://your-function-app.azurewebsites.net/api/TaricQuotaScraper', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({origin: 'MA', order_number: '091100', year: 2026})
});
const data3 = await response3.json();
console.log('Specific quota:', data3.results[0]);
```

## Common Origin Codes

| Code | Description |
|------|-------------|
| 1011 | ERGA OMNES |
| CN | China |
| US | United States |
| MA | Morocco |
| TR | Türkiye |
| IN | India |
| JP | Japan |
| BR | Brazil |

## Dependencies

- `requests` - HTTP library
- `beautifulsoup4` - HTML parsing
- `lxml` - XML/HTML parser for BeautifulSoup

## Notes

- The TARIC database is updated regularly by the European Commission
- Some quota numbers may not have results for all origins
- The balance quantity shows the remaining quota available
- The "more info" URL provides detailed tariff information

## Error Handling

The function handles the following error cases:
- Missing required parameters (origin or order_number)
- Invalid JSON in request body
- Network errors when connecting to TARIC
- Parsing errors when processing the response
- No results found for given criteria

## Source

Data is scraped from: https://ec.europa.eu/taxation_customs/dds2/taric/quota_consultation.jsp
