import re
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime


def _clean_text(text: str) -> str:
    """Clean whitespace, non-breaking spaces, and normalize text."""
    if not text:
        return text
    text = text.replace('\xa0', ' ').replace('\r', ' ').replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text if text else None


def scrape_quota_detail(order_number: str, start_date: str) -> dict:
    """
    Scrapes detailed quota information from the TARIC details page.
    
    Args:
        order_number: The quota order number (e.g., '091100')
        start_date: Start date in format YYYY-MM-DD (e.g., '2026-01-01')
    
    Returns:
        Dictionary containing detailed quota information
    """
    base_url = "https://ec.europa.eu/taxation_customs/dds2/taric/quota_tariff_details.jsp"
    params = {
        'Lang': 'en',
        'StartDate': start_date,
        'Code': order_number
    }
    
    try:
        logging.info(f"Fetching quota details for Code={order_number}, StartDate={start_date}")
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        response = session.get(base_url, params=params)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the details container specifically
        container = soup.find('div', {'id': 'quotaDetailsMarkedUpContainer'})
        if not container:
            logging.warning(f"No details container found for order {order_number}")
            return None

        details_table = container.find('table', {'class': 'ecl-table'})
        if not details_table:
            logging.warning(f"No details table found for order {order_number}")
            return None

        # Map of label text -> clean key name
        label_map = {
            "Order number": "order_number",
            "Validity period": "validity_period",
            "Origin": "origin",
            "Initial amount": "initial_amount",
            "Amount": "amount",
            "Balance": "balance",
            "Transferred Amount": "transferred_amount",
            "Exhaustion date": "exhaustion_date",
            "Critical": "critical",
            "Last import date": "last_import_date",
            "Last allocation date": "last_allocation_date",
            "Blocking period": "blocking_period",
            "Suspension period": "suspension_period",
            "Allocated percentage at the last allocation": "allocated_percentage",
            "Associated TARIC code": "associated_taric_code",
        }

        # Extract data from the table
        details = {}
        rows = details_table.find_all('tr', {'class': 'ecl-table__row'})

        for row in rows:
            cells = row.find_all('td', recursive=False)
            if len(cells) < 2:
                continue

            label_cell = cells[0]
            value_cell = cells[1]

            # Get the label from the label cell only (ignore nested link text)
            label_parts = []
            for child in label_cell.children:
                if isinstance(child, str):
                    label_parts.append(child.strip())
            label = ' '.join(label_parts).strip()
            if not label:
                label = label_cell.get_text(strip=True)
            # Clean up label for matching
            label_clean = label.replace(':', '').strip()

            # Find the matching key, or fall back to "total_awaiting_allocation" for that special field
            if "Total awaiting allocation" in label_cell.get_text():
                key = "total_awaiting_allocation"
            else:
                key = label_map.get(label_clean)
                if not key:
                    # Fallback: generate key from label
                    key = label_clean.lower().replace(' ', '_')

            # Fields that contain quantity + unit (e.g. "1500000 Kilogram")
            amount_fields = {"initial_amount", "amount", "balance"}

            # Get the value, handling special cases
            if key == "origin":
                origin_divs = value_cell.find_all('div')
                if origin_divs:
                    value = [div.get_text(strip=True) for div in origin_divs]
                else:
                    value = _clean_text(value_cell.get_text())
            elif key == "associated_taric_code":
                links = value_cell.find_all('a')
                if links:
                    value = []
                    for link in links:
                        code_text = link.get_text(strip=True)
                        if code_text:
                            value.append({
                                'code': code_text,
                                'url': link.get('href')
                            })
                    if len(value) == 1:
                        value = value[0]
                else:
                    value = _clean_text(value_cell.get_text())
            elif key == "transferred_amount":
                nested_table = value_cell.find('table')
                if nested_table:
                    nested_rows = nested_table.find_all('tr')
                    if nested_rows:
                        transfers = [_clean_text(nr.get_text()) for nr in nested_rows]
                        transfers = [t for t in transfers if t]
                        value = transfers if transfers else None
                    else:
                        value = None
                else:
                    value = _clean_text(value_cell.get_text())
            elif key == "validity_period":
                raw = _clean_text(value_cell.get_text())
                if raw and ' - ' in raw:
                    period_parts = raw.split(' - ', 1)
                    value = {"start": period_parts[0].strip(), "end": period_parts[1].strip()}
                else:
                    value = raw
            elif key in amount_fields:
                raw = _clean_text(value_cell.get_text())
                if raw:
                    parts = raw.split()
                    quantity = parts[0].replace(',', '') if parts else raw
                    unit = ' '.join(parts[1:]) if len(parts) > 1 else None
                    value = {"quantity": quantity, "unit": unit}
                else:
                    value = None
            else:
                value = _clean_text(value_cell.get_text())

            details[key] = value
        
        logging.info(f"Successfully scraped details for order {order_number}")
        return details
        
    except requests.RequestException as e:
        logging.error(f"Request error fetching details: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Error scraping quota details: {str(e)}")
        return None


def scrape_taric_quota(origin: str = None, order_number: str = None, year: int = None, include_details: bool = False) -> dict:
    """
    Scrapes TARIC quota data from the European Commission website.
    
    Args:
        origin: Optional origin country code (e.g., 'CN', 'MA', '1011' for ERGA OMNES)
        order_number: Optional quota order number (e.g., '091100')
        year: Optional year to search (defaults to current year)
        include_details: If True, fetches detailed information for each quota found
    
    Note:
        At least one of origin or order_number must be provided.
    
    Returns:
        Dictionary containing the quota data
    """
    # Use current year if not specified
    if year is None:
        year = datetime.now().year
    
    # The actual data is loaded via AJAX from quota_list.jsp
    base_url = "https://ec.europa.eu/taxation_customs/dds2/taric/quota_list.jsp"
    params = {
        'Lang': 'en',
        'Origin': origin if origin else '',
        'Code': order_number if order_number else '',
        'Year': str(year),
        'Status': '',
        'Critical': '',
        'Expand': 'false',
        'Offset': '0'
    }
    
    try:
        # Make GET request to AJAX endpoint
        logging.info(f"Fetching TARIC data for Origin={origin}, Code={order_number}, Year={year}")
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'X-Requested-With': 'XMLHttpRequest',
        })
        
        response = session.get(base_url, params=params)
        response.raise_for_status()
        
        logging.info(f"Request URL: {response.url}")
        
        # Parse the HTML response
        soup = BeautifulSoup(response.content, 'html.parser')
        quota_table = soup.find('table', {'id': 'quotaTable'})
        
        if not quota_table:
            logging.warning("No quota table found in response")
            return {
                "success": True,
                "origin": origin,
                "order_number": order_number,
                "year": year,
                "results_count": 0,
                "results": [],
                "message": "No results found for the given criteria"
            }
        
        # Extract table data
        results = []
        tbody = quota_table.find('tbody', {'class': 'ecl-table__body'})
        
        if tbody:
            rows = tbody.find_all('tr', {'class': 'ecl-table__row'})
            logging.info(f"Found {len(rows)} result rows")
            
            for row in rows:
                cells = row.find_all('td', {'class': 'ecl-table__cell'})
                
                if len(cells) >= 6:
                    # Extract order number
                    order_num = cells[0].get_text(strip=True)
                    
                    # Extract origins
                    origins = cells[1].get_text(strip=True)
                    
                    # Extract dates
                    start_date = cells[2].get_text(strip=True)
                    end_date = cells[3].get_text(strip=True)
                    
                    # Extract balance (quantity and unit)
                    balance_text = _clean_text(cells[4].get_text())
                    # Parse balance to separate quantity and unit
                    balance_parts = balance_text.split() if balance_text else []
                    balance_quantity = balance_parts[0].replace(',', '') if balance_parts else ""
                    balance_unit = " ".join(balance_parts[1:]) if len(balance_parts) > 1 else ""
                    
                    # Extract more info link
                    more_info_link = ""
                    link_tag = cells[5].find('a', {'id': 'quotaLink'})
                    if link_tag and link_tag.get('href'):
                        more_info_link = link_tag['href']
                        # Make it absolute URL if it's relative
                        if more_info_link.startswith('/'):
                            more_info_link = f"https://ec.europa.eu{more_info_link}"
                    
                    result_item = {
                        "order_number": order_num,
                        "origins": origins,
                        "start_date": start_date,
                        "end_date": end_date,
                        "balance": {
                            "quantity": balance_quantity,
                            "unit": balance_unit
                        },
                        "more_info_url": more_info_link
                    }
                    
                    # Fetch detailed information if requested
                    if include_details and start_date:
                        # Convert date from DD-MM-YYYY to YYYY-MM-DD
                        try:
                            date_parts = start_date.split('-')
                            if len(date_parts) == 3:
                                formatted_date = f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"
                                details = scrape_quota_detail(order_num, formatted_date)
                                if details:
                                    result_item["details"] = details
                        except Exception as e:
                            logging.warning(f"Could not fetch details for {order_num}: {str(e)}")
                    
                    results.append(result_item)
        
        logging.info(f"Successfully scraped {len(results)} quota records")
        
        return {
            "success": True,
            "origin": origin,
            "order_number": order_number,
            "year": year,
            "results_count": len(results),
            "results": results,
            "request_url": response.url
        }
        
    except requests.RequestException as e:
        logging.error(f"Request error: {str(e)}")
        raise Exception(f"Failed to fetch data from TARIC website: {str(e)}")
    except Exception as e:
        logging.error(f"Scraping error: {str(e)}")
        raise Exception(f"Error scraping TARIC data: {str(e)}")
