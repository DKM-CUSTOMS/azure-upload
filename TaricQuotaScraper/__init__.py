import azure.functions as func
import logging
import json
from .scraper import scrape_taric_quota

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('TaricQuotaScraper function processed a request.')

    try:
        # Get parameters from request
        req_body = req.get_json()
        origin = req_body.get('origin')
        order_number = req_body.get('order_number')
        year = req_body.get('year')  # Optional
        include_details = req_body.get('include_details', False)  # Optional

        # At least one search parameter must be provided
        if not origin and not order_number:
            return func.HttpResponse(
                json.dumps({
                    "error": "At least one of 'origin' or 'order_number' must be provided",
                    "examples": {
                        "search_by_origin": {"origin": "MA"},
                        "search_by_order": {"order_number": "091100"},
                        "search_by_both": {"origin": "MA", "order_number": "091100"}
                    }
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Scrape the data
        result = scrape_taric_quota(origin, order_number, year, include_details)
        
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error in TaricQuotaScraper: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
