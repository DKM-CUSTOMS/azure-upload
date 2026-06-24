import json
import logging

import azure.functions as func
from azure.core.exceptions import AzureError

from AI_agents.Azure.content_understanding_item_table_extractor import (
    ContentUnderstandingItemTableExtractor,
)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Content Understanding item table extraction request.")

    try:
        req_body = req.get_json()
    except ValueError:
        return _json_response({"error": "Invalid JSON"}, status_code=400)

    pdf_base64 = req_body.get("file")
    filename = req_body.get("filename", "document.pdf")
    analyzer_id = req_body.get("analyzer_id") or req_body.get("analyzerId")
    content_range = req_body.get("content_range") or req_body.get("contentRange")
    include_raw = bool(req_body.get("include_raw") or req_body.get("includeRaw"))

    if not pdf_base64:
        return _json_response(
            {"error": "Missing required 'file' base64 PDF payload"},
            status_code=400,
        )

    if req_body.get("files"):
        return _json_response(
            {
                "error": (
                    "Send exactly one PDF using {'file': '', 'filename': ''}. "
                    "Multiple files are rejected to control cost."
                )
            },
            status_code=400,
        )

    try:
        extractor = ContentUnderstandingItemTableExtractor()
        result = extractor.extract_from_base64(
            pdf_base64=pdf_base64,
            filename=filename,
            analyzer_id=analyzer_id,
            content_range=content_range,
            include_raw=include_raw,
        )
        return _json_response(result)
    except AzureError as error:
        logging.exception("Azure Content Understanding request failed")
        return _json_response(
            {"error": "Azure Content Understanding request failed", "details": str(error)},
            status_code=502,
        )
    except ValueError as error:
        return _json_response({"error": str(error)}, status_code=400)
    except Exception as error:
        logging.exception("Unexpected Content Understanding item extraction error")
        return _json_response(
            {"error": "Unexpected processing error", "details": str(error)},
            status_code=500,
        )


def _json_response(payload, status_code=200):
    return func.HttpResponse(
        body=json.dumps(payload, ensure_ascii=False),
        status_code=status_code,
        mimetype="application/json",
    )
