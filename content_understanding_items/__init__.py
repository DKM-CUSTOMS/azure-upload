import json
import logging
import os

import azure.functions as func
from azure.core.exceptions import AzureError

from AI_agents.Azure.content_understanding_item_table_extractor import (
    ContentUnderstandingItemTableExtractor,
)
from content_understanding_items.create_excel import write_items_to_excel

XLSX_MIMETYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Content Understanding item table extraction request.")

    try:
        req_body = req.get_json()
    except ValueError:
        return _json_response({"error": "Invalid JSON"}, status_code=400)

    files = req_body.get("files")
    if not isinstance(files, list) or not files:
        return _json_response(
            {"error": "Missing required 'files' array of {'file': '', 'filename': ''}"},
            status_code=400,
        )

    # Only the first attachment is processed to control Azure cost (one billed
    # Content Understanding call per request), while staying compatible with
    # callers that always send a 'files' array.
    attachment = files[0]
    if not isinstance(attachment, dict):
        return _json_response(
            {"error": "files[0] must be an object with 'file' and 'filename'"},
            status_code=400,
        )

    pdf_base64 = attachment.get("file")
    filename = attachment.get("filename", "document.pdf")

    if not pdf_base64:
        return _json_response(
            {"error": "Missing required 'file' base64 PDF payload in files[0]"},
            status_code=400,
        )

    try:
        extractor = ContentUnderstandingItemTableExtractor()
        result = extractor.extract_from_base64(pdf_base64=pdf_base64, filename=filename)

        excel_file = write_items_to_excel(result.get("columns"), result.get("items"))
        download_name = os.path.splitext(filename)[0] + ".xlsx"
        headers = {
            "Content-Disposition": f'attachment; filename="{download_name}"',
            "Content-Type": XLSX_MIMETYPE,
        }
        return func.HttpResponse(excel_file.getvalue(), headers=headers, mimetype=XLSX_MIMETYPE)
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
