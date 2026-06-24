import base64
import logging
import os
import re
import unicodedata
from typing import Any, Dict, List, Optional


ITEM_FIELD_NAMES = ("LineItems", "Items", "InvoiceItems", "Products")

KEY_ALIASES = {
    "Article nbr": ["article", "article number", "item number", "product code", "sku", "part number"],
    "Description": ["description", "item", "product", "goods", "mal tan", "mal tanimi", "mal tanimi"],
    "HS code": ["hs code", "hscode", "commodity code", "tariff", "gtip"],
    "Quantity": ["quantity", "qty", "miktar", "pieces", "adet", "units"],
    "Unit price": ["unit price", "price each", "birim fiyat"],
    "Unit": ["unit", "uom", "birim"],
    "Amount": ["amount", "line total", "total", "mal hizmet tutari", "mal hizmet tutari", "tutar"],
    "Currency": ["currency", "currency code"],
}

ITEM_TABLE_HEADER_TERMS = [
    "description",
    "item",
    "product",
    "goods",
    "mal tan",
    "gtip",
    "hs",
    "commodity",
    "tariff",
    "qty",
    "quantity",
    "miktar",
    "unit price",
    "birim",
    "amount",
    "total",
    "tutar",
]


class ContentUnderstandingItemTableExtractor:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        key: Optional[str] = None,
        analyzer_id: Optional[str] = None,
        api_version: Optional[str] = None,
    ):
        self.endpoint = (
            endpoint
            or os.getenv("CONTENTUNDERSTANDING_ENDPOINT")
            or os.getenv("AZURE_CONTENT_UNDERSTANDING_ENDPOINT")
        )
        self.key = (
            key
            or os.getenv("CONTENTUNDERSTANDING_KEY")
            or os.getenv("CONTENT_UNDERSTANDING_KEY")
        )
        self.analyzer_id = analyzer_id or os.getenv("CONTENTUNDERSTANDING_ANALYZER_ID", "prebuilt-invoice")
        self.api_version = api_version or os.getenv("CONTENTUNDERSTANDING_API_VERSION", "2025-11-01")

    def extract_from_base64(
        self,
        pdf_base64: str,
        filename: str = "document.pdf",
        analyzer_id: Optional[str] = None,
        content_range: Optional[str] = None,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        if not self.endpoint:
            raise ValueError("Missing CONTENTUNDERSTANDING_ENDPOINT or AZURE_CONTENT_UNDERSTANDING_ENDPOINT")

        pdf_bytes = self._decode_base64_pdf(pdf_base64)
        if not pdf_bytes:
            raise ValueError("Empty PDF payload")

        result, usage = self._analyze_pdf_bytes(
            pdf_bytes=pdf_bytes,
            analyzer_id=analyzer_id or self.analyzer_id,
            content_range=content_range,
        )
        extraction = self._extract_item_table(result)
        response = {
            "filename": filename or "document.pdf",
            "analyzer_id": analyzer_id or self.analyzer_id,
            "source": extraction["source"],
            "columns": extraction["columns"],
            "items": extraction["items"],
            "item_count": len(extraction["items"]),
            "usage": usage,
        }
        if include_raw:
            response["raw_result"] = self._safe_as_dict(result)
        return response

    def _analyze_pdf_bytes(self, pdf_bytes: bytes, analyzer_id: str, content_range: Optional[str]):
        from azure.ai.contentunderstanding import ContentUnderstandingClient
        from azure.core.credentials import AzureKeyCredential
        from azure.identity import DefaultAzureCredential

        credential = AzureKeyCredential(self.key) if self.key else DefaultAzureCredential()
        client = ContentUnderstandingClient(
            endpoint=self.endpoint,
            credential=credential,
            api_version=self.api_version,
        )

        kwargs = {"analyzer_id": analyzer_id, "binary_input": pdf_bytes}
        if content_range:
            kwargs["content_range"] = content_range

        poller = client.begin_analyze_binary(**kwargs)
        result = poller.result()
        usage = self._usage_to_dict(getattr(poller, "usage", None))

        close = getattr(credential, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                logging.debug("Failed to close Azure credential cleanly", exc_info=True)

        return result, usage

    def _extract_item_table(self, result: Any) -> Dict[str, Any]:
        contents = getattr(result, "contents", None) or []
        for content in contents:
            fields = getattr(content, "fields", None) or {}
            extracted = self._extract_from_fields(fields)
            if extracted["items"]:
                return extracted

        for content in contents:
            extracted = self._extract_from_tables(getattr(content, "tables", None) or [])
            if extracted["items"]:
                return extracted

        for content in contents:
            markdown = getattr(content, "markdown", "") or ""
            extracted = self._extract_from_markdown(markdown)
            if extracted["items"]:
                return extracted

        raw = self._safe_as_dict(result)
        extracted = self._extract_from_raw_dict(raw)
        if extracted["items"]:
            return extracted

        return {"source": "none", "columns": [], "items": []}

    def _extract_from_fields(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        for field_name in ITEM_FIELD_NAMES:
            item_field = fields.get(field_name)
            values = self._field_value(item_field)
            if not isinstance(values, list):
                continue

            rows = []
            for value in values:
                row = self._field_value(value)
                if not isinstance(row, dict):
                    continue
                normalized = self._normalize_item_row(row)
                if self._looks_like_item(normalized):
                    rows.append(normalized)

            if rows:
                return {
                    "source": f"fields.{field_name}",
                    "columns": self._ordered_columns(rows),
                    "items": rows,
                }

        return {"source": "fields", "columns": [], "items": []}

    def _extract_from_tables(self, tables: List[Any]) -> Dict[str, Any]:
        best = {"score": 0, "columns": [], "items": []}
        for table in tables:
            matrix = self._table_to_matrix(table)
            if len(matrix) < 2:
                continue
            headers = [self._clean_cell(value) for value in matrix[0]]
            score = self._score_headers(headers)
            if score < 3:
                continue
            rows = []
            for values in matrix[1:]:
                row = {
                    headers[index] or f"Column {index + 1}": self._clean_cell(value)
                    for index, value in enumerate(values)
                    if index < len(headers)
                }
                normalized = self._normalize_item_row(row)
                if self._looks_like_item(normalized):
                    rows.append(normalized)
            if rows and score > best["score"]:
                best = {"score": score, "columns": self._ordered_columns(rows), "items": rows}

        if best["items"]:
            return {"source": "layout.tables", "columns": best["columns"], "items": best["items"]}
        return {"source": "layout.tables", "columns": [], "items": []}

    def _extract_from_markdown(self, markdown: str) -> Dict[str, Any]:
        best = {"score": 0, "columns": [], "items": []}
        for table in self._markdown_tables(markdown):
            headers = table[0]
            score = self._score_headers(headers)
            if score < 3:
                continue
            rows = []
            for values in table[1:]:
                row = {
                    headers[index] or f"Column {index + 1}": values[index]
                    for index in range(min(len(headers), len(values)))
                }
                normalized = self._normalize_item_row(row)
                if self._looks_like_item(normalized):
                    rows.append(normalized)
            if rows and score > best["score"]:
                best = {"score": score, "columns": self._ordered_columns(rows), "items": rows}

        if best["items"]:
            return {"source": "markdown.tables", "columns": best["columns"], "items": best["items"]}
        return {"source": "markdown.tables", "columns": [], "items": []}

    def _extract_from_raw_dict(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        for content in raw.get("contents", []) or []:
            fields = content.get("fields", {}) if isinstance(content, dict) else {}
            extracted = self._extract_from_raw_fields(fields)
            if extracted["items"]:
                return extracted
        return {"source": "raw", "columns": [], "items": []}

    def _extract_from_raw_fields(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        for field_name in ITEM_FIELD_NAMES:
            field = fields.get(field_name)
            values = self._raw_field_value(field)
            if not isinstance(values, list):
                continue

            rows = []
            for value in values:
                row = self._raw_field_value(value)
                if isinstance(row, dict):
                    normalized = self._normalize_item_row(row)
                    if self._looks_like_item(normalized):
                        rows.append(normalized)

            if rows:
                return {
                    "source": f"raw.fields.{field_name}",
                    "columns": self._ordered_columns(rows),
                    "items": rows,
                }
        return {"source": "raw.fields", "columns": [], "items": []}

    def _field_value(self, field: Any) -> Any:
        if field is None:
            return None
        if isinstance(field, dict):
            return self._raw_field_value(field)
        if hasattr(field, "value_object") and getattr(field, "value_object"):
            return {
                key: self._field_value(value)
                for key, value in getattr(field, "value_object").items()
            }
        value = getattr(field, "value", None)
        if isinstance(value, dict):
            return {key: self._field_value(val) for key, val in value.items()}
        if isinstance(value, list):
            return [self._field_value(item) for item in value]
        return value

    def _raw_field_value(self, field: Any) -> Any:
        if not isinstance(field, dict):
            return field
        for key in ("value", "valueArray", "valueObject"):
            if key in field:
                value = field[key]
                if isinstance(value, list):
                    return [self._raw_field_value(item) for item in value]
                if isinstance(value, dict):
                    return {name: self._raw_field_value(item) for name, item in value.items()}
                return value
        return field.get("content") or field.get("text")

    def _table_to_matrix(self, table: Any) -> List[List[str]]:
        cells = getattr(table, "cells", None) or []
        row_count = getattr(table, "row_count", None) or 0
        column_count = getattr(table, "column_count", None) or 0
        if not cells or not row_count or not column_count:
            return []

        matrix = [["" for _ in range(column_count)] for _ in range(row_count)]
        for cell in cells:
            row_index = getattr(cell, "row_index", None)
            column_index = getattr(cell, "column_index", None)
            if row_index is None or column_index is None:
                continue
            if row_index >= row_count or column_index >= column_count:
                continue
            matrix[row_index][column_index] = self._clean_cell(
                getattr(cell, "content", None) or getattr(cell, "text", None) or getattr(cell, "value", "")
            )
        return matrix

    def _markdown_tables(self, markdown: str) -> List[List[List[str]]]:
        tables = []
        current = []
        for line in (markdown or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("|") and stripped.endswith("|"):
                cells = [self._clean_cell(cell) for cell in stripped.strip("|").split("|")]
                if re.fullmatch(r"[\s:\-|]+", stripped):
                    continue
                if all(re.fullmatch(r":?-{2,}:?", cell.replace(" ", "")) for cell in cells):
                    continue
                current.append(cells)
            elif current:
                if len(current) >= 2:
                    tables.append(current)
                current = []
        if len(current) >= 2:
            tables.append(current)
        return tables

    def _normalize_item_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {}
        used_keys = set()
        for target, aliases in KEY_ALIASES.items():
            for key, value in row.items():
                key_norm = self._normalize_key(key)
                if key in used_keys:
                    continue
                if key_norm == self._normalize_key(target) or any(alias in key_norm for alias in aliases):
                    normalized[target] = self._clean_cell(value)
                    used_keys.add(key)
                    break

        for key, value in row.items():
            if key not in used_keys and value not in (None, ""):
                normalized[self._clean_cell(key)] = self._clean_cell(value)

        return {key: value for key, value in normalized.items() if value not in (None, "")}

    def _looks_like_item(self, row: Dict[str, Any]) -> bool:
        if not row:
            return False
        has_description = bool(row.get("Description"))
        has_quantity = bool(row.get("Quantity"))
        has_amount = bool(row.get("Amount") or row.get("Unit price"))
        has_code = bool(row.get("HS code") or row.get("Article nbr"))
        return has_description and (has_quantity or has_amount or has_code)

    def _ordered_columns(self, rows: List[Dict[str, Any]]) -> List[str]:
        preferred = list(KEY_ALIASES.keys())
        present = []
        for column in preferred:
            if any(column in row for row in rows):
                present.append(column)
        for row in rows:
            for column in row:
                if column not in present:
                    present.append(column)
        return present

    def _score_headers(self, headers: List[str]) -> int:
        normalized = " ".join(self._normalize_key(header) for header in headers)
        return sum(1 for term in ITEM_TABLE_HEADER_TERMS if term in normalized)

    def _decode_base64_pdf(self, value: str) -> bytes:
        if not isinstance(value, str):
            return b""
        clean = value.strip()
        if clean.lower().startswith("data:") and "," in clean:
            clean = clean.split(",", 1)[1]
        return base64.b64decode(clean, validate=False)

    def _usage_to_dict(self, usage: Any) -> Dict[str, Any]:
        if usage is None:
            return {}
        if hasattr(usage, "as_dict"):
            return usage.as_dict()
        output = {}
        for key in ("document_pages_standard", "document_pages_basic", "document_pages_minimal", "contextualization_tokens", "tokens"):
            value = getattr(usage, key, None)
            if value is not None:
                output[key] = value
        return output

    def _safe_as_dict(self, value: Any) -> Dict[str, Any]:
        if hasattr(value, "as_dict"):
            return value.as_dict()
        return {}

    def _clean_cell(self, value: Any) -> str:
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    def _normalize_key(self, value: Any) -> str:
        value = self._clean_cell(value).lower().replace("\u0131", "i")
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
        return value.replace("_", " ")





