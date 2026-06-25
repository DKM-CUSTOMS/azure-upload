import base64
import json
import logging
import os
import re

import fitz
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class TransmarePDFExtractor:
    def __init__(
        self,
        key_vault_url="https://kv-functions-python.vault.azure.net",
        secret_name="OPENAI-API-KEY",
        model=None,
        timeout=120,
    ):
        self.key_vault_url = key_vault_url
        self.secret_name = secret_name
        self.model = model or os.getenv("TRANSMARE_OPENAI_MODEL", "gpt-4o-2024-08-06")
        self.timeout = timeout
        self.api_key = None
        self.initialize_api_key()

    def initialize_api_key(self):
        try:
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=self.key_vault_url, credential=credential)
            self.api_key = client.get_secret(self.secret_name).value
            return True
        except Exception as e:
            logging.error(f"Failed to retrieve OpenAI API key: {str(e)}")
            return False

    def classify_pdf(self, pdf_base64, filename="transmare.pdf"):
        clean_base64 = self._strip_data_url_prefix(pdf_base64)
        text = self._extract_pdf_text(clean_base64)
        rule_result = self._classify_from_text(text, filename)

        if rule_result["decision"] != "uncertain":
            return rule_result

        ai_result = self._classify_with_ai(clean_base64, filename, text)
        return ai_result or rule_result

    def extract_transmare_json(self, pdf_base64, filename="transmare.pdf"):
        if not self.api_key:
            logging.error("OpenAI API key is not initialized")
            return None

        clean_base64 = self._strip_data_url_prefix(pdf_base64)
        payload = {
            "model": self.model,
            "temperature": 0,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_file",
                            "filename": filename or "transmare.pdf",
                            "file_data": f"data:application/pdf;base64,{clean_base64}",
                        },
                        {
                            "type": "input_text",
                            "text": self._build_prompt(),
                        },
                    ],
                }
            ],
        }

        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            raw_text = self._extract_output_text(response.json())
            extracted = self._parse_json(raw_text)
            if self._needs_repair(extracted):
                repaired = self._repair_extraction(clean_base64, filename, extracted)
                return repaired or extracted
            return extracted
        except requests.exceptions.RequestException as e:
            logging.error(f"OpenAI PDF extraction request failed: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected OpenAI PDF extraction error: {e}", exc_info=True)
            return None

    def _extract_pdf_text(self, clean_base64):
        try:
            pdf_bytes = base64.b64decode(clean_base64)
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                return "\n".join(page.get_text() for page in doc)
        except Exception as e:
            logging.warning(f"Could not extract PDF text for classification: {e}")
            return ""

    def _needs_repair(self, extracted):
        invoices = self._get_invoice_list(extracted)
        if not invoices:
            return False

        for invoice in invoices:
            items = invoice.get("Items") or []
            if not items:
                return True

            if self._to_float(invoice.get("Gross weight Total")) == 0:
                return True

            pieces_values = [self._to_float(item.get("Pieces")) for item in items if isinstance(item, dict)]
            if pieces_values and all(value == 0 for value in pieces_values):
                return True

            gross_values = [self._to_float(item.get("Gross weight")) for item in items if isinstance(item, dict)]
            if gross_values and all(value == 0 for value in gross_values):
                return True

            for item in items:
                if not isinstance(item, dict):
                    continue
                pieces = self._to_float(item.get("Pieces"))
                gross_weight = self._to_float(item.get("Gross weight"))
                net_weight = self._to_float(item.get("Net weight"))
                if pieces == 0:
                    return True
                if net_weight == 0 and gross_weight > 0:
                    return True
                if gross_weight == 0 and (net_weight > 0 or pieces > 0):
                    return True

        return False

    def _repair_extraction(self, clean_base64, filename, extracted):
        if not self.api_key:
            logging.error("OpenAI API key is not initialized")
            return None

        payload = {
            "model": self.model,
            "temperature": 0,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_file",
                            "filename": filename or "transmare.pdf",
                            "file_data": f"data:application/pdf;base64,{clean_base64}",
                        },
                        {
                            "type": "input_text",
                            "text": self._build_repair_prompt(extracted, self._extract_pdf_text(clean_base64)),
                        },
                    ],
                }
            ],
        }

        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            repaired = self._parse_json(self._extract_output_text(response.json()))
            if self._needs_repair(repaired):
                logging.warning("OpenAI repair still has missing Pieces, Net weight, or Gross weight values")
            return repaired
        except requests.exceptions.RequestException as e:
            logging.error(f"OpenAI PDF repair request failed: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected OpenAI PDF repair error: {e}", exc_info=True)
            return None

    def _classify_from_text(self, text, filename):
        normalized = self._normalize_text(text)
        reasons = []
        blockers = []

        invoice_titles = [
            "commercial invoice", "proforma invoice", "proforma facture",
            "facture proforma", "invoice", "facture", "factuur", "rechnung",
            "fatura", "e-fatura", "sales invoice", "tax invoice",
            "order confirmation", "confirmation de commande",
        ]
        # Real invoice document-type titles. A bare "invoice" substring is NOT
        # here on purpose: carrier docs (booking confirmations) mention it too.
        strong_invoice_titles = [
            "commercial invoice", "proforma invoice", "proforma facture",
            "facture proforma", "sales invoice", "tax invoice",
            "factuur", "rechnung", "fatura", "e-fatura",
        ]
        # Carrier/transport document types that are never a commercial invoice.
        decisive_non_invoice_titles = [
            "booking confirmation", "bill of lading", "arrival notice",
            "shipping instruction", "sea waybill",
        ]
        invoice_fields = [
            "invoice no", "invoice number", "invoice date", "fatura no",
            "fatura tarihi", "vkn", "vat", "ettn", "total amount", "grand total",
            "payable amount", "odenecek tutar", "mal hizmet toplam",
        ]
        goods_fields = [
            "hs code", "commodity", "tariff", "gtip", "description", "quantity",
            "unit price", "amount", "currency", "gross weight", "net weight",
            "incoterm", "origin",
        ]
        non_invoice_titles = [
            "packing list", "bill of lading", "cmr", "certificate of origin",
            "delivery note", "arrival notice", "booking confirmation",
            "shipping instruction", "purchase order", "quote", "quotation",
        ]

        title_hits = [term for term in invoice_titles if term in normalized]
        strong_title_hits = [term for term in strong_invoice_titles if term in normalized]
        field_hits = [term for term in invoice_fields if term in normalized]
        goods_hits = [term for term in goods_fields if term in normalized]
        negative_hits = [term for term in non_invoice_titles if term in normalized]
        # Position of the earliest carrier-doc title vs the earliest real invoice
        # title. The document's own type leads the page; references to other doc
        # types appear later in the body. Comparing positions tells them apart even
        # when a booking's fine print mentions "commercial invoice".
        decisive_negative_pos = self._earliest_position(decisive_non_invoice_titles, normalized)
        strong_title_pos = self._earliest_position(strong_invoice_titles, normalized)
        money_hits = len(re.findall(r"\b(?:eur|usd|gbp|try|mad|dkk|sek|nok|chf)\b|[$]", normalized))
        hs_hits = len(re.findall(r"\b\d{8,12}\b", normalized))

        if title_hits:
            reasons.append(f"invoice title terms: {', '.join(title_hits[:4])}")
        if field_hits:
            reasons.append(f"invoice field terms: {', '.join(field_hits[:4])}")
        if goods_hits:
            reasons.append(f"goods table terms: {', '.join(goods_hits[:4])}")
        if money_hits:
            reasons.append(f"currency/amount markers: {money_hits}")
        if hs_hits:
            reasons.append(f"possible HS codes: {hs_hits}")
        if negative_hits:
            blockers.append(f"non-invoice terms: {', '.join(negative_hits[:4])}")

        # A document led by a carrier/transport title (booking confirmation, B/L,
        # ...) ahead of any real invoice title is decisively not an invoice,
        # regardless of how many goods/currency/HS markers it carries.
        if decisive_negative_pos is not None and (
            strong_title_pos is None or decisive_negative_pos < strong_title_pos
        ):
            leading = [
                term for term in decisive_non_invoice_titles
                if normalized.find(term) == decisive_negative_pos
            ]
            return {
                "decision": "non_invoice",
                "confidence": 0.92,
                "filename": filename,
                "reasons": reasons,
                "blockers": blockers or [f"non-invoice document type: {', '.join(leading[:4])}"],
            }

        score = (
            min(len(title_hits), 2) * 35
            + min(len(field_hits), 4) * 10
            + min(len(goods_hits), 5) * 6
            + min(money_hits, 4) * 4
            + min(hs_hits, 4) * 5
        )
        # Subtract for non-invoice markers unless a real invoice title is present;
        # a bare "invoice" substring must not buy immunity from this penalty.
        if negative_hits and not strong_title_hits:
            score -= min(len(negative_hits), 3) * 20

        if score >= 65 and title_hits and (field_hits or goods_hits or money_hits):
            return {
                "decision": "invoice",
                "confidence": min(0.99, round(score / 100, 2)),
                "filename": filename,
                "reasons": reasons,
                "blockers": blockers,
            }

        if score <= 10 and negative_hits:
            return {
                "decision": "non_invoice",
                "confidence": 0.9,
                "filename": filename,
                "reasons": reasons,
                "blockers": blockers,
            }

        return {
            "decision": "uncertain",
            "confidence": round(max(0.1, min(score / 100, 0.7)), 2),
            "filename": filename,
            "reasons": reasons,
            "blockers": blockers,
        }

    def _classify_with_ai(self, clean_base64, filename, text):
        if not self.api_key:
            logging.error("OpenAI API key is not initialized")
            return None

        payload = {
            "model": self.model,
            "temperature": 0,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_file",
                            "filename": filename or "transmare.pdf",
                            "file_data": f"data:application/pdf;base64,{clean_base64}",
                        },
                        {
                            "type": "input_text",
                            "text": self._build_classification_prompt(text),
                        },
                    ],
                }
            ],
        }

        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            raw_text = self._extract_output_text(response.json())
            result = self._parse_json(raw_text)
            if not isinstance(result, dict):
                return None

            decision = result.get("decision", "uncertain")
            if decision not in ["invoice", "non_invoice", "uncertain"]:
                decision = "uncertain"

            confidence = result.get("confidence", 0)
            try:
                confidence = float(confidence)
            except (TypeError, ValueError):
                confidence = 0

            if decision == "invoice" and confidence < 0.85:
                decision = "uncertain"
            if decision == "non_invoice" and confidence < 0.9:
                decision = "uncertain"

            return {
                "decision": decision,
                "confidence": max(0, min(confidence, 1)),
                "filename": filename,
                "reasons": result.get("reasons", []),
                "blockers": result.get("blockers", []),
            }
        except requests.exceptions.RequestException as e:
            logging.error(f"OpenAI PDF classification request failed: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected OpenAI PDF classification error: {e}", exc_info=True)
            return None

    def _build_prompt(self):
        return """
You are extracting invoice data for the existing Transmare Azure Function.

Return ONLY valid JSON. Do not wrap it in markdown.

Output shape:
{
  "invoices": [
    {
      "Vat Number": "",
      "Principal": "",
      "Inv Reference": "",
      "Other Ref": "",
      "Inv Date": "",
      "Incoterm": "",
      "Total": "",
      "Freight": "",
      "Gross weight Total": "",
      "Currency": "",
      "Export office": "",
      "Truck": "",
      "Customs Code": "",
      "Address": [
        {
          "Company name": "",
          "Street": "",
          "City": "",
          "Postal Code": "",
          "Country": ""
        }
      ],
      "Items": [
        {
          "Article nbr": "",
          "Description": "",
          "HS code": "",
          "Origin": "",
          "Pieces": "",
          "Gross weight": "",
          "Net weight": "",
          "Price": "",
          "Customs Code": ""
        }
      ]
    }
  ]
}

Rules:
- Keep the exact key names shown above. Do not rename fields.
- Extract every invoice in the PDF. If the PDF contains one invoice, return one object in "invoices".
- Extract every item row. Do not summarize, group, skip, or truncate rows.
- Address must be an array with one object for the receiver/consignee/export customer address.
- Items must be an array of objects. Every real item row must include "Article nbr"; if there is no article number, use the row number or best visible item identifier.
- "Pieces" is the invoice line quantity/collis/package count. Map columns named Qty, Quantity, Collis, Colis, Number of packages, Packages, Bags, Drums, Cartons, or Units to "Pieces".
- Do not put 0 in "Pieces" unless the invoice visibly says the quantity/collis is zero. If the line shows "200 BAG25", "Pieces" must be "200".
- "Net weight" must be the line net weight, not left blank when the PDF has a net/netto/weight column. If a line shows a package size such as BAG25 or DRUM50 and quantity is visible, use the visible invoice net weight or the quantity times package size only when the PDF layout clearly supports it.
- Extract invoice-level "Gross weight Total" and item-level "Gross weight" separately. Do not copy the total gross into every item unless there is exactly one item line.
- Gross weight is mandatory when visible. Look for labels such as Gross, Gross weight, Gross Wt, G.W., GW, Bruto, Brut, Peso bruto, and Total gross.
- Do not put 0 in "Gross weight" or "Gross weight Total" unless the invoice visibly says the gross weight is zero. If gross is not visible, use an empty string.
- Preserve number formatting as seen in the PDF when uncertain. The Transmare function will normalize numbers later.
- "Incoterm" should be one string like "EXW Istanbul" or "DAP Antwerp" when available.
- "Origin" may be a country name or country code from the invoice.
- "HS code" must contain only the commodity/HS code for that row.
- "Price" is the invoice line amount, not unit price, unless only unit price exists.
- Missing values must be empty strings, not null. Prefer empty string over invented 0 for missing Pieces, Net weight, Gross weight, and Price.
""".strip()

    def _build_classification_prompt(self, text):
        text_sample = (text or "")[:8000]
        return f"""
Classify the attached PDF before invoice extraction.

Return ONLY valid JSON with this exact shape:
{{
  "decision": "invoice | non_invoice | uncertain",
  "confidence": 0.0,
  "reasons": [],
  "blockers": []
}}

Decision rules:
- Use "invoice" only when the file is clearly an invoice/proforma/commercial invoice/order confirmation with invoice-like totals or payable commercial line items.
- Use "non_invoice" only when the file is clearly not invoice-like.
- Use "uncertain" for packing lists, transport documents, certificates, unclear scans, or mixed documents where an invoice might be present but is not certain.
- Prefer "uncertain" over a risky guess. Ambiguous files must not be discarded or extracted.
- Reasons must cite visible evidence, not assumptions.

Text extracted locally, for reference:
{text_sample}
""".strip()

    def _build_repair_prompt(self, extracted, pdf_text=""):
        current_json = json.dumps(extracted, ensure_ascii=False, indent=2)
        text_sample = (pdf_text or "")[:12000]
        return f"""
You already extracted this Transmare invoice JSON, but critical logistics fields are missing or zero.
Re-read the attached PDF and return the complete corrected JSON in the exact same shape.
Return ONLY valid JSON. Do not wrap in markdown.

Current JSON to repair:
{current_json}

Text extracted locally from the PDF, useful for finding gross/net/quantity labels:
{text_sample}

Repair rules:
- Keep all invoice and item rows. Do not remove or reorder items.
- Fix "Pieces" for every item from the invoice quantity/collis/package count column. Column names may be Qty, Quantity, Collis, Colis, Packages, Bags, Drums, Cartons, or Units.
- Never leave "Pieces" as 0 if the invoice line has a visible quantity/collis value. Example: "200 BAG25" means "Pieces": "200".
- Fix "Net weight" for every item from the line net/netto/weight column.
- Never leave "Net weight" as 0 if the line has visible net weight. For package-coded items like BAG25 or DRUM50, use the visible net weight if present; if the invoice clearly shows quantity and package size, the net weight is quantity times package size.
- Fix "Gross weight" for every item from the line gross/gross weight/bruto/G.W./GW column.
- Fix "Gross weight Total" from the invoice total gross / total G.W. / total bruto value. Never leave it as 0 if any total gross value is visible.
- If there is exactly one item line and the invoice shows only one total gross weight, use that same gross value for the item "Gross weight" and invoice "Gross weight Total".
- Gross weight is usually equal to or greater than net weight. If extracted gross is lower than net, re-check the value.
- Fix invoice totals if visible: "Gross weight Total", "Total", "Freight", and "Currency".
- If a value is genuinely absent after careful inspection, use an empty string, not 0.
""".strip()

    def _get_invoice_list(self, extracted):
        if isinstance(extracted, dict) and isinstance(extracted.get("invoices"), list):
            return extracted.get("invoices")
        if isinstance(extracted, dict) and "Items" in extracted:
            return [extracted]
        if isinstance(extracted, list):
            return [entry for entry in extracted if isinstance(entry, dict)]
        return []

    def _to_float(self, value):
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        cleaned = re.sub(r"[^0-9,.-]", "", str(value)).strip()
        if not cleaned:
            return 0.0
        if "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def _extract_output_text(self, response_json):
        if response_json.get("output_text"):
            return response_json["output_text"]

        chunks = []
        for output in response_json.get("output", []):
            for content in output.get("content", []):
                text = content.get("text")
                if text:
                    chunks.append(text)
        return "\n".join(chunks).strip()

    def _parse_json(self, raw_text):
        if not raw_text:
            logging.error("OpenAI PDF extraction returned no text")
            return None

        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", cleaned)
            if match:
                cleaned = match.group(1).strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logging.error(f"Failed to parse OpenAI JSON: {cleaned[:1000]}")
            return None

    def _strip_data_url_prefix(self, pdf_base64):
        if not isinstance(pdf_base64, str):
            return ""
        if "," in pdf_base64 and pdf_base64.strip().lower().startswith("data:"):
            return pdf_base64.split(",", 1)[1]
        return pdf_base64.strip()

    def _normalize_text(self, text):
        return re.sub(r"\s+", " ", (text or "").lower())

    def _earliest_position(self, terms, normalized):
        positions = [normalized.find(term) for term in terms if term in normalized]
        return min(positions) if positions else None
