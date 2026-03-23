"""
di_client.py
------------
Azure Document Intelligence Layout client.

Uses the installed SDK: azure-ai-formrecognizer (DocumentAnalysisClient)
with model "prebuilt-layout".

Returns a clean dict with:
    - markdown_content  (str)  – reconstructed text from the layout result
    - tables            (list) – list of 2-D grids (row x col) of cell strings
    - paragraphs        (list) – list of paragraph content strings
    - page_count        (int)  – total pages in the document

Credentials are loaded from Azure Key Vault via DefaultAzureCredential,
matching the pattern already used throughout this project.

NOTE
----
azure-ai-formrecognizer 3.3.x returns an AnalyzeResult with:
    result.content          → full text (not markdown, but the best we have with this SDK)
    result.tables           → list of DocumentTable
    result.paragraphs       → list of DocumentParagraph
    result.pages            → list of DocumentPage

The newer azure-ai-documentintelligence SDK supports markdown output natively,
but since this project already depends on formrecognizer 3.3.x, we use that.
If you upgrade to azure-ai-documentintelligence, set output_content_format="markdown"
on begin_analyze_document and the markdown_content field will be proper markdown.
"""

import base64
import logging

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential


# ─────────────────────────────────────────────
#  Defaults (same endpoints/secrets as the rest of the project)
# ─────────────────────────────────────────────

DEFAULT_KEY_VAULT_URL = "https://kv-functions-python.vault.azure.net"
DEFAULT_SECRET_NAME   = "azure-form-recognizer-key-2"
DEFAULT_DI_ENDPOINT   = "https://document-intelligence-python.cognitiveservices.azure.com/"


class DILayoutClient:
    """
    Wraps Azure Document Intelligence Layout analysis (prebuilt-layout).

    Usage
    -----
        client = DILayoutClient()
        result = client.analyze_layout(pdf_base64)
        # result["markdown_content"] -> str  (the document's full text content)
        # result["tables"]           -> list of list[list[str]]
        # result["paragraphs"]       -> list of str
        # result["page_count"]       -> int
    """

    def __init__(
        self,
        key_vault_url: str = DEFAULT_KEY_VAULT_URL,
        secret_name:   str = DEFAULT_SECRET_NAME,
        endpoint:      str = DEFAULT_DI_ENDPOINT,
    ):
        self.endpoint = endpoint
        self.api_key  = None
        self._client  = None
        self._load_credentials(key_vault_url, secret_name)

    # ──────────────────────────────────────────
    #  Credential initialisation
    # ──────────────────────────────────────────

    def _load_credentials(self, key_vault_url: str, secret_name: str) -> None:
        """Retrieve the DI API key from Azure Key Vault."""
        try:
            credential   = DefaultAzureCredential()
            kv_client    = SecretClient(vault_url=key_vault_url, credential=credential)
            self.api_key = kv_client.get_secret(secret_name).value
            self._client = DocumentAnalysisClient(
                endpoint=self.endpoint,
                credential=AzureKeyCredential(self.api_key),
            )
            logging.info("DILayoutClient: credentials loaded successfully from Key Vault")
        except Exception as exc:
            logging.error(f"DILayoutClient: failed to load credentials – {exc}")

    # ──────────────────────────────────────────
    #  Public API
    # ──────────────────────────────────────────

    def analyze_layout(self, pdf_base64: str) -> dict:
        """
        Send a base64-encoded PDF to Azure Document Intelligence prebuilt-layout.

        Parameters
        ----------
        pdf_base64 : str
            Base64-encoded PDF bytes.

        Returns
        -------
        dict:
            markdown_content  str              – full document text content
            tables            list[list[str]]  – each table is a 2-D grid of cell strings
            paragraphs        list[str]        – paragraph text blocks
            page_count        int              – number of pages detected
        """
        if not self._client:
            logging.error("DILayoutClient: client not initialised – cannot analyse document")
            return self._empty_result(error="Client not initialised")

        logging.info("DILayoutClient: starting prebuilt-layout analysis …")

        try:
            pdf_bytes = base64.b64decode(pdf_base64)

            poller = self._client.begin_analyze_document(
                "prebuilt-layout",
                pdf_bytes,
            )
            result = poller.result()
            logging.info("DILayoutClient: DI analysis polling complete")

            return self._parse_result(result)

        except Exception as exc:
            logging.error(f"DILayoutClient: analysis failed – {exc}")
            return self._empty_result(error=str(exc))

    # ──────────────────────────────────────────
    #  Internal parsers
    # ──────────────────────────────────────────

    def _parse_result(self, result) -> dict:
        """
        Convert raw AnalyzeResult (formrecognizer 3.3.x) to a plain dict.

        result.content     → full text of the document
        result.tables      → list of DocumentTable objects
        result.paragraphs  → list of DocumentParagraph objects
        result.pages       → list of DocumentPage objects
        """

        # ── full document text (used as "markdown_content") ──────────────────
        # formrecognizer 3.3 returns plain text, not markdown.
        # BLClassifier and BLFieldExtractor both work fine with plain text.
        markdown_content: str = (result.content or "").strip()

        # ── tables ───────────────────────────────────────────────────────────
        tables: list[list[list[str]]] = []
        for table in (result.tables or []):
            row_count = table.row_count
            col_count = table.column_count
            # Build a 2-D grid
            grid = [[""] * col_count for _ in range(row_count)]
            for cell in table.cells:
                r, c = cell.row_index, cell.column_index
                grid[r][c] = (cell.content or "").strip()
            tables.append(grid)

        # ── paragraphs ───────────────────────────────────────────────────────
        paragraphs: list[str] = []
        for para in (result.paragraphs or []):
            text = (para.content or "").strip()
            if text:
                paragraphs.append(text)

        # ── page count ───────────────────────────────────────────────────────
        page_count: int = len(result.pages) if result.pages else 0

        logging.info(
            f"DILayoutClient: parsed {page_count} pages, "
            f"{len(tables)} tables, "
            f"{len(paragraphs)} paragraphs, "
            f"{len(markdown_content):,} content chars"
        )

        return {
            "markdown_content": markdown_content,
            "tables":           tables,
            "paragraphs":       paragraphs,
            "page_count":       page_count,
        }

    @staticmethod
    def _empty_result(error: str = "") -> dict:
        return {
            "markdown_content": "",
            "tables":           [],
            "paragraphs":       [],
            "page_count":       0,
            "error":            error,
        }

    # ──────────────────────────────────────────
    #  Compact table text helper (for LLM prompt)
    # ──────────────────────────────────────────

    @staticmethod
    def tables_to_compact_text(tables: list) -> str:
        """
        Convert the list-of-grids structure into a plain-text block
        for inclusion in the LLM extraction prompt.

        Each table is rendered as a pipe-separated ASCII table.

        Parameters
        ----------
        tables : list[list[list[str]]]
            Outer list = tables, middle list = rows, inner list = columns.
        """
        lines: list[str] = []
        for idx, grid in enumerate(tables):
            lines.append(f"--- Table {idx + 1} ---")
            for row in grid:
                lines.append(" | ".join(cell.replace("\n", " ") for cell in row))
            lines.append("")
        return "\n".join(lines)
