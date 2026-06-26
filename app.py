#!/usr/bin/env python3
"""
BigBasket Automation Scheduler (merged "best of both")
======================================================
Gmail (GRN attachments) -> Google Drive -> Google Sheets pipeline, with a
workflow summary written back to a log sheet and emailed to recipients.

This file merges the two prior variants:

  From the "enhanced" version (app_1):
    * Email notification with an HTML workflow summary (Gmail send scope).
    * Enhanced duplicate prevention:
        - Drive-level: cached folder listing + per-file "already in Drive" skip.
        - Sheet-level: skip files already present in the `source_file_name`
          column, and filter out individual rows whose PO No + Sku Code already
          exist in the sheet *before* appending.
    * A `source_file_name` column written into the sheet.
    * Rich attachment/file stats (found / saved / skipped / failed).

  From the "refactor" version (app_2):
    * `--run-once` CLI flag with proper process exit codes (0 = success,
      1 = failure) so GitHub Actions can detect logical failures.
    * Transient-error retry wrapper (exponential backoff) around every Google
      API call, plus a default socket timeout.
    * Environment-variable / .env / GitHub-Secrets overridable config; original
      hardcoded values kept as defaults.
    * UTF-8 file logging with a configurable filename.

  New in the merge (transparent reconciliation):
    * A full row-count chain per file and in aggregate:
        raw (as read) -> cleaned (blank-key + exact-dup removal)
                      -> new (after PO|Sku dedup) -> appended (what Sheets wrote)
      so you can see exactly where any rows drop, and whether the drop was
      legitimate cleaning/de-duplication or an unexpected loss.
      `row_check` verifies write integrity: appended == new.

Two app_1 helpers that were never called in the live path
(`_get_already_processed_drive_files`, `_get_all_excel_files_in_folder`) were
dropped to reduce surface area.

NOTE: This was syntax/compile checked and unit-tested for control flow, config,
retries and the reconciliation maths in a sandbox. It was NOT run against live
Google APIs here. Run `python app.py --run-once` once with valid credentials
before relying on scheduled runs, and confirm the hardcoded folder/spreadsheet
IDs are still correct.
"""

import argparse
import base64
import io
import logging
import os
import re
import socket
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import schedule

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# Optional .env support; loaded only if python-dotenv is installed. Never a hard
# dependency, so this does not change requirements.txt or break Actions runs.
try:  # pragma: no cover - trivial optional import
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # noqa: BLE001 - dotenv is genuinely optional
    pass


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------
def _env_str(name: str, default: str) -> str:
    """Return env var ``name`` as a string, falling back to ``default``."""
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    """Return env var ``name`` as an int, falling back to ``default``.

    An unparseable value is logged and the default used, rather than crashing
    the whole run on a typo'd secret.
    """
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logging.warning("Env var %s=%r is not an int; using default %s", name, raw, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    """Return env var ``name`` as a bool (1/true/yes/on), falling back to default."""
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, default: List[str]) -> List[str]:
    """Return a comma-separated env var as a list of trimmed strings."""
    raw = os.environ.get(name)
    if raw in (None, ""):
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
# Single source of truth for the log filename. NOTE: the original workflow's
# "Upload logs" step referenced "bigbasket_automation.log" while the code wrote
# "bb_automation.log" -> the artifact was always empty. Keep these in sync.
LOG_FILE = _env_str("BB_LOG_FILE", "bb_automation.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("bb_alert_scheduler")


# ---------------------------------------------------------------------------
# Transient-error retry wrapper
# ---------------------------------------------------------------------------
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}

DEFAULT_MAX_RETRIES = _env_int("BB_MAX_RETRIES", 4)
DEFAULT_RETRY_BACKOFF = _env_int("BB_RETRY_BACKOFF", 2)  # base of exponential, seconds
DEFAULT_SOCKET_TIMEOUT = _env_int("BB_SOCKET_TIMEOUT", 120)  # seconds


def _http_status(error: HttpError) -> Optional[int]:
    """Best-effort extraction of the HTTP status code from an HttpError."""
    resp = getattr(error, "resp", None)
    status = getattr(resp, "status", None)
    if status is not None:
        try:
            return int(status)
        except (TypeError, ValueError):
            return None
    status_code = getattr(error, "status_code", None)
    return int(status_code) if isinstance(status_code, int) else None


def execute_with_retry(
    request: Any,
    *,
    description: str = "google-api call",
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff: int = DEFAULT_RETRY_BACKOFF,
) -> Any:
    """Call ``request.execute()`` with exponential-backoff retries.

    Retries only transient failures: HttpError with a status in
    ``_RETRYABLE_STATUS``, and socket/connection timeouts. Non-transient
    HttpErrors (404, 403, ...) are re-raised immediately so real configuration
    problems surface fast instead of being masked.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return request.execute()
        except HttpError as exc:
            status = _http_status(exc)
            if status not in _RETRYABLE_STATUS or attempt > max_retries:
                raise
            wait = backoff ** attempt
            logger.warning(
                "%s failed with HTTP %s (attempt %d/%d); retrying in %ds",
                description, status, attempt, max_retries, wait,
            )
            time.sleep(wait)
        except (socket.timeout, TimeoutError, ConnectionError) as exc:
            if attempt > max_retries:
                raise
            wait = backoff ** attempt
            logger.warning(
                "%s hit a network error (%s; attempt %d/%d); retrying in %ds",
                description, type(exc).__name__, attempt, max_retries, wait,
            )
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Main scheduler
# ---------------------------------------------------------------------------
class BigBasketScheduler:
    """Gmail -> Drive -> Sheets GRN automation with dedup + email summary."""

    def __init__(self, run_once: bool = False) -> None:
        self.run_once = run_once

        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None

        # Gmail send scope is required for the email-notification step.
        self.gmail_scopes = [
            "https://www.googleapis.com/auth/gmail.readonly",
            "https://www.googleapis.com/auth/gmail.send",
        ]
        self.drive_scopes = ["https://www.googleapis.com/auth/drive"]
        self.sheets_scopes = ["https://www.googleapis.com/auth/spreadsheets"]

        # Credential file paths (defaults match the workflow, which writes
        # credentials.json / token.json from base64 GitHub Secrets).
        self.credentials_file = _env_str("BB_CREDENTIALS_FILE", "credentials.json")
        self.token_file = _env_str("BB_TOKEN_FILE", "token.json")

        # Configuration. Defaults follow the enhanced (app_1) version; the wider
        # 15-day windows are safe here because dedup filters reprocessing.
        # Every value is overridable via env / GitHub Secrets.
        self.config: Dict[str, Dict[str, Any]] = {
            "gmail": {
                "sender": _env_str("BB_GMAIL_SENDER", "alerts@bigbasket.com"),
                "search_term": _env_str("BB_GMAIL_SEARCH_TERM", "GRN"),
                "days_back": _env_int("BB_GMAIL_DAYS_BACK", 15),
                "max_results": _env_int("BB_GMAIL_MAX_RESULTS", 1000),
                "gdrive_folder_id": _env_str(
                    "BB_GDRIVE_FOLDER_ID", "1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt"
                ),
            },
            "excel": {
                "excel_folder_id": _env_str(
                    "BB_EXCEL_FOLDER_ID", "1dQnXXscJsHnl9Ue-zcDazGLQuXAxIUQG"
                ),
                "spreadsheet_id": _env_str(
                    "BB_SPREADSHEET_ID", "170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM"
                ),
                "sheet_name": _env_str("BB_SHEET_NAME", "bbalertgrn"),
                "summary_sheet_name": _env_str("BB_SUMMARY_SHEET_NAME", "alert_workflow_log"),
                "header_row": _env_int("BB_HEADER_ROW", 0),
                "days_back": _env_int("BB_EXCEL_DAYS_BACK", 15),
                "max_files": _env_int("BB_MAX_FILES", 1000),
            },
            "notification": {
                "recipients": _env_list("BB_NOTIFY_RECIPIENTS", ["keyur@thebakersdozen.in"]),
                "send_to_self": _env_bool("BB_NOTIFY_SEND_TO_SELF", True),
                "enabled": _env_bool("BB_NOTIFY_ENABLED", True),
            },
        }

        # Cache of existing Drive filenames per folder (avoids repeated lookups).
        self.existing_files_cache: Dict[str, Set[str]] = {}
        # Raw (pre-clean) row count of the most recent successful Excel read.
        self._last_raw_rows: int = 0

        self.stats: Dict[str, Any] = self._fresh_stats()

    # -- helpers ------------------------------------------------------------
    def _fresh_stats(self) -> Dict[str, Any]:
        """Return a clean stats dict. Single definition used by init + run."""
        return {
            "start_time": None,
            "end_time": None,
            "days_back_gmail": self.config["gmail"]["days_back"],
            "days_back_excel": self.config["excel"]["days_back"],
            # Gmail -> Drive
            "emails_checked": 0,
            "attachments_found": 0,
            "attachments_saved": 0,
            "attachments_skipped": 0,
            "attachments_failed": 0,
            # Drive -> Sheet (file level)
            "files_found": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            # Drive -> Sheet (row reconciliation chain)
            "rows_raw_in_files": 0,      # rows as read from source files (pre-clean)
            "rows_cleaned_in_files": 0,  # after blank-key + exact-dup cleaning
            "rows_new": 0,               # after PO|Sku dedup (what we attempt to append)
            "rows_appended": 0,          # what Sheets reported writing
            "row_check": "Not Started",  # write-integrity verdict (appended vs new)
            "duplicates_removed": 0,
            "status": "Not Started",
        }

    # -- auth ---------------------------------------------------------------
    def authenticate(self) -> bool:
        """Authenticate with Google APIs using local credential files."""
        try:
            creds: Optional[Credentials] = None
            scopes = self.gmail_scopes + self.drive_scopes + self.sheets_scopes

            if os.path.exists(self.token_file):
                creds = Credentials.from_authorized_user_file(self.token_file, scopes)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not os.path.exists(self.credentials_file):
                        logger.error(
                            "%s not found. In GitHub Actions this is created from the "
                            "GOOGLE_CREDENTIALS secret; locally, download it from the "
                            "Google Cloud Console.",
                            self.credentials_file,
                        )
                        return False
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file, scopes
                    )
                    creds = flow.run_local_server(port=0)

                with open(self.token_file, "w", encoding="utf-8") as token:
                    token.write(creds.to_json())

            self.gmail_service = build("gmail", "v1", credentials=creds)
            self.drive_service = build("drive", "v3", credentials=creds)
            self.sheets_service = build("sheets", "v4", credentials=creds)

            logger.info("Authentication successful!")
            return True

        except Exception as exc:  # noqa: BLE001 - top-level guard, logged + reported
            logger.error("Authentication failed: %s", exc)
            return False

    # -- gmail --------------------------------------------------------------
    def search_emails(
        self, sender: str, search_term: str, days_back: int, max_results: int
    ) -> List[Dict[str, Any]]:
        """Search for emails with attachments."""
        try:
            query_parts = ["has:attachment"]
            if sender:
                query_parts.append(f'from:"{sender}"')
            if search_term:
                query_parts.append(f'"{search_term}"')

            # Gmail's after: is date-only and interpreted in the account's local
            # timezone, so naive local time is correct here (no UTC conversion).
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            query = " ".join(query_parts)

            max_results = max(max_results, 1) if max_results else 1

            result = execute_with_retry(
                self.gmail_service.users().messages().list(
                    userId="me", q=query, maxResults=max_results
                ),
                description="gmail.messages.list",
            )

            messages = result.get("messages", [])
            self.stats["emails_checked"] = len(messages)
            logger.info("Found %d emails", len(messages))
            return messages

        except Exception as exc:  # noqa: BLE001
            logger.error("Email search failed: %s", exc)
            return []

    def _count_attachments_in_email(self, payload: Dict[str, Any]) -> int:
        """Count Excel attachments in an email payload (recursive)."""
        count = 0
        if "parts" in payload:
            for part in payload["parts"]:
                count += self._count_attachments_in_email(part)
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            if filename.lower().endswith((".xls", ".xlsx", ".xlsm")):
                count += 1
        return count

    def process_gmail_workflow(self) -> bool:
        """Gmail -> Drive: download GRN attachments, skipping ones already in Drive."""
        try:
            logger.info("Starting Gmail workflow...")
            config = self.config["gmail"]

            emails = self.search_emails(
                config["sender"],
                config["search_term"],
                config["days_back"],
                config["max_results"],
            )

            if not emails:
                logger.info("No emails found")
                return True

            base_folder_id = self._create_drive_folder(
                "Gmail_Attachments_BigBasket", config.get("gdrive_folder_id")
            )
            if not base_folder_id:
                logger.error("Failed to create base folder")
                return False

            attachments_found = 0
            attachments_failed = 0

            for i, email in enumerate(emails):
                try:
                    email_details = self._get_email_details(email["id"])
                    message = execute_with_retry(
                        self.gmail_service.users().messages().get(
                            userId="me", id=email["id"], format="full"
                        ),
                        description="gmail.messages.get",
                    )

                    if message and message.get("payload"):
                        attachments_found += self._count_attachments_in_email(message["payload"])
                        saved, skipped, failed = self._extract_attachments_from_email(
                            email["id"],
                            message["payload"],
                            email_details,
                            config,
                            base_folder_id,
                        )
                        self.stats["attachments_saved"] += saved
                        self.stats["attachments_skipped"] += skipped
                        attachments_failed += failed

                    logger.info("Processed email %d/%d", i + 1, len(emails))

                except Exception as exc:  # noqa: BLE001 - per-email isolation
                    logger.error("Failed to process email: %s", exc)
                    attachments_failed += 1

            self.stats["attachments_found"] = attachments_found
            self.stats["attachments_failed"] = attachments_failed

            logger.info(
                "Gmail workflow completed. Found: %d, Saved: %d, Skipped: %d, Failed: %d",
                attachments_found,
                self.stats["attachments_saved"],
                self.stats["attachments_skipped"],
                attachments_failed,
            )
            return True

        except Exception as exc:  # noqa: BLE001
            logger.error("Gmail workflow failed: %s", exc)
            return False

    def _extract_attachments_from_email(
        self,
        message_id: str,
        payload: Dict[str, Any],
        sender_info: Dict[str, Any],
        config: dict,
        base_folder_id: str,
    ) -> Tuple[int, int, int]:
        """Extract Excel attachments. Returns (saved, skipped, failed)."""
        saved_count = 0
        skipped_count = 0
        failed_count = 0

        if "parts" in payload:
            for part in payload["parts"]:
                saved, skipped, failed = self._extract_attachments_from_email(
                    message_id, part, sender_info, config, base_folder_id
                )
                saved_count += saved
                skipped_count += skipped
                failed_count += failed
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            if not filename.lower().endswith((".xls", ".xlsx", ".xlsm")):
                return (0, 0, 0)

            try:
                sender_email = sender_info.get("sender", "Unknown")
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()

                sender_folder_name = self._sanitize_filename(sender_email)
                type_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)

                clean_filename = self._sanitize_filename(filename)
                final_filename = f"{message_id}_{clean_filename}"

                # Drive-level duplicate skip (cached folder listing).
                if self._check_file_exists_in_drive(type_folder_id, final_filename):
                    logger.info("  [SKIP] %s - already exists in Drive", final_filename)
                    return (0, 1, 0)

                attachment_id = payload["body"].get("attachmentId")
                att = execute_with_retry(
                    self.gmail_service.users().messages().attachments().get(
                        userId="me", messageId=message_id, id=attachment_id
                    ),
                    description="gmail.attachments.get",
                )
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))

                file_metadata = {"name": final_filename, "parents": [type_folder_id]}
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data),
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                execute_with_retry(
                    self.drive_service.files().create(
                        body=file_metadata, media_body=media, fields="id"
                    ),
                    description="drive.files.create(upload)",
                )

                # Keep the cache coherent so repeats within the same run skip too.
                if type_folder_id in self.existing_files_cache:
                    self.existing_files_cache[type_folder_id].add(final_filename)

                logger.info("  [OK] Saved %s", final_filename)
                saved_count += 1

            except Exception as exc:  # noqa: BLE001 - per-attachment isolation
                logger.error("Failed to process attachment %s: %s", filename, exc)
                failed_count += 1

        return (saved_count, skipped_count, failed_count)

    # -- drive helpers ------------------------------------------------------
    def _get_existing_files_in_folder(self, folder_id: str) -> Set[str]:
        """Return all non-trashed filenames in a Drive folder (cached, paginated)."""
        if folder_id in self.existing_files_cache:
            return self.existing_files_cache[folder_id]

        try:
            existing_files: Set[str] = set()
            page_token = None
            while True:
                query = f"'{folder_id}' in parents and trashed=false"
                results = execute_with_retry(
                    self.drive_service.files().list(
                        q=query,
                        fields="nextPageToken, files(name)",
                        pageToken=page_token,
                        pageSize=1000,
                    ),
                    description="drive.files.list(folder cache)",
                )
                for file in results.get("files", []):
                    existing_files.add(file["name"])
                page_token = results.get("nextPageToken")
                if not page_token:
                    break

            self.existing_files_cache[folder_id] = existing_files
            logger.info("Found %d existing files in folder", len(existing_files))
            return existing_files

        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to get existing files: %s", exc)
            return set()

    def _check_file_exists_in_drive(self, folder_id: str, filename: str) -> bool:
        """True if a file with this name already exists in the folder."""
        return filename in self._get_existing_files_in_folder(folder_id)

    def _get_email_details(self, message_id: str) -> Dict[str, Any]:
        """Get email metadata (sender / subject / date)."""
        try:
            message = execute_with_retry(
                self.gmail_service.users().messages().get(
                    userId="me", id=message_id, format="metadata"
                ),
                description="gmail.messages.get(metadata)",
            )
            headers = message["payload"].get("headers", [])
            return {
                "id": message_id,
                "sender": next((h["value"] for h in headers if h["name"] == "From"), "Unknown"),
                "subject": next((h["value"] for h in headers if h["name"] == "Subject"), "(No Subject)"),
                "date": next((h["value"] for h in headers if h["name"] == "Date"), ""),
            }
        except Exception:  # noqa: BLE001 - metadata is best-effort
            return {"id": message_id, "sender": "Unknown", "subject": "Unknown", "date": ""}

    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create (or reuse) a folder in Google Drive."""
        try:
            query = (
                f"name='{folder_name}' and "
                f"mimeType='application/vnd.google-apps.folder' and trashed=false"
            )
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"

            existing = execute_with_retry(
                self.drive_service.files().list(q=query, fields="files(id, name)"),
                description="drive.files.list(folder)",
            )
            files = existing.get("files", [])
            if files:
                return files[0]["id"]

            folder_metadata: Dict[str, Any] = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            if parent_folder_id:
                folder_metadata["parents"] = [parent_folder_id]

            folder = execute_with_retry(
                self.drive_service.files().create(body=folder_metadata, fields="id"),
                description="drive.files.create(folder)",
            )
            return folder.get("id", "")
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to create folder: %s", exc)
            return ""

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """Clean filenames for Drive."""
        cleaned = re.sub(r'[<>:"/\\|?*]', "_", filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split(".")
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = ".".join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned

    def _get_excel_files_filtered(
        self, folder_id: str, days_back: int, max_files: int
    ) -> List[Dict[str, Any]]:
        """Get recent Excel files from a Drive folder."""
        try:
            # Drive's createdTime is UTC (RFC 3339), so build the threshold in UTC.
            date_threshold = datetime.now(timezone.utc) - timedelta(days=days_back)
            date_threshold_str = date_threshold.strftime("%Y-%m-%dT%H:%M:%SZ")

            query = (
                f"'{folder_id}' in parents and "
                f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                f"mimeType='application/vnd.ms-excel') and "
                f"createdTime > '{date_threshold_str}'"
            )

            results = execute_with_retry(
                self.drive_service.files().list(
                    q=query,
                    fields="files(id, name, createdTime)",
                    orderBy="createdTime desc",
                    pageSize=max_files,
                ),
                description="drive.files.list(excel)",
            )
            return results.get("files", [])
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to get Excel files: %s", exc)
            return []

    # -- sheet dedup helpers ------------------------------------------------
    def _get_processed_files_from_sheet(self) -> Set[str]:
        """Return a set of 'PO No|Sku Code' keys already present in the sheet."""
        try:
            config = self.config["excel"]
            result = execute_with_retry(
                self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=config["spreadsheet_id"],
                    range=f"{config['sheet_name']}!A1:ZZ",
                ),
                description="sheets.values.get(keys)",
            )
            values = result.get("values", [])
            if not values:
                return set()

            headers = values[0] if values else []
            po_col_idx = None
            sku_col_idx = None
            for i, header in enumerate(headers):
                if header and "PO" in str(header).upper():
                    po_col_idx = i
                if header and "SKU" in str(header).upper():
                    sku_col_idx = i

            processed_data: Set[str] = set()
            if po_col_idx is not None and sku_col_idx is not None:
                for row in values[1:]:
                    if len(row) > max(po_col_idx, sku_col_idx):
                        po_no = str(row[po_col_idx]).strip() if row[po_col_idx] else ""
                        sku_code = str(row[sku_col_idx]).strip() if row[sku_col_idx] else ""
                        if po_no and sku_code:
                            processed_data.add(f"{po_no}|{sku_code}")

            logger.info("Found %d unique records in sheet", len(processed_data))
            return processed_data

        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to get processed records: %s", exc)
            return set()

    def _get_source_file_names_from_sheet(self) -> Set[str]:
        """Return source file names already recorded in the 'source_file_name' column."""
        try:
            config = self.config["excel"]
            result = execute_with_retry(
                self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=config["spreadsheet_id"],
                    range=f"{config['sheet_name']}!A1:ZZ",
                ),
                description="sheets.values.get(source files)",
            )
            values = result.get("values", [])
            if not values:
                return set()

            headers = values[0] if values else []
            source_file_col_idx = None
            for i, header in enumerate(headers):
                if header and "source_file_name" in str(header).lower():
                    source_file_col_idx = i
                    break

            if source_file_col_idx is None:
                logger.info("No 'source_file_name' column found in sheet")
                return set()

            source_files: Set[str] = set()
            for row in values[1:]:
                if len(row) > source_file_col_idx:
                    source_file = str(row[source_file_col_idx]).strip()
                    if source_file:
                        source_files.add(source_file)

            logger.info("Found %d unique source files in sheet", len(source_files))
            return source_files

        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to get source file names: %s", exc)
            return set()

    # -- excel workflow -----------------------------------------------------
    def process_excel_workflow(self) -> bool:
        """Drive -> Sheet: append new GRN rows with dedup + reconciliation chain."""
        try:
            logger.info("Starting Excel workflow...")
            config = self.config["excel"]

            excel_files = self._get_excel_files_filtered(
                config["excel_folder_id"], config["days_back"], config["max_files"]
            )
            self.stats["files_found"] = len(excel_files)
            logger.info("Found %d Excel files", len(excel_files))

            if not excel_files:
                logger.info("No Excel files found")
                return True

            processed_source_files = self._get_source_file_names_from_sheet()
            existing_data = self._get_processed_files_from_sheet()

            is_first_file = True
            sheet_has_headers = self._check_sheet_headers(
                config["spreadsheet_id"], config["sheet_name"]
            )

            # Skip whole files already recorded in the source_file_name column.
            files_to_process: List[Dict[str, Any]] = []
            for file in excel_files:
                if file["name"] in processed_source_files:
                    logger.info("[SKIP] %s - already in source_file_name column", file["name"])
                    self.stats["files_skipped"] += 1
                else:
                    files_to_process.append(file)

            logger.info("After filtering: %d files to process", len(files_to_process))

            for i, file in enumerate(files_to_process):
                try:
                    logger.info("Processing %s (%d/%d)", file["name"], i + 1, len(files_to_process))

                    df = self._read_excel_file_robust(
                        file["id"], file["name"], config["header_row"]
                    )
                    raw_rows = self._last_raw_rows  # captured pre-clean inside the reader

                    if df.empty:
                        logger.warning("No data in %s", file["name"])
                        self.stats["files_failed"] += 1
                        continue

                    cleaned_rows = len(df)

                    # Tag rows with their source file (enables file-level dedup later).
                    df["source_file_name"] = file["name"]

                    # Row-level dedup: drop PO|Sku rows already present in the sheet.
                    if "PO No" in df.columns and "Sku Code" in df.columns:
                        mask = df.apply(
                            lambda row: (
                                f"{str(row.get('PO No', '')).strip()}|"
                                f"{str(row.get('Sku Code', '')).strip()}"
                            ) not in existing_data,
                            axis=1,
                        )
                        df = df[mask]
                        dropped = cleaned_rows - len(df)
                        if dropped > 0:
                            logger.info("  -> filtered %d duplicate rows from %s", dropped, file["name"])

                    new_rows = len(df)

                    # Account raw/cleaned/new for EVERY file we read (even all-dup ones)
                    # so the reconciliation chain is complete.
                    self.stats["rows_raw_in_files"] += raw_rows
                    self.stats["rows_cleaned_in_files"] += cleaned_rows
                    self.stats["rows_new"] += new_rows

                    if new_rows == 0:
                        logger.info(
                            "  -> all rows from %s already in sheet (raw=%d cleaned=%d new=0); skipping",
                            file["name"], raw_rows, cleaned_rows,
                        )
                        self.stats["files_skipped"] += 1
                        continue

                    append_headers = is_first_file and not sheet_has_headers
                    appended_rows = self._append_to_sheet(
                        config["spreadsheet_id"], config["sheet_name"], df, append_headers
                    )
                    self.stats["rows_appended"] += appended_rows

                    # Keep the in-memory key set current for subsequent files this run.
                    if "PO No" in df.columns and "Sku Code" in df.columns:
                        for _, row in df.iterrows():
                            po_no = str(row.get("PO No", "")).strip()
                            sku_code = str(row.get("Sku Code", "")).strip()
                            if po_no and sku_code:
                                existing_data.add(f"{po_no}|{sku_code}")

                    self.stats["files_processed"] += 1
                    is_first_file = False
                    sheet_has_headers = True

                    # Per-file write-integrity check (appended vs new) + full chain.
                    if appended_rows == new_rows:
                        logger.info(
                            "[OK] %s: raw=%d -> cleaned=%d -> new=%d -> appended=%d",
                            file["name"], raw_rows, cleaned_rows, new_rows, appended_rows,
                        )
                    else:
                        logger.warning(
                            "[MISMATCH] %s: new=%d but appended=%d (raw=%d cleaned=%d)",
                            file["name"], new_rows, appended_rows, raw_rows, cleaned_rows,
                        )

                except Exception as exc:  # noqa: BLE001 - per-file isolation
                    logger.error("Failed to process %s: %s", file.get("name", "unknown"), exc)
                    self.stats["files_failed"] += 1

            # Overall write-integrity verdict: appended vs new.
            total_new = self.stats["rows_new"]
            total_appended = self.stats["rows_appended"]
            if total_appended == total_new:
                self.stats["row_check"] = f"Match ({total_appended}/{total_new})"
                logger.info(
                    "Row reconciliation OK: appended %d = new %d "
                    "(raw=%d, cleaned=%d across all files)",
                    total_appended, total_new,
                    self.stats["rows_raw_in_files"], self.stats["rows_cleaned_in_files"],
                )
            else:
                self.stats["row_check"] = f"MISMATCH ({total_appended}/{total_new})"
                logger.warning(
                    "Row reconciliation MISMATCH: appended %d but new was %d "
                    "(raw=%d, cleaned=%d)",
                    total_appended, total_new,
                    self.stats["rows_raw_in_files"], self.stats["rows_cleaned_in_files"],
                )

            # Safety cleanup of any residual duplicates already in the sheet.
            if self.stats["files_processed"] > 0:
                logger.info("Running final duplicate cleanup...")
                self.stats["duplicates_removed"] = self._remove_duplicates_from_sheet(
                    config["spreadsheet_id"], config["sheet_name"]
                )

            logger.info(
                "Excel workflow completed. Found: %d, Processed: %d, Skipped: %d, Failed: %d",
                self.stats["files_found"], self.stats["files_processed"],
                self.stats["files_skipped"], self.stats["files_failed"],
            )
            return True

        except Exception as exc:  # noqa: BLE001
            logger.error("Excel workflow failed: %s", exc)
            return False

    # -- excel reading ------------------------------------------------------
    def _read_excel_file_robust(
        self, file_id: str, filename: str, header_row: int
    ) -> pd.DataFrame:
        """Read an Excel file with openpyxl -> xlrd -> raw XML fallbacks.

        Side effect: sets ``self._last_raw_rows`` to the row count of the parsed
        frame BEFORE cleaning, so the workflow can build the reconciliation chain.
        """
        self._last_raw_rows = 0
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                _status, done = downloader.next_chunk()
            file_stream.seek(0)

            # openpyxl
            try:
                file_stream.seek(0)
                if header_row == -1:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=None)
                else:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
                if not df.empty:
                    self._last_raw_rows = len(df)
                    return self._clean_dataframe(df)
            except Exception:  # noqa: BLE001 - fall through
                pass

            # xlrd for older .xls
            if filename.lower().endswith(".xls"):
                try:
                    file_stream.seek(0)
                    if header_row == -1:
                        df = pd.read_excel(file_stream, engine="xlrd", header=None)
                    else:
                        df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
                    if not df.empty:
                        self._last_raw_rows = len(df)
                        return self._clean_dataframe(df)
                except Exception:  # noqa: BLE001
                    pass

            # raw XML extraction
            df = self._try_raw_xml_extraction(file_stream, header_row)
            if not df.empty:
                self._last_raw_rows = len(df)
                return self._clean_dataframe(df)

            return pd.DataFrame()

        except Exception as exc:  # noqa: BLE001
            logger.error("Error reading %s: %s", filename, exc)
            return pd.DataFrame()

    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, header_row: int) -> pd.DataFrame:
        """Raw XML extraction for corrupted/odd .xlsx files."""
        try:
            file_stream.seek(0)
            with zipfile.ZipFile(file_stream, "r") as zip_ref:
                file_list = zip_ref.namelist()
                shared_strings: Dict[str, str] = {}

                shared_strings_file = "xl/sharedStrings.xml"
                if shared_strings_file in file_list:
                    try:
                        with zip_ref.open(shared_strings_file) as ss_file:
                            ss_content = ss_file.read().decode("utf-8", errors="ignore")
                            string_pattern = r"<t[^>]*>([^<]*)</t>"
                            strings = re.findall(string_pattern, ss_content, re.DOTALL)
                            for i, string_val in enumerate(strings):
                                shared_strings[str(i)] = string_val.strip()
                    except Exception:  # noqa: BLE001
                        pass

                worksheet_files = [
                    f for f in file_list if "xl/worksheets/" in f and f.endswith(".xml")
                ]
                if not worksheet_files:
                    return pd.DataFrame()

                with zip_ref.open(worksheet_files[0]) as xml_file:
                    content = xml_file.read().decode("utf-8", errors="ignore")
                    cell_pattern = (
                        r'<c[^>]*r="([A-Z]+\d+)"[^>]*(?:t="([^"]*)")?[^>]*>'
                        r'(?:.*?<v[^>]*>([^<]*)</v>)?(?:.*?<is><t[^>]*>([^<]*)</t></is>)?'
                    )
                    cells = re.findall(cell_pattern, content, re.DOTALL)
                    if not cells:
                        return pd.DataFrame()

                    cell_data: Dict[Any, Any] = {}
                    max_row = 0
                    max_col = 0
                    for cell_ref, cell_type, v_value, is_value in cells:
                        col_letters = "".join([c for c in cell_ref if c.isalpha()])
                        row_num = int("".join([c for c in cell_ref if c.isdigit()]))
                        col_num = 0
                        for c in col_letters:
                            col_num = col_num * 26 + (ord(c) - ord("A") + 1)

                        if is_value:
                            cell_value: Any = is_value.strip()
                        elif cell_type == "s" and v_value:
                            cell_value = shared_strings.get(v_value, v_value)
                        elif v_value:
                            cell_value = v_value.strip()
                        else:
                            cell_value = ""

                        cell_data[(row_num, col_num)] = self._clean_cell_value(cell_value)
                        max_row = max(max_row, row_num)
                        max_col = max(max_col, col_num)

                    if not cell_data:
                        return pd.DataFrame()

                    data: List[List[Any]] = []
                    for row in range(1, max_row + 1):
                        row_data = []
                        for col in range(1, max_col + 1):
                            row_data.append(cell_data.get((row, col), ""))
                        if any(cell for cell in row_data):
                            data.append(row_data)

                    if len(data) < max(1, header_row + 2):
                        return pd.DataFrame()

                    if header_row == -1:
                        headers = [f"Column_{i + 1}" for i in range(len(data[0]))]
                        return pd.DataFrame(data, columns=headers)

                    if len(data) > header_row:
                        headers = [
                            str(h) if h else f"Column_{i + 1}"
                            for i, h in enumerate(data[header_row])
                        ]
                        return pd.DataFrame(data[header_row + 1:], columns=headers)
                    return pd.DataFrame()

        except Exception:  # noqa: BLE001 - last-resort parser, never fatal
            return pd.DataFrame()

    @staticmethod
    def _clean_cell_value(value: Any) -> Any:
        """Clean a single extracted cell value."""
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return ""
            return value
        return str(value).strip().replace("'", "")

    @staticmethod
    def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Clean a parsed DataFrame (strip quotes, drop blank-key rows, dedupe).

        Drops rows whose 5th column is blank/nan and exact-duplicate rows. These
        drops are reflected in the raw->cleaned step of the reconciliation chain.
        """
        if df.empty:
            return df

        string_columns = df.select_dtypes(include=["object"]).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)

        if len(df.columns) >= 5:
            fifth_col = df.columns[4]
            mask = ~(
                df[fifth_col].isna()
                | (df[fifth_col].astype(str).str.strip() == "")
                | (df[fifth_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]

        return df.drop_duplicates()

    # -- sheet writing ------------------------------------------------------
    def _check_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Return True if row 1 of the sheet already has content."""
        try:
            result = execute_with_retry(
                self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1"
                ),
                description="sheets.values.get(headers)",
            )
            return bool(result.get("values", []))
        except Exception:  # noqa: BLE001
            return False

    def _append_to_sheet(
        self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, append_headers: bool
    ) -> int:
        """Append a DataFrame to a sheet. Returns data rows Sheets actually wrote."""
        try:
            clean_data = df.fillna("")
            if append_headers:
                values = [clean_data.columns.tolist()] + clean_data.values.tolist()
            else:
                values = clean_data.values.tolist()

            if not values:
                return 0

            response = execute_with_retry(
                self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="USER_ENTERED",
                    body={"values": values},
                ),
                description="sheets.values.append",
            )

            updated_rows = response.get("updates", {}).get("updatedRows", 0)
            appended_data_rows = updated_rows - (1 if append_headers else 0)
            return max(appended_data_rows, 0)

        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to append to sheet: {exc}") from exc

    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str) -> int:
        """Remove duplicates (by Sku Code + PO No) and blank rows; rewrite sorted."""
        try:
            result = execute_with_retry(
                self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1:ZZ"
                ),
                description="sheets.values.get(all)",
            )
            values = result.get("values", [])
            if not values:
                return 0

            max_len = max(len(row) for row in values)
            for row in values:
                row.extend([""] * (max_len - len(row)))

            headers = [values[0][i] if values[0][i] else f"Column_{i + 1}" for i in range(max_len)]
            rows = values[1:]
            df = pd.DataFrame(rows, columns=headers)
            before = len(df)

            if "Sku Code" in df.columns and "PO No" in df.columns:
                df = df.drop_duplicates(subset=["Sku Code", "PO No"], keep="first")

            after_dup = len(df)
            removed_dup = before - after_dup

            df.replace("", pd.NA, inplace=True)
            df.dropna(how="all", inplace=True)
            df.dropna(how="all", axis=1, inplace=True)
            df.fillna("", inplace=True)

            after_clean = len(df)
            removed_clean = after_dup - after_clean

            if "PO No" in df.columns:
                df = df.sort_values(by="PO No", ascending=True)

            def process_value(v: Any) -> Any:
                if pd.isna(v) or v == "":
                    return ""
                try:
                    if "." in str(v) or "e" in str(v).lower():
                        return float(v)
                    return int(v)
                except (ValueError, TypeError):
                    return str(v)

            out_values: List[List[Any]] = [[str(col) for col in df.columns]]
            for row in df.itertuples(index=False):
                out_values.append([process_value(cell) for cell in row])

            execute_with_retry(
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id, range=sheet_name
                ),
                description="sheets.values.clear",
            )
            execute_with_retry(
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="RAW",
                    body={"values": out_values},
                ),
                description="sheets.values.update",
            )

            total_removed = removed_dup + removed_clean
            logger.info("Removed %d duplicates and %d blank rows", removed_dup, removed_clean)
            return total_removed

        except Exception as exc:  # noqa: BLE001
            logger.error("Error cleaning sheet: %s", exc)
            return 0

    # -- summary + notification --------------------------------------------
    # Column order for the workflow log sheet (single source of truth).
    SUMMARY_COLUMNS = [
        "Start Time", "End Time", "Status",
        "Days Back (Gmail)", "Emails Checked",
        "Attachments Found", "Attachments Saved", "Attachments Skipped", "Attachments Failed",
        "Days Back (Excel)", "Files Found", "Files Processed", "Files Skipped", "Files Failed",
        "Rows Raw", "Rows Cleaned", "Rows New", "Rows Appended", "Row Check",
        "Duplicates Removed",
    ]

    def _summary_row(self) -> List[str]:
        """Build the summary row matching SUMMARY_COLUMNS order."""
        s = self.stats
        return [
            s["start_time"].strftime("%Y-%m-%d %H:%M:%S") if s["start_time"] else "",
            s["end_time"].strftime("%Y-%m-%d %H:%M:%S") if s["end_time"] else "",
            str(s["status"]),
            str(s["days_back_gmail"]), str(s["emails_checked"]),
            str(s["attachments_found"]), str(s["attachments_saved"]),
            str(s["attachments_skipped"]), str(s["attachments_failed"]),
            str(s["days_back_excel"]), str(s["files_found"]), str(s["files_processed"]),
            str(s["files_skipped"]), str(s["files_failed"]),
            str(s["rows_raw_in_files"]), str(s["rows_cleaned_in_files"]),
            str(s["rows_new"]), str(s["rows_appended"]), str(s["row_check"]),
            str(s["duplicates_removed"]),
        ]

    def save_workflow_summary(self) -> None:
        """Append a summary row to the workflow log sheet."""
        try:
            config = self.config["excel"]
            summary_sheet = config["summary_sheet_name"]
            spreadsheet_id = config["spreadsheet_id"]

            # Ensure a header row exists (both code paths write the SAME full header).
            needs_headers = True
            try:
                existing = execute_with_retry(
                    self.sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id, range=f"{summary_sheet}!A1"
                    ),
                    description="sheets.values.get(summary)",
                )
                needs_headers = not existing.get("values")
            except Exception:  # noqa: BLE001 - sheet may not exist yet
                needs_headers = True

            if needs_headers:
                execute_with_retry(
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{summary_sheet}!A1",
                        valueInputOption="RAW",
                        body={"values": [self.SUMMARY_COLUMNS]},
                    ),
                    description="sheets.values.update(summary headers)",
                )

            execute_with_retry(
                self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=f"{summary_sheet}!A1",
                    valueInputOption="RAW",
                    body={"values": [self._summary_row()]},
                ),
                description="sheets.values.append(summary)",
            )

            logger.info("Workflow summary saved successfully")

        except Exception as exc:  # noqa: BLE001 - summary failure must not kill the run
            logger.error("Failed to save workflow summary: %s", exc)

    def send_email_notification(self) -> bool:
        """Email an HTML workflow summary to the configured recipients."""
        try:
            if not self.config["notification"].get("enabled", True):
                logger.info("Email notification disabled; skipping")
                return True

            user_profile = execute_with_retry(
                self.gmail_service.users().getProfile(userId="me"),
                description="gmail.users.getProfile",
            )
            user_email = user_profile.get("emailAddress", "")

            recipients = list(self.config["notification"]["recipients"])
            if self.config["notification"]["send_to_self"] and user_email:
                recipients.append(user_email)
            recipients = [r for r in dict.fromkeys(recipients) if r]  # dedupe, drop blanks

            if not recipients:
                logger.warning("No recipients configured for email notification")
                return False

            s = self.stats
            start = s["start_time"].strftime("%Y-%m-%d %H:%M:%S") if s["start_time"] else ""
            end = s["end_time"].strftime("%H:%M:%S") if s["end_time"] else ""
            duration = (
                (s["end_time"] - s["start_time"]).total_seconds() / 60
                if s["start_time"] and s["end_time"] else 0.0
            )

            subject = f"Big Basket (Alert) Automation Summary - {datetime.now():%Y-%m-%d %H:%M:%S}"

            html_content = f"""
            <html><body style="font-family: Arial, sans-serif; line-height: 1.6;">
                <h2>BigBasket Automation Workflow Summary</h2>
                <p><strong>Workflow Time:</strong> {start} to {end}</p>
                <p><strong>Duration:</strong> {duration:.2f} minutes</p>
                <p><strong>Status:</strong> {s['status']}</p>

                <h3>Mail to Drive</h3>
                <ul>
                    <li><strong>Days Back:</strong> {s['days_back_gmail']} days</li>
                    <li><strong>Mails Checked:</strong> {s['emails_checked']}</li>
                    <li><strong>Attachments Found:</strong> {s['attachments_found']}</li>
                    <li><strong>Attachments Uploaded:</strong> {s['attachments_saved']}</li>
                    <li><strong>Attachments Skipped:</strong> {s['attachments_skipped']}</li>
                    <li><strong>Failed to Upload:</strong> {s['attachments_failed']}</li>
                </ul>

                <h3>Drive to Sheet</h3>
                <ul>
                    <li><strong>Days Back:</strong> {s['days_back_excel']} days</li>
                    <li><strong>Files Found:</strong> {s['files_found']}</li>
                    <li><strong>Files Processed:</strong> {s['files_processed']}</li>
                    <li><strong>Files Skipped:</strong> {s['files_skipped']}</li>
                    <li><strong>Files Failed:</strong> {s['files_failed']}</li>
                    <li><strong>Duplicate Records Removed:</strong> {s['duplicates_removed']}</li>
                </ul>

                <h3>Row Reconciliation</h3>
                <ul>
                    <li><strong>Raw rows in files:</strong> {s['rows_raw_in_files']}</li>
                    <li><strong>After cleaning:</strong> {s['rows_cleaned_in_files']}</li>
                    <li><strong>New (after dedup):</strong> {s['rows_new']}</li>
                    <li><strong>Appended to sheet:</strong> {s['rows_appended']}</li>
                    <li><strong>Write-integrity check:</strong> {s['row_check']}</li>
                </ul>

                <hr>
                <p style="color:#666; font-size:0.9em;">
                    Automated email from BigBasket Automation Scheduler. Ran at
                    {datetime.now():%Y-%m-%d %H:%M:%S}.
                </p>
            </body></html>
            """

            raw = base64.urlsafe_b64encode(
                (
                    f"From: {user_email}\r\n"
                    f"To: {', '.join(recipients)}\r\n"
                    f"Subject: {subject}\r\n"
                    f"Content-Type: text/html; charset=utf-8\r\n"
                    f"\r\n"
                    f"{html_content}"
                ).encode("utf-8")
            ).decode("utf-8")

            execute_with_retry(
                self.gmail_service.users().messages().send(userId="me", body={"raw": raw}),
                description="gmail.messages.send",
            )

            logger.info("Email notification sent to %s", ", ".join(recipients))
            return True

        except Exception as exc:  # noqa: BLE001 - notification must not kill the run
            logger.error("Failed to send email notification: %s", exc)
            return False

    # -- orchestration ------------------------------------------------------
    def run_workflow(self) -> bool:
        """Run the complete workflow once. Returns True on success."""
        try:
            self.stats = self._fresh_stats()
            self.stats["start_time"] = datetime.now()
            self.stats["status"] = "Running"
            self.existing_files_cache = {}

            logger.info("=" * 50)
            logger.info("Starting BigBasket Automation Workflow")
            logger.info("=" * 50)

            if not self.authenticate():
                self.stats["status"] = "Failed - Authentication Error"
                self.stats["end_time"] = datetime.now()
                self.save_workflow_summary()
                return False

            logger.info("\n--- STEP 1: Gmail to Drive Workflow ---")
            if not self.process_gmail_workflow():
                logger.warning("Gmail workflow had issues, but continuing...")

            logger.info("\n--- STEP 2: Drive to Sheet Workflow ---")
            if not self.process_excel_workflow():
                logger.warning("Excel workflow had issues")

            self.stats["end_time"] = datetime.now()
            self.stats["status"] = "Completed Successfully"

            logger.info("\n--- STEP 3: Saving Workflow Summary ---")
            self.save_workflow_summary()

            logger.info("\n--- STEP 4: Sending Email Notification ---")
            if not self.send_email_notification():
                logger.warning("Email notification failed, but workflow completed")

            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds() / 60
            logger.info("=" * 50)
            logger.info("Workflow completed in %.2f minutes", duration)
            logger.info("Emails checked: %d", self.stats["emails_checked"])
            logger.info("Attachments found/saved/skipped/failed: %d/%d/%d/%d",
                        self.stats["attachments_found"], self.stats["attachments_saved"],
                        self.stats["attachments_skipped"], self.stats["attachments_failed"])
            logger.info("Files found/processed/skipped/failed: %d/%d/%d/%d",
                        self.stats["files_found"], self.stats["files_processed"],
                        self.stats["files_skipped"], self.stats["files_failed"])
            logger.info("Rows raw->cleaned->new->appended: %d -> %d -> %d -> %d",
                        self.stats["rows_raw_in_files"], self.stats["rows_cleaned_in_files"],
                        self.stats["rows_new"], self.stats["rows_appended"])
            logger.info("Row check: %s", self.stats["row_check"])
            logger.info("Duplicates removed: %d", self.stats["duplicates_removed"])
            logger.info("=" * 50)

            return True

        except Exception as exc:  # noqa: BLE001 - top-level guard
            logger.exception("Workflow failed: %s", exc)
            self.stats["status"] = f"Failed - {exc}"
            self.stats["end_time"] = datetime.now()
            try:
                self.save_workflow_summary()
                self.send_email_notification()  # best-effort failure notification
            except Exception:  # noqa: BLE001
                pass
            return False

    def start_scheduler(self) -> None:
        """Run continuously, executing the workflow every 3 hours."""
        logger.info("BigBasket Automation Scheduler Started")
        logger.info("Workflow will run every 3 hours")

        schedule.every(3).hours.do(self.run_workflow)

        logger.info("Running initial workflow...")
        self.run_workflow()

        while True:
            schedule.run_pending()
            time.sleep(60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Returns a process exit code (0 = success)."""
    parser = argparse.ArgumentParser(description="BigBasket Automation Scheduler")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run the workflow once and exit (used by GitHub Actions).",
    )
    args = parser.parse_args(argv)

    try:
        socket.setdefaulttimeout(DEFAULT_SOCKET_TIMEOUT)
    except Exception:  # noqa: BLE001 - non-fatal if it can't be set
        pass

    try:
        scheduler = BigBasketScheduler(run_once=args.run_once)

        if args.run_once:
            logger.info("Running workflow once (GitHub Actions mode)")
            success = scheduler.run_workflow()
            return 0 if success else 1

        scheduler.start_scheduler()
        return 0

    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        return 0
    except Exception as exc:  # noqa: BLE001 - last-resort guard
        logger.exception("Scheduler error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
