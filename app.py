"""
BigBasket Automation Scheduler with Enhanced Duplicate Prevention
Prevents duplicate files in Drive and duplicate data in Sheets
Added source_file_name column and filtering
Added email notification with workflow summary
"""

import schedule
import time
import json
import os
import logging
import sys  # Added for CLI arguments
import argparse  # Added for CLI arguments
from datetime import datetime
from typing import Dict, Any, List, Set
import pandas as pd
import io
import re
import zipfile

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# Configure logging with UTF-8 encoding for Windows compatibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bb_automation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class BigBasketScheduler:
    def __init__(self, run_once=False):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        self.run_once = run_once  # Added for GitHub Actions
        
        # API scopes - added Gmail send scope
        self.gmail_scopes = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.send'
        ]
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Configuration
        self.config = {
            'gmail': {
                'sender': 'alerts@bigbasket.com',
                'search_term': 'GRN',
                'days_back': 7,
                'max_results': 1000,
                'gdrive_folder_id': '1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt'
            },
            'excel': {
                'excel_folder_id': '1dQnXXscJsHnl9Ue-zcDazGLQuXAxIUQG',
                'spreadsheet_id': '170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM',
                'sheet_name': 'bbalertgrn',
                'summary_sheet_name': 'alert_workflow_log',
                'header_row': 0,
                'days_back': 7,
                'max_files': 1000
            },
            'notification': {
                'recipients': ['keyur@thebakersdozen.in'],
                'send_to_self': True
            }
        }
        
        # Workflow statistics - enhanced with email-specific stats
        self.stats = {
            'start_time': None,
            'end_time': None,
            'days_back_gmail': self.config['gmail']['days_back'],
            'days_back_excel': self.config['excel']['days_back'],
            'emails_checked': 0,
            'attachments_found': 0,
            'attachments_saved': 0,
            'attachments_skipped': 0,
            'attachments_failed': 0,
            'files_found': 0,
            'files_processed': 0,
            'files_skipped': 0,
            'files_failed': 0,
            'duplicates_removed': 0,
            'status': 'Not Started'
        }
        
        # Cache for existing files (prevents repeated API calls)
        self.existing_files_cache = {}
    
    def authenticate(self):
        """Authenticate with Google APIs using local credentials"""
        try:
            creds = None
            token_file = 'token.json'
            credentials_file = 'credentials.json'
            
            # Check if token.json exists
            if os.path.exists(token_file):
                creds = Credentials.from_authorized_user_file(
                    token_file, 
                    self.gmail_scopes + self.drive_scopes + self.sheets_scopes
                )
            
            # If credentials are not valid, authenticate
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not os.path.exists(credentials_file):
                        logging.error(f"credentials.json not found. Please download it from Google Cloud Console.")
                        return False
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        credentials_file,
                        self.gmail_scopes + self.drive_scopes + self.sheets_scopes
                    )
                    creds = flow.run_local_server(port=0)
                
                # Save credentials for next run
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            logging.info("Authentication successful!")
            return True
            
        except Exception as e:
            logging.error(f"Authentication failed: {str(e)}")
            return False
    
    def search_emails(self, sender: str, search_term: str, days_back: int, max_results: int):
        """Search for emails with attachments"""
        try:
            from datetime import timedelta
            
            query_parts = ["has:attachment"]
            if sender:
                query_parts.append(f'from:"{sender}"')
            if search_term:
                query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            query = " ".join(query_parts)
            
            # FIX: Ensure max_results is at least 1
            max_results = max(max_results, 1) if max_results else 1
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.stats['emails_checked'] = len(messages)
            logging.info(f"Found {len(messages)} emails")
            return messages
            
        except Exception as e:
            logging.error(f"Email search failed: {str(e)}")
            return []
    
    def _get_existing_files_in_folder(self, folder_id: str) -> Set[str]:
        """
        Get all existing filenames in a Drive folder (cached)
        Returns a set of filenames for quick lookup
        """
        if folder_id in self.existing_files_cache:
            return self.existing_files_cache[folder_id]
        
        try:
            existing_files = set()
            page_token = None
            
            while True:
                query = f"'{folder_id}' in parents and trashed=false"
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(name)",
                    pageToken=page_token,
                    pageSize=1000
                ).execute()
                
                files = results.get('files', [])
                for file in files:
                    existing_files.add(file['name'])
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            self.existing_files_cache[folder_id] = existing_files
            logging.info(f"Found {len(existing_files)} existing files in folder")
            return existing_files
            
        except Exception as e:
            logging.error(f"Failed to get existing files: {str(e)}")
            return set()
    
    def _check_file_exists_in_drive(self, folder_id: str, filename: str) -> bool:
        """
        Check if a file with the given name already exists in the folder
        """
        existing_files = self._get_existing_files_in_folder(folder_id)
        return filename in existing_files
    
    def _get_processed_files_from_sheet(self) -> Set[str]:
        """
        Get list of files that have already been processed (from sheet data)
        This creates a unique identifier from file data to detect duplicates
        """
        try:
            config = self.config['excel']
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=config['spreadsheet_id'],
                range=f"{config['sheet_name']}!A1:ZZ"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            # Create a set of unique identifiers from existing data
            # Using PO No + Sku Code as unique identifier
            processed_data = set()
            
            headers = values[0] if values else []
            
            # Find column indices for key fields
            po_col_idx = None
            sku_col_idx = None
            
            for i, header in enumerate(headers):
                if header and 'PO' in str(header).upper():
                    po_col_idx = i
                if header and 'SKU' in str(header).upper():
                    sku_col_idx = i
            
            if po_col_idx is not None and sku_col_idx is not None:
                for row in values[1:]:  # Skip header
                    if len(row) > max(po_col_idx, sku_col_idx):
                        po_no = str(row[po_col_idx]).strip() if row[po_col_idx] else ''
                        sku_code = str(row[sku_col_idx]).strip() if row[sku_col_idx] else ''
                        
                        if po_no and sku_code:
                            unique_key = f"{po_no}|{sku_code}"
                            processed_data.add(unique_key)
            
            logging.info(f"Found {len(processed_data)} unique records in sheet")
            return processed_data
            
        except Exception as e:
            logging.error(f"Failed to get processed files: {str(e)}")
            return set()
    
    def _get_already_processed_drive_files(self) -> Set[str]:
        """
        Get list of Drive files (by name) that have already been fully processed into the sheet
        Based on: file name exists AND its data (PO|Sku keys) is present in sheet
        """
        try:
            config = self.config['excel']
            processed_data = self._get_processed_files_from_sheet()
            processed_files = set()

            excel_files = self._get_all_excel_files_in_folder(config['excel_folder_id'])

            for file in excel_files:
                filename = file['name']
                file_id = file['id']

                try:
                    df = self._read_excel_file_robust(file_id, filename, self.config['excel']['header_row'])
                    if df.empty:
                        continue

                    if 'PO No' not in df.columns or 'Sku Code' not in df.columns:
                        continue

                    # Check if ANY row from this file exists in the sheet
                    file_data_in_sheet = False
                    for _, row in df.iterrows():
                        po = str(row['PO No']).strip()
                        sku = str(row['Sku Code']).strip()
                        if po and sku and f"{po}|{sku}" in processed_data:
                            file_data_in_sheet = True
                            break

                    if file_data_in_sheet:
                        processed_files.add(filename)
                        logging.info(f"Marked as processed (data exists): {filename}")

                except Exception as e:
                    logging.warning(f"Could not verify file {filename}: {e}")

            return processed_files

        except Exception as e:
            logging.error(f"Error in _get_already_processed_drive_files: {e}")
            return set()
    
    def _get_source_file_names_from_sheet(self) -> Set[str]:
        """
        Get list of source file names that have already been processed
        from the 'source_file_name' column in the sheet
        """
        try:
            config = self.config['excel']
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=config['spreadsheet_id'],
                range=f"{config['sheet_name']}!A1:ZZ"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            headers = values[0] if values else []
            source_file_col_idx = None
            
            # Find column index for 'source_file_name'
            for i, header in enumerate(headers):
                if header and 'source_file_name' in str(header).lower():
                    source_file_col_idx = i
                    break
            
            if source_file_col_idx is None:
                logging.info("No 'source_file_name' column found in sheet")
                return set()
            
            source_files = set()
            for row in values[1:]:  # Skip header
                if len(row) > source_file_col_idx:
                    source_file = str(row[source_file_col_idx]).strip()
                    if source_file:
                        source_files.add(source_file)
            
            logging.info(f"Found {len(source_files)} unique source files in sheet")
            return source_files
            
        except Exception as e:
            logging.error(f"Failed to get source file names from sheet: {str(e)}")
            return set()
    
    def _get_all_excel_files_in_folder(self, folder_id: str) -> List[Dict]:
        """Get all Excel files in a folder (without date filtering)"""
        try:
            if not folder_id or folder_id.strip() == '':
                logging.error("Folder ID is empty or invalid")
                return []
            
            query = (f"'{folder_id}' in parents and "
                    f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                    f"mimeType='application/vnd.ms-excel') and trashed=false")
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, createdTime)",
                orderBy='createdTime desc'
            ).execute()
            
            files = results.get('files', [])
            logging.info(f"Found {len(files)} Excel files in folder {folder_id}")
            return files
        except Exception as e:
            logging.error(f"Failed to get Excel files from folder {folder_id}: {str(e)}")
            return []
    
    def process_gmail_workflow(self):
        """Process Gmail attachment download workflow with duplicate checking"""
        try:
            logging.info("Starting Gmail workflow...")
            config = self.config['gmail']
            
            # Search emails
            emails = self.search_emails(
                config['sender'],
                config['search_term'],
                config['days_back'],
                config['max_results']
            )
            
            if not emails:
                logging.info("No emails found")
                return True
            
            # Create base folder
            base_folder_id = self._create_drive_folder(
                "Gmail_Attachments_BigBasket",
                config.get('gdrive_folder_id')
            )
            
            if not base_folder_id:
                logging.error("Failed to create base folder")
                return False
            
            # Track attachment stats
            attachments_found = 0
            attachments_failed = 0
            
            # Process each email
            for i, email in enumerate(emails):
                try:
                    email_details = self._get_email_details(email['id'])
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if message and message.get('payload'):
                        # Count attachments found in this email
                        attachments_count = self._count_attachments_in_email(message['payload'])
                        attachments_found += attachments_count
                        
                        saved, skipped, failed = self._extract_attachments_from_email(
                            email['id'],
                            message['payload'],
                            email_details,
                            config,
                            base_folder_id
                        )
                        self.stats['attachments_saved'] += saved
                        self.stats['attachments_skipped'] += skipped
                        attachments_failed += failed
                    
                    logging.info(f"Processed email {i+1}/{len(emails)}")
                    
                except Exception as e:
                    logging.error(f"Failed to process email: {str(e)}")
                    attachments_failed += 1
            
            # Update stats
            self.stats['attachments_found'] = attachments_found
            self.stats['attachments_failed'] = attachments_failed
            
            logging.info(f"Gmail workflow completed. Found: {attachments_found}, Saved: {self.stats['attachments_saved']}, Skipped: {self.stats['attachments_skipped']}, Failed: {attachments_failed}")
            return True
            
        except Exception as e:
            logging.error(f"Gmail workflow failed: {str(e)}")
            return False
    
    def _count_attachments_in_email(self, payload: Dict) -> int:
        """Count Excel attachments in email payload"""
        count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                count += self._count_attachments_in_email(part)
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            if filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                count += 1
        
        return count
    
    def process_excel_workflow(self):
        """Process Excel GRN workflow with duplicate checking"""
        try:
            logging.info("Starting Excel workflow...")
            config = self.config['excel']
            
            # Get Excel files
            excel_files = self._get_excel_files_filtered(
                config['excel_folder_id'],
                config['days_back'],
                config['max_files']
            )
            
            self.stats['files_found'] = len(excel_files)
            logging.info(f"Found {len(excel_files)} Excel files")
            
            if not excel_files:
                logging.info("No Excel files found")
                return True
            
            # Get already processed files from source_file_name column
            processed_source_files = self._get_source_file_names_from_sheet()
            
            # Get existing data from sheet for row-level duplicate checking
            existing_data = self._get_processed_files_from_sheet()
            
            is_first_file = True
            sheet_has_headers = self._check_sheet_headers(
                config['spreadsheet_id'],
                config['sheet_name']
            )
            
            # Filter out files that are already in source_file_name column
            files_to_process = []
            for file in excel_files:
                if file['name'] in processed_source_files:
                    logging.info(f"[SKIP] Skipping {file['name']} - already in source_file_name column")
                    self.stats['files_skipped'] += 1
                else:
                    files_to_process.append(file)
            
            logging.info(f"After filtering: {len(files_to_process)} files to process")
            
            # Process each filtered file
            for i, file in enumerate(files_to_process):
                try:
                    logging.info(f"Processing {file['name']} ({i+1}/{len(files_to_process)})")
                    
                    df = self._read_excel_file_robust(
                        file['id'],
                        file['name'],
                        config['header_row']
                    )
                    
                    if df.empty:
                        logging.warning(f"No data in {file['name']}")
                        self.stats['files_failed'] += 1
                        continue
                    
                    # Add source_file_name column
                    df['source_file_name'] = file['name']
                    
                    # Filter out rows that already exist in sheet (PO No + Sku Code)
                    if 'PO No' in df.columns and 'Sku Code' in df.columns:
                        original_rows = len(df)
                        
                        # Create mask for rows NOT in existing data
                        mask = df.apply(
                            lambda row: f"{str(row.get('PO No', '')).strip()}|{str(row.get('Sku Code', '')).strip()}" not in existing_data,
                            axis=1
                        )
                        df = df[mask]
                        
                        new_rows = len(df)
                        skipped_rows = original_rows - new_rows
                        
                        if skipped_rows > 0:
                            logging.info(f"  ↳ Filtered out {skipped_rows} duplicate rows from {file['name']}")
                        
                        if df.empty:
                            logging.info(f"  ↳ All data from {file['name']} already exists in sheet - skipping")
                            self.stats['files_skipped'] += 1
                            continue
                    
                    append_headers = is_first_file and not sheet_has_headers
                    self._append_to_sheet(
                        config['spreadsheet_id'],
                        config['sheet_name'],
                        df,
                        append_headers
                    )
                    
                    # Update existing data cache with newly added data
                    if 'PO No' in df.columns and 'Sku Code' in df.columns:
                        for _, row in df.iterrows():
                            po_no = str(row.get('PO No', '')).strip()
                            sku_code = str(row.get('Sku Code', '')).strip()
                            if po_no and sku_code:
                                existing_data.add(f"{po_no}|{sku_code}")
                    
                    self.stats['files_processed'] += 1
                    is_first_file = False
                    sheet_has_headers = True
                    
                    logging.info(f"[OK] Appended {len(df)} new rows from {file['name']}")
                    
                except Exception as e:
                    logging.error(f"Failed to process {file.get('name', 'unknown')}: {str(e)}")
                    self.stats['files_failed'] += 1
            
            # Remove any remaining duplicates (safety cleanup)
            if self.stats['files_processed'] > 0:
                logging.info("Running final duplicate cleanup...")
                duplicates = self._remove_duplicates_from_sheet(
                    config['spreadsheet_id'],
                    config['sheet_name']
                )
                self.stats['duplicates_removed'] = duplicates
            
            logging.info(f"Excel workflow completed. Found: {self.stats['files_found']}, Processed: {self.stats['files_processed']}, Skipped: {self.stats['files_skipped']}, Failed: {self.stats['files_failed']}")
            return True
            
        except Exception as e:
            logging.error(f"Excel workflow failed: {str(e)}")
            return False
    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            return {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
        except:
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: str = None) -> str:
        """Create folder in Google Drive"""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            # Invalidate cache for parent folder
            if parent_folder_id and parent_folder_id in self.existing_files_cache:
                del self.existing_files_cache[parent_folder_id]
            
            return folder.get('id')
        except Exception as e:
            logging.error(f"Failed to create folder: {str(e)}")
            return ""
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean filenames"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, 
                                       sender_info: Dict, config: dict, base_folder_id: str) -> tuple:
        """
        Extract Excel attachments with duplicate checking
        Returns (saved_count, skipped_count, failed_count)
        """
        import base64
        
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
            
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                return (0, 0, 0)
            
            try:
                # Get sender folder
                sender_email = sender_info.get('sender', 'Unknown')
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                
                sender_folder_name = self._sanitize_filename(sender_email)
                type_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                
                # Create final filename
                clean_filename = self._sanitize_filename(filename)
                final_filename = f"{message_id}_{clean_filename}"
                
                # CHECK IF FILE ALREADY EXISTS IN DRIVE
                if self._check_file_exists_in_drive(type_folder_id, final_filename):
                    logging.info(f"  [SKIP] Skipping {final_filename} - already exists in Drive")
                    return (0, 1, 0)
                
                # Download and upload attachment
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                file_metadata = {
                    'name': final_filename,
                    'parents': [type_folder_id]
                }
                
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data),
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                # Update cache
                if type_folder_id in self.existing_files_cache:
                    self.existing_files_cache[type_folder_id].add(final_filename)
                
                logging.info(f"  [OK] Saved {final_filename}")
                saved_count += 1
                
            except Exception as e:
                logging.error(f"Failed to process attachment {filename}: {str(e)}")
                failed_count += 1
        
        return (saved_count, skipped_count, failed_count)
    
    def _get_excel_files_filtered(self, folder_id: str, days_back: int, max_files: int):
        """Get Excel files from Drive"""
        try:
            if not folder_id or folder_id.strip() == '':
                logging.error("Folder ID is empty or invalid")
                return []
            
            from datetime import timedelta
            
            date_threshold = datetime.now() - timedelta(days=days_back)
            date_threshold_str = date_threshold.strftime('%Y-%m-%dT%H:%M:%S')
            
            query = (f"'{folder_id}' in parents and "
                    f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                    f"mimeType='application/vnd.ms-excel') and "
                    f"createdTime > '{date_threshold_str}' and trashed=false")
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, createdTime)",
                orderBy='createdTime desc',
                pageSize=max_files
            ).execute()
            
            files = results.get('files', [])
            logging.info(f"Found {len(files)} Excel files in folder {folder_id}")
            return files
        except Exception as e:
            logging.error(f"Failed to get Excel files from folder {folder_id}: {str(e)}")
            
            # Try to list available folders for debugging
            try:
                folders = self.drive_service.files().list(
                    q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                    fields="files(id, name)",
                    pageSize=10
                ).execute()
                
                folder_list = folders.get('files', [])
                logging.info(f"Available folders: {[(f['name'], f['id'][:10] + '...') for f in folder_list]}")
            except Exception as debug_e:
                logging.error(f"Could not list folders for debugging: {debug_e}")
            
            return []
    
    def _read_excel_file_robust(self, file_id: str, filename: str, header_row: int) -> pd.DataFrame:
        """Read Excel file with fallback strategies"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_stream.seek(0)
            
            # Try openpyxl
            try:
                file_stream.seek(0)
                if header_row == -1:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=None)
                else:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
                if not df.empty:
                    return self._clean_dataframe(df)
            except:
                pass
            
            # Try xlrd for older files
            if filename.lower().endswith('.xls'):
                try:
                    file_stream.seek(0)
                    if header_row == -1:
                        df = pd.read_excel(file_stream, engine="xlrd", header=None)
                    else:
                        df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
                    if not df.empty:
                        return self._clean_dataframe(df)
                except:
                    pass
            
            # Try raw XML extraction
            df = self._try_raw_xml_extraction(file_stream, header_row)
            if not df.empty:
                return self._clean_dataframe(df)
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error reading {filename}: {str(e)}")
            return pd.DataFrame()
    
    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, header_row: int) -> pd.DataFrame:
        """Raw XML extraction for corrupted files"""
        try:
            file_stream.seek(0)
            with zipfile.ZipFile(file_stream, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                shared_strings = {}
                
                shared_strings_file = 'xl/sharedStrings.xml'
                if shared_strings_file in file_list:
                    try:
                        with zip_ref.open(shared_strings_file) as ss_file:
                            ss_content = ss_file.read().decode('utf-8', errors='ignore')
                            string_pattern = r'<t[^>]*>([^<]*)</t>'
                            strings = re.findall(string_pattern, ss_content, re.DOTALL)
                            for i, string_val in enumerate(strings):
                                shared_strings[str(i)] = string_val.strip()
                    except:
                        pass
                
                worksheet_files = [f for f in file_list if 'xl/worksheets/' in f and f.endswith('.xml')]
                if not worksheet_files:
                    return pd.DataFrame()
                
                with zip_ref.open(worksheet_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8', errors='ignore')
                    cell_pattern = r'<c[^>]*r="([A-Z]+\d+)"[^>]*(?:t="([^"]*)")?[^>]*>(?:.*?<v[^>]*>([^<]*)</v>)?(?:.*?<is><t[^>]*>([^<]*)</t></is>)?'
                    cells = re.findall(cell_pattern, content, re.DOTALL)
                    
                    if not cells:
                        return pd.DataFrame()
                    
                    cell_data = {}
                    max_row = 0
                    max_col = 0
                    
                    for cell_ref, cell_type, v_value, is_value in cells:
                        col_letters = ''.join([c for c in cell_ref if c.isalpha()])
                        row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
                        col_num = 0
                        for c in col_letters:
                            col_num = col_num * 26 + (ord(c) - ord('A') + 1)
                        
                        if is_value:
                            cell_value = is_value.strip()
                        elif cell_type == 's' and v_value:
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
                    
                    data = []
                    for row in range(1, max_row + 1):
                        row_data = []
                        for col in range(1, max_col + 1):
                            row_data.append(cell_data.get((row, col), ""))
                        if any(cell for cell in row_data):
                            data.append(row_data)
                    
                    if len(data) < max(1, header_row + 2):
                        return pd.DataFrame()
                    
                    if header_row == -1:
                        headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                        return pd.DataFrame(data, columns=headers)
                    else:
                        if len(data) > header_row:
                            headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                            return pd.DataFrame(data[header_row+1:], columns=headers)
                        else:
                            return pd.DataFrame()
                
        except:
            return pd.DataFrame()
    
    def _clean_cell_value(self, value):
        """Clean cell values"""
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return ""
            return value
        cleaned = str(value).strip().replace("'", "")
        return cleaned
    
    def _clean_dataframe(self, df):
        """Clean DataFrame"""
        if df.empty:
            return df
        
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        
        if len(df.columns) >= 5:
            fifth_col = df.columns[4]
            mask = ~(
                df[fifth_col].isna() | 
                (df[fifth_col].astype(str).str.strip() == "") |
                (df[fifth_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]
        
        df = df.drop_duplicates()
        return df
    
    def _check_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Check if sheet has headers"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1"
            ).execute()
            return bool(result.get('values', []))
        except:
            return False
    
    def _append_to_sheet(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, append_headers: bool):
        """Append DataFrame to Google Sheet"""
        try:
            clean_data = df.fillna('')
            
            if append_headers:
                values = [clean_data.columns.tolist()] + clean_data.values.tolist()
            else:
                values = clean_data.values.tolist()
            
            if not values:
                return
            
            body = {'values': values}
            
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
        except Exception as e:
            raise Exception(f"Failed to append to sheet: {str(e)}")
    
    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str) -> int:
        """Remove duplicates based on PO No and Sku Code"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:ZZ"
            ).execute()
            values = result.get('values', [])
            
            if not values:
                return 0
            
            max_len = max(len(row) for row in values)
            for row in values:
                row.extend([''] * (max_len - len(row)))
            
            headers = [values[0][i] if values[0][i] else f"Column_{i+1}" for i in range(max_len)]
            rows = values[1:]
            df = pd.DataFrame(rows, columns=headers)
            before = len(df)
            
            # Remove duplicates based on Sku Code and PO No
            if "Sku Code" in df.columns and "PO No" in df.columns:
                df = df.drop_duplicates(subset=["Sku Code", "PO No"], keep="first")
            
            after_dup = len(df)
            removed_dup = before - after_dup
            
            # Clean blanks
            df.replace('', pd.NA, inplace=True)
            df.dropna(how='all', inplace=True)
            df.dropna(how='all', axis=1, inplace=True)
            df.fillna('', inplace=True)
            
            after_clean = len(df)
            removed_clean = after_dup - after_clean
            
            # Sort by PO No
            if "PO No" in df.columns:
                df = df.sort_values(by="PO No", ascending=True)
            
            # Prepare data
            def process_value(v):
                if pd.isna(v) or v == '':
                    return ''
                try:
                    if '.' in str(v) or 'e' in str(v).lower():
                        return float(v)
                    return int(v)
                except (ValueError, TypeError):
                    return str(v)
            
            values = []
            values.append([str(col) for col in df.columns])
            for row in df.itertuples(index=False):
                values.append([process_value(cell) for cell in row])
            
            # Update sheet
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            body = {"values": values}
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body=body
            ).execute()
            
            total_removed = removed_dup + removed_clean
            logging.info(f"Removed {removed_dup} duplicates and {removed_clean} blank rows")
            return total_removed
                
        except Exception as e:
            logging.error(f"Error cleaning sheet: {str(e)}")
            return 0
    
    def save_workflow_summary(self):
        """Save workflow summary to Google Sheet"""
        try:
            config = self.config['excel']
            summary_sheet = config['summary_sheet_name']
            spreadsheet_id = config['spreadsheet_id']
            
            # Prepare summary data
            summary_data = [
                [
                    self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S') if self.stats['start_time'] else '',
                    self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S') if self.stats['end_time'] else '',
                    str(self.stats['emails_checked']),
                    str(self.stats['attachments_found']),
                    str(self.stats['attachments_saved']),
                    str(self.stats['attachments_skipped']),
                    str(self.stats['attachments_failed']),
                    str(self.stats['files_found']),
                    str(self.stats['files_processed']),
                    str(self.stats['files_skipped']),
                    str(self.stats['files_failed']),
                    str(self.stats['duplicates_removed']),
                    self.stats['status']
                ]
            ]
            
            # Check if summary sheet exists, if not create headers
            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{summary_sheet}!A1"
                ).execute()
                
                if not result.get('values'):
                    headers = [['Start Time', 'End Time', 'Emails Checked', 'Attachments Found',
                               'Attachments Saved', 'Attachments Skipped', 'Attachments Failed',
                               'Files Found', 'Files Processed', 'Files Skipped', 'Files Failed',
                               'Duplicates Removed', 'Status']]
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{summary_sheet}!A1",
                        valueInputOption='RAW',
                        body={'values': headers}
                    ).execute()
            except:
                # Sheet doesn't exist, create it with headers
                headers = [['Start Time', 'End Time', 'Emails Checked', 'Attachments Found',
                           'Attachments Saved', 'Attachments Skipped', 'Attachments Failed',
                           'Files Found', 'Files Processed', 'Files Skipped', 'Files Failed',
                           'Duplicates Removed', 'Status']]
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{summary_sheet}!A1",
                    valueInputOption='RAW',
                    body={'values': headers}
                ).execute()
            
            # Append summary
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{summary_sheet}!A1",
                valueInputOption='RAW',
                body={'values': summary_data}
            ).execute()
            
            logging.info("Workflow summary saved successfully")
            
        except Exception as e:
            logging.error(f"Failed to save workflow summary: {str(e)}")
    
    def send_email_notification(self):
        """Send email notification with workflow summary"""
        try:
            import base64
            
            # Get authenticated user's email
            user_profile = self.gmail_service.users().getProfile(userId='me').execute()
            user_email = user_profile.get('emailAddress', '')
            
            # Prepare recipients
            recipients = self.config['notification']['recipients'].copy()
            if self.config['notification']['send_to_self'] and user_email:
                recipients.append(user_email)
            
            if not recipients:
                logging.warning("No recipients configured for email notification")
                return False
            
            # Create email subject
            subject = f"Big Basket (Alert) Automation Workflow Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Create email body (HTML format for better readability)
            duration_minutes = (self.stats['end_time'] - self.stats['start_time']).total_seconds() / 60
            
            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6;">
                <h2>BigBasket Automation Workflow Summary</h2>
                <p><strong>Workflow Time:</strong> {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')} to {self.stats['end_time'].strftime('%H:%M:%S')}</p>
                <p><strong>Duration:</strong> {duration_minutes:.2f} minutes</p>
                <p><strong>Status:</strong> {self.stats['status']}</p>
                
                <h3>📧 Mail to Drive Workflow</h3>
                <ul>
                    <li><strong>Days Back Parameter:</strong> {self.stats['days_back_gmail']} days</li>
                    <li><strong>Number of Mails Checked:</strong> {self.stats['emails_checked']}</li>
                    <li><strong>Number of Attachments Found:</strong> {self.stats['attachments_found']}</li>
                    <li><strong>Number of Attachments Uploaded:</strong> {self.stats['attachments_saved']}</li>
                    <li><strong>Number of Attachments Skipped:</strong> {self.stats['attachments_skipped']}</li>
                    <li><strong>Failed to Upload:</strong> {self.stats['attachments_failed']}</li>
                </ul>
                
                <h3>📊 Drive to Sheet Workflow</h3>
                <ul>
                    <li><strong>Days Back Parameter:</strong> {self.stats['days_back_excel']} days</li>
                    <li><strong>Number of Files Found:</strong> {self.stats['files_found']}</li>
                    <li><strong>Number of Files Processed:</strong> {self.stats['files_processed']}</li>
                    <li><strong>Number of Files Skipped:</strong> {self.stats['files_skipped']}</li>
                    <li><strong>Number of Files Failed to Process:</strong> {self.stats['files_failed']}</li>
                    <li><strong>Duplicate Records Removed:</strong> {self.stats['duplicates_removed']}</li>
                </ul>
                
                <hr>
                <p style="color: #666; font-size: 0.9em;">
                    This is an automated email from BigBasket Automation Scheduler.
                    Workflow ran at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </body>
            </html>
            """
            
            # Plain text version for compatibility
            text_content = f"""
            BigBasket Automation Workflow Summary
            
            Workflow Time: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')} to {self.stats['end_time'].strftime('%H:%M:%S')}
            Duration: {duration_minutes:.2f} minutes
            Status: {self.stats['status']}
            
            📧 Mail to Drive Workflow:
            - Days Back Parameter: {self.stats['days_back_gmail']} days
            - Number of Mails Checked: {self.stats['emails_checked']}
            - Number of Attachments Found: {self.stats['attachments_found']}
            - Number of Attachments Uploaded: {self.stats['attachments_saved']}
            - Number of Attachments Skipped: {self.stats['attachments_skipped']}
            - Failed to Upload: {self.stats['attachments_failed']}
            
            📊 Drive to Sheet Workflow:
            - Days Back Parameter: {self.stats['days_back_excel']} days
            - Number of Files Found: {self.stats['files_found']}
            - Number of Files Processed: {self.stats['files_processed']}
            - Number of Files Skipped: {self.stats['files_skipped']}
            - Number of Files Failed to Process: {self.stats['files_failed']}
            - Duplicate Records Removed: {self.stats['duplicates_removed']}
            
            ---
            This is an automated email from BigBasket Automation Scheduler.
            Workflow ran at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            # Create email message
            message = {
                'raw': base64.urlsafe_b64encode(
                    f"From: {user_email}\r\n"
                    f"To: {', '.join(recipients)}\r\n"
                    f"Subject: {subject}\r\n"
                    f"Content-Type: text/html; charset=utf-8\r\n"
                    f"\r\n"
                    f"{html_content}".encode('utf-8')
                ).decode('utf-8')
            }
            
            # Send email
            self.gmail_service.users().messages().send(
                userId='me',
                body=message
            ).execute()
            
            logging.info(f"Email notification sent to {', '.join(recipients)}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to send email notification: {str(e)}")
            return False
    
    def run_workflow(self):
        """Run complete workflow"""
        try:
            # Reset stats
            self.stats = {
                'start_time': datetime.now(),
                'end_time': None,
                'days_back_gmail': self.config['gmail']['days_back'],
                'days_back_excel': self.config['excel']['days_back'],
                'emails_checked': 0,
                'attachments_found': 0,
                'attachments_saved': 0,
                'attachments_skipped': 0,
                'attachments_failed': 0,
                'files_found': 0,
                'files_processed': 0,
                'files_skipped': 0,
                'files_failed': 0,
                'duplicates_removed': 0,
                'status': 'Running'
            }
            
            # Reset cache
            self.existing_files_cache = {}
            
            logging.info("="*50)
            logging.info("Starting BigBasket Automation Workflow")
            logging.info("="*50)
            
            # Authenticate
            if not self.authenticate():
                self.stats['status'] = 'Failed - Authentication Error'
                self.stats['end_time'] = datetime.now()
                self.save_workflow_summary()
                return False
            
            # Run Gmail workflow
            logging.info("\n--- STEP 1: Gmail to Drive Workflow ---")
            gmail_success = self.process_gmail_workflow()
            
            if not gmail_success:
                logging.warning("Gmail workflow had issues, but continuing...")
            
            # Run Excel workflow
            logging.info("\n--- STEP 2: Drive to Sheet Workflow ---")
            excel_success = self.process_excel_workflow()
            
            if not excel_success:
                logging.warning("Excel workflow had issues")
            
            # Mark as complete
            self.stats['end_time'] = datetime.now()
            self.stats['status'] = 'Completed Successfully'
            
            # Save summary
            logging.info("\n--- STEP 3: Saving Workflow Summary ---")
            self.save_workflow_summary()
            
            # Send email notification
            logging.info("\n--- STEP 4: Sending Email Notification ---")
            email_success = self.send_email_notification()
            
            if not email_success:
                logging.warning("Email notification failed, but workflow completed")
            
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds() / 60
            logging.info("="*50)
            logging.info(f"Workflow completed in {duration:.2f} minutes")
            logging.info(f"Emails checked: {self.stats['emails_checked']}")
            logging.info(f"Attachments found: {self.stats['attachments_found']}")
            logging.info(f"Attachments saved: {self.stats['attachments_saved']}")
            logging.info(f"Attachments skipped: {self.stats['attachments_skipped']}")
            logging.info(f"Attachments failed: {self.stats['attachments_failed']}")
            logging.info(f"Files found: {self.stats['files_found']}")
            logging.info(f"Files processed: {self.stats['files_processed']}")
            logging.info(f"Files skipped: {self.stats['files_skipped']}")
            logging.info(f"Files failed: {self.stats['files_failed']}")
            logging.info(f"Duplicates removed: {self.stats['duplicates_removed']}")
            logging.info("="*50)
            
            return True
            
        except Exception as e:
            logging.error(f"Workflow failed: {str(e)}")
            self.stats['status'] = f'Failed - {str(e)}'
            self.stats['end_time'] = datetime.now()
            try:
                self.save_workflow_summary()
                self.send_email_notification()  # Send failure notification too
            except:
                pass
            return False
    
    def start_scheduler(self):
        """Start the scheduler to run workflow every 3 hours"""
        logging.info("BigBasket Automation Scheduler Started")
        logging.info("Workflow will run every 3 hours")
        
        # Schedule the job every 3 hours
        schedule.every(3).hours.do(self.run_workflow)
        
        # Run once immediately
        logging.info("Running initial workflow...")
        self.run_workflow()
        
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute


def main():
    """Main entry point with CLI arguments for GitHub Actions"""
    parser = argparse.ArgumentParser(description='BigBasket Automation Scheduler')
    parser.add_argument('--run-once', action='store_true', 
                       help='Run the workflow once and exit (for GitHub Actions)')
    args = parser.parse_args()
    
    try:
        scheduler = BigBasketScheduler(run_once=args.run_once)
        
        if args.run_once:
            # Run once and exit (for GitHub Actions)
            logging.info("Running workflow once (GitHub Actions mode)")
            scheduler.run_workflow()
        else:
            # Start scheduler for continuous operation
            scheduler.start_scheduler()
            
    except KeyboardInterrupt:
        logging.info("\nScheduler stopped by user")
    except Exception as e:
        logging.error(f"Scheduler error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":

    main()
