from __future__ import annotations

import gspread
import pandas as pd


def fetch_sheet_as_dataframe(
    service_account_file: str,
    spreadsheet_id: str,
    worksheet_name: str,
) -> pd.DataFrame:
    if not service_account_file:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_FILE is required.")
    if not spreadsheet_id:
        raise ValueError("Spreadsheet ID is required.")

    client = gspread.service_account(filename=service_account_file)
    worksheet = client.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    records = worksheet.get_all_records()
    return pd.DataFrame(records)

