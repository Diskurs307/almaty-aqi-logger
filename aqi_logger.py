import os
import json
from datetime import datetime, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials


def get_aqi_from_iqair():
    api_key = os.environ["IQAIR_API_KEY"]
    url = (
        "https://api.airvisual.com/v2/city"
        "?city=Almaty&state=Almaty&country=Kazakhstan"
        f"&key={api_key}"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    aqi_us = pollution["aqius"]
    pm25 = pollution.get("p2", None)
    temp_c = weather.get("tp", None)
    humidity = weather.get("hu", None)

    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "timestamp": timestamp,
        "aqi_us": aqi_us,
        "pm25": pm25,
        "temp_c": temp_c,
        "humidity": humidity,
    }


def append_to_google_sheet(row_dict):
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    client = gspread.authorize(creds)

    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1  # первый лист

    row = [
        row_dict["timestamp"],
        row_dict["aqi_us"],
        row_dict["pm25"],
        row_dict["temp_c"],
        row_dict["humidity"],
    ]

    worksheet.append_row(row, value_input_option="RAW")


def main():
    print("Getting AQI data from IQAir...")
    row = get_aqi_from_iqair()
    print("Got:", row)
    print("Appending to Google Sheet...")
    append_to_google_sheet(row)
    print("Done.")


if __name__ == "__main__":
    main()
