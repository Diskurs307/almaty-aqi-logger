import os
import json
from datetime import datetime, timezone, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials


# Классификация AQI по шкале США
def classify_aqi(aqi):
    if aqi <= 50:
        return "good"
    elif aqi <= 100:
        return "moderate"
    elif aqi <= 150:
        return "unhealthy_sensitive"
    elif aqi <= 200:
        return "unhealthy"
    elif aqi <= 300:
        return "very_unhealthy"
    else:
        return "hazardous"


# Получаем данные AQI из IQAir
def get_aqi_from_iqair():
    api_key = os.environ["IQAIR_API_KEY"]

    url = (
        "https://api.airvisual.com/v2/nearest_city"
        "?lat=43.238949&lon=76.889709"
        f"&key={api_key}"
    )

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    aqi_us = pollution["aqius"]
    main_pollutant = pollution.get("mainus")  # например: p2, o3, pm10

    temp_c = weather.get("tp")
    humidity = weather.get("hu")

    # Время в UTC
    ts_utc = datetime.now(timezone.utc)

    # Время в Алматы (UTC+5)
    almaty_tz = timezone(timedelta(hours=5))
    ts_almaty = ts_utc.astimezone(almaty_tz)

    return {
        "timestamp_utc": ts_utc.isoformat(),
        "timestamp_almaty": ts_almaty.isoformat(),
        "aqi_us": aqi_us,
        "aqi_category": classify_aqi(aqi_us),
        "main_pollutant": main_pollutant,
        "temp_c": temp_c,
        "humidity": humidity,
    }


# Записываем строку в Google Sheet
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
    worksheet = spreadsheet.sheet1

    row = [
        row_dict["timestamp_utc"],
        row_dict["timestamp_almaty"],
        row_dict["aqi_us"],
        row_dict["aqi_category"],
        row_dict["main_pollutant"],
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
