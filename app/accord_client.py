import time
import requests
from app.config import (
    API_MAX_RETRIES, API_RETRY_BACKOFF_1, API_RETRY_BACKOFF_2, 
    API_RETRY_BACKOFF_3, API_TIMEOUT_SECONDS
)

BASE_URL = "https://contentapi.accordwebservices.com/RawData/GetRawDataJSON"

def fetch_accord_feed(filename: str, date_ddmmyyyy: str, token: str) -> tuple[int, dict | None]:
    max_attempts = API_MAX_RETRIES
    backoff_times = [API_RETRY_BACKOFF_1, API_RETRY_BACKOFF_2, API_RETRY_BACKOFF_3]
    
    for attempt in range(max_attempts):
        try:
            response = requests.get(
                BASE_URL,
                params={
                    "filename": filename,
                    "date": date_ddmmyyyy,
                    "section": "Fundamental",
                    "sub": "",
                    "token": token,
                },
                timeout=API_TIMEOUT_SECONDS,
            )
            
            if response.status_code == 200:
                return 200, response.json()
            
            if response.status_code == 204:
                return 204, None
            
            if response.status_code in (403, 404):
                return response.status_code, None
            
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < max_attempts - 1:
                    time.sleep(backoff_times[attempt])
                    continue
                
            raise RuntimeError(
                f"Unexpected API status={response.status_code}, body={response.text[:300]}"
            )
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_attempts - 1:
                time.sleep(backoff_times[attempt])
                continue
            raise RuntimeError(f"Request failed after {max_attempts} attempts: {e}")