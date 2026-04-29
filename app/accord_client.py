import requests

BASE_URL = "https://contentapi.accordwebservices.com/RawData/GetRawDataJSON"


def fetch_accord_feed(filename: str, date_ddmmyyyy: str, token: str) -> tuple[int, dict | None]:
    response = requests.get(
        BASE_URL,
        params={
            "filename": filename,
            "date": date_ddmmyyyy,
            "section": "Fundamental",
            "sub": "",
            "token": token,
        },
        timeout=60,
    )

    if response.status_code == 200:
        return 200, response.json()

    if response.status_code == 204:
        return 204, None

    if response.status_code in (403, 404):
        return response.status_code, None

    raise RuntimeError(
        f"Unexpected API status={response.status_code}, body={response.text[:300]}"
    )