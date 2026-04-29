from app.config import ENABLE_ALERTS

def send_alert(title: str, message: str) -> None:
    if ENABLE_ALERTS:
        print(f"[ALERT] {title} - {message}")
