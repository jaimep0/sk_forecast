import os
import requests
from dotenv import load_dotenv

load_dotenv()


def get_ml_creds():
    return {
        "client_id": os.getenv("ML_CLIENT_ID"),
        "client_secret": os.getenv("ML_CLIENT_SECRET"),
        "refresh_token": os.getenv("ML_REFRESH_TOKEN"),
        "access_token": os.getenv("ML_ACCESS_TOKEN"),
        "user_id": os.getenv("ML_USER_ID"),
    }


def get_ml_access_token():
    r = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": os.getenv("ML_CLIENT_ID"),
            "client_secret": os.getenv("ML_CLIENT_SECRET"),
            "refresh_token": os.getenv("ML_REFRESH_TOKEN"),
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    data = r.json()

    if r.status_code == 200 and "access_token" in data:
        return data["access_token"]

    raise RuntimeError(f"Could not refresh ML token: {r.status_code} - {data}")


def get_ml_creds():
    return {
        "client_id": os.getenv("ML_CLIENT_ID"),
        "client_secret": os.getenv("ML_CLIENT_SECRET"),
        "refresh_token": os.getenv("ML_REFRESH_TOKEN"),
        "user_id": os.getenv("ML_USER_ID"),
        "access_token": get_ml_access_token(),
    }