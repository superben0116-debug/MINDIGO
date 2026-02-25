import os

# Configuration placeholders. Replace with real values or load from ENV.
LINGXING_APP_ID = os.getenv("LINGXING_APP_ID", "")
LINGXING_APP_SECRET = os.getenv("LINGXING_APP_SECRET", "")
LINGXING_ACCESS_TOKEN = os.getenv("LINGXING_ACCESS_TOKEN", "")
LINGXING_SID_LIST = [x.strip() for x in os.getenv("LINGXING_SID_LIST", "").split(",") if x.strip()]

# Shipper config for KAPI export
SHIPPER = {
    "zip": os.getenv("SHIPPER_ZIP", ""),
    "city": os.getenv("SHIPPER_CITY", ""),
    "state": os.getenv("SHIPPER_STATE", ""),
    "country": os.getenv("SHIPPER_COUNTRY", ""),
    "address_type": os.getenv("SHIPPER_ADDRESS_TYPE", ""),
    "service": os.getenv("SHIPPER_SERVICE", ""),
    "contact_name": os.getenv("SHIPPER_CONTACT_NAME", ""),
    "contact_phone": os.getenv("SHIPPER_CONTACT_PHONE", ""),
    "contact_email": os.getenv("SHIPPER_CONTACT_EMAIL", ""),
    "address_name": os.getenv("SHIPPER_ADDRESS_NAME", ""),
    "address_line1": os.getenv("SHIPPER_ADDRESS_LINE1", ""),
    "address_line2": os.getenv("SHIPPER_ADDRESS_LINE2", ""),
    "pickup_time_from": os.getenv("SHIPPER_PICKUP_TIME_FROM", ""),
    "pickup_time_to": os.getenv("SHIPPER_PICKUP_TIME_TO", ""),
}
