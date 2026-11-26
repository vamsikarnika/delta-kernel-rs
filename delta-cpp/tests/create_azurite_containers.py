"""Creates containers in Azurite with a SharedKey-signed request (object_store cannot create them)."""
import base64, hashlib, hmac, sys, urllib.request, urllib.error
from email.utils import formatdate

ACCOUNT = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # gitleaks:allow  (Azurite's public, documented development-account key; not a secret)
VERSION = "2021-08-06"

def create(container, host="127.0.0.1:10000"):
    date = formatdate(usegmt=True)
    headers = {"x-ms-date": date, "x-ms-version": VERSION, "Content-Length": "0"}
    canon_headers = f"x-ms-date:{date}\nx-ms-version:{VERSION}\n"
    resource = f"/{ACCOUNT}/{ACCOUNT}/{container}\nrestype:container"
    string_to_sign = "PUT\n\n\n\n\n\n\n\n\n\n\n\n" + canon_headers + resource
    sig = base64.b64encode(hmac.new(base64.b64decode(KEY), string_to_sign.encode(), hashlib.sha256).digest()).decode()
    headers["Authorization"] = f"SharedKey {ACCOUNT}:{sig}"
    req = urllib.request.Request(f"http://{host}/{ACCOUNT}/{container}?restype=container", method="PUT", headers=headers, data=None)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return f"{e.code} {e.read().decode()[:160]}"

for c in sys.argv[1:]:
    print(c, create(c))
