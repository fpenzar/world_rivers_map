import requests
import pyotp

url = "http://192.168.1.186:8000/"
SECRET = "6ELOVKWIFIUU6BQCWQOS5JOU4F5RVGNC"
totp = pyotp.TOTP(SECRET, interval=5 * 60)

def get_data(z, x, y):
    headers = {"totp-token": totp.now()}
    response = requests.get(url + f"data/{z}/{x}/{y}.pbf", headers=headers)
    print(len(response.content))
    print(response)


get_data(4,13, 10)