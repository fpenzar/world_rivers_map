import pyotp

SECRET = "6ELOVKWIFIUU6BQCWQOS5JOU4F5RVGNC"


class TOTP_manager:

    def __init__(self, secret):
        self.secret = secret
    
    def verify(self, token):
        totp = pyotp.TOTP(self.secret, interval=60)
        return totp.verify(token)