import pyotp


class TOTP_manager:

    def __init__(self, secret):
        self.secret = secret
    
    def verify(self, token):
        # digest is SHA1
        # validity is 4 minutes
        totp = pyotp.TOTP(self.secret, interval=4*60, digits=6)
        return totp.verify(token)