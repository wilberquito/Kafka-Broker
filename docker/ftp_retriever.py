from ftplib import FTP
from io import BytesIO
        
def retrieve_bytes(file: str, host: str, passwd: str, user: str, port:int=21) -> bytes:
    ''' Download target file via FTP 
    This method can throw handled exceptions like authentications problem to remote server
    '''
    with FTP() as ftp:
        ftp.connect(host, port)
        ftp.login(user, passwd)
        r = BytesIO()
        ftp.retrbinary('RETR ' + file, r.write)
        return r.getvalue()

