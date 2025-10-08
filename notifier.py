import smtplib
from email.mime.text import MIMEText

SMTP_SERVER = 'smtp.seuservidor.com'
SMTP_PORT = 587
SMTP_USER = 'no-reply@netunna.com'
SMTP_PASS = 'SENHA_AQUI'

def send_alert(assunto, corpo, destino):
    msg = MIMEText(corpo)
    msg['Subject'] = assunto
    msg['From'] = SMTP_USER
    msg['To'] = destino
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print(f'📧 Alerta enviado para {destino}')
    except Exception as e:
        print(f'❌ Falha ao enviar alerta: {e}')
