"""
SmtpEmailSender — cliente SMTP para enviar correos HTML.

Abre y cierra la conexión SMTP en cada envío: simple, sin estado compartido y
sin problemas de concurrencia. Para el caso de uso actual (máximo 5 correos
por request en el endpoint /send-email-test) el costo del handshake es
despreciable.
"""
import asyncio
import html
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from logging_config import get_logger

logger = get_logger(__name__)


class SmtpEmailSender:
    def __init__(self, host: str, port: int, user: str, password: str, sender: str):
        self._host     = host
        self._port     = port
        self._user     = user
        self._password = password
        self._sender   = sender  # Formato: 'Nombre <correo@dominio>' o 'correo@dominio'

    async def send(self, to: str, subject: str, content: str) -> bool:
        # smtplib es bloqueante — se ejecuta en thread para no bloquear el event loop.
        # Devolvemos True/False en vez de propagar la excepción para que el caller
        # pueda continuar con los demás destinatarios si uno falla.
        try:
            await asyncio.to_thread(self._send_sync, to, subject, content)
            return True
        except Exception as e:
            logger.warning("SMTP send failed | to=%s | error=%s", to, e)
            return False

    def _send_sync(self, to: str, subject: str, content: str) -> None:
        msg = MIMEMultipart()
        msg["From"]    = self._sender
        msg["To"]      = to
        msg["Subject"] = subject
        # html.unescape revierte entidades HTML (&amp;, &lt;) que el frontend
        # puede haber introducido al serializar el content.
        msg.attach(MIMEText(html.unescape(content), "html"))

        # 'with' garantiza el cierre de la conexión aunque smtp.send_message falle.
        with smtplib.SMTP(self._host, self._port) as smtp:
            smtp.starttls()
            smtp.login(self._user, self._password)
            smtp.send_message(msg)
