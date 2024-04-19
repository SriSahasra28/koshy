import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class utils:
    def __init__(self):
        self.sender_email = 'kimblylabs@gmail.com'
        self.receiver_email = 'kimblylabs@gmail.com' #'Mahesh@obelindia.com'
        self.smtp_server = 'smtp.gmail.com'
        self.smtp_port = 587
        self.username = 'kimblylabs@gmail.com'
        self.password = 'givnpwzasnwqssdw'
    def send_email(self, subject, message):
        # Create a multipart message object
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = self.receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))
        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.ehlo()
            server.starttls()
            server.login(self.username, self.password)
            server.sendmail(self.sender_email, self.receiver_email, msg.as_string())
            print("Email sent successfully!")
        except Exception as e:
            print("Error sending email:", str(e))
        finally:
            server.quit()
