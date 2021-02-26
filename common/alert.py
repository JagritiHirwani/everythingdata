from beartype import beartype
import smtplib
from datetime import datetime as dt, timedelta


class Alert:
    """
    parameter_name: name of the parameter to monitor
    threshold     : threshold value to monitor
    """
    def __init__(self              ,
                 parameter_name    ,
                 threshold         ,
                 alert_type = None ,
                 **options):

        if alert_type is None:
            self.alert_type = ['EMAIL']
        else:
            self.alert_type = alert_type

        assert isinstance(self.alert_type, list), "Pass alert type as a list"

        self.parameter_name   = parameter_name
        self.threshold        = threshold
        self.executed_at      = dt.utcnow() + timedelta(minutes=-3)
        self.live_executed_at = dt.utcnow()

    @beartype
    def email_alert(self, email_sender_credential: dict, send_to: (str, list), **options):
        """
        To use this feature, you need have turned off the "secure apps" feature from your gmail account
        to enable this application to access your gmail account to send emails. You can do this by going here
        https://myaccount.google.com/security
        :param email_sender_credential:
        :param send_to:
        :param options:
        :return:
        """
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        assert email_sender_credential.get('email_id') and email_sender_credential.get('password'), \
            "Please pass email_sender_credential like {'email_id' : <email-id>, 'password' : <password>}"
        try:
            print("Trying to send email..")
            if dt.utcnow() > self.executed_at + timedelta(minutes=1):
                server    = smtplib.SMTP_SSL('smtp.gmail.com', 465)
                send_from = email_sender_credential['email_id']
                password  = email_sender_credential['password']
                subject   = options.get('subject') or f"Alert for {self.parameter_name} generated by Python"
                body      = options.get('body') or f"Sending an alert, as value for {self.parameter_name} has reached "\
                                                   f"threshold : {self.threshold} and the current value is" \
                                                   f" the following<br><br>"
                if options.get('data'):
                    body += options['data']
                msg = MIMEMultipart()
                msg['From']    = send_from
                msg['To']      = send_to
                msg['Subject'] = subject
                msg.attach(MIMEText(body, 'html'))

                text = msg.as_string()

                server.login(user = send_from, password = password)
                server.sendmail(send_from, send_to, text)
                print("Email sent!")
                self.executed_at = dt.utcnow()
                server.close()
            else:
                print("Just sent out an email.. Will be sending after 1 min")

        except Exception as err:
            print(f"Error in sending mail")
            raise ValueError(err.args[0])

    @beartype
    def set_alert_on_live_data(self, diff_data_func, **options):
        """
        Set alert on data by giving parameter name and threshold
        :return:
        """
        import time
        import pandas as pd

        fetch_data_interval_seconds = options.get('fetch_data_interval_seconds') or 30
        email_alert = True if 'email' in [val.lower() for val in self.alert_type] else False
        while True:
            ss = options.get('ss')
            if ss:
                ss.commit_batch_data([
                    {
                        'name': 'Jagriti', 'company': 'micro', 'hostel': 'ff21'
                    },
                    {
                        'sajal': 'Yes this is sajal'
                    }
                ])
            if dt.utcnow() > self.live_executed_at + timedelta(seconds=fetch_data_interval_seconds):
                df = diff_data_func(**options)
                df[self.parameter_name] = df[self.parameter_name].apply(pd.to_numeric)
                if isinstance(df, pd.DataFrame) and (df[self.parameter_name] > self.threshold).any():
                    if email_alert:
                        self.email_alert(data=df.to_html(), **options)
                self.live_executed_at = dt.utcnow()
            print(f"Sleeping for {fetch_data_interval_seconds} seconds before checking again...")
            time.sleep(fetch_data_interval_seconds)
            print("hello, alive again")
