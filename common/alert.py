import smtplib
from datetime import datetime as dt, timedelta

from beartype import beartype


class Alert:
    """
    parameter_name: name of the parameter to monitor
    threshold     : threshold value to monitor
    """

    def __init__(self,
                 parameter_name,
                 threshold,
                 alert_type=None,
                 **options):

        if alert_type is None:
            self.alert_type = ['EMAIL']
        else:
            self.alert_type = alert_type

        assert isinstance(self.alert_type, list), "Pass alert type as a list"

        self.parameter_name = parameter_name
        self.threshold = threshold
        self.executed_at = dt.utcnow() + timedelta(minutes=-3)
        self.live_executed_at = dt.utcnow()
        self.threshold = self.Threshold(**options)

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
                server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
                send_from = email_sender_credential['email_id']
                password = email_sender_credential['password']
                subject = options.get('subject') or f"Alert for {self.parameter_name} generated by Python"
                body = options.get('body') or f"Sending an alert, as value for {self.parameter_name} has reached " \
                                              f"threshold : <b>{self.threshold}</b> and the current value is" \
                                              f" the following:<br><br>"
                if options.get('data'):
                    body += options['data']
                msg = MIMEMultipart()
                msg['From'] = send_from
                msg['To'] = send_to
                msg['Subject'] = subject
                msg.attach(MIMEText(body, 'html'))

                text = msg.as_string()

                server.login(user=send_from, password=password)
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
        itr = 0
        while True:
            ss = options.get('ss')
            itr += 1
            if ss:
                ss.commit_batch_data([
                    {
                        'name': f'Jagriti-{itr}', 'company': 'micro', 'hostel': 'ff21', 'itr': itr
                    }
                ])
            if dt.utcnow() > self.live_executed_at + timedelta(seconds=fetch_data_interval_seconds):
                df = diff_data_func(**options)
                df[self.parameter_name] = df[self.parameter_name].apply(pd.to_numeric)

                if isinstance(df, pd.DataFrame):

                    if self.threshold.greater_than and (df[self.parameter_name] > self.threshold.greater_than).any():
                        if email_alert:
                            self.email_alert(data=df.to_html(), **options)
                    if self.threshold.lesser_than and (df[self.parameter_name] < self.threshold.lesser_than).any():
                        if email_alert:
                            self.email_alert(data=df.to_html(), **options)

                    if self.threshold.avg_for_vals:

                        if self.threshold.avg_for_vals.get('greater_than') \
                                and (
                                df[self.parameter_name].mean() > self.threshold.avg_for_vals.get('greater_than')).any():
                            if email_alert:
                                self.email_alert(data=df.to_html(), **options)

                        if self.threshold.avg_for_vals.get('lesser_than') \
                                and (
                                df[self.parameter_name].mean() < self.threshold.avg_for_vals.get('lesser_than')).any():
                            if email_alert:
                                self.email_alert(data=df.to_html(), **options)

                    if self.threshold.values_between:
                        if (df[self.parameter_name] < self.threshold.values_between[0]).any() \
                                or (df[self.parameter_name] > self.threshold.values_between[1]).any():
                            if email_alert:
                                self.email_alert(data=df.to_html(), **options)

                self.live_executed_at = dt.utcnow()
            print(f"Sleeping for {fetch_data_interval_seconds} seconds before checking again...")
            time.sleep(fetch_data_interval_seconds)
            print("hello, alive again")

    class Threshold:
        """
        Generates alert if data is greater than, lesser than or pass a dict in 'avg_for_vals'
        to get the avg of the fetched values ,
        avg needs to be taken, 'greater_than' or 'lesser_than' contains the value,
        values_between = [min_value, max_value]
        """

        def __init__(self,
                     lesser_than    = None,
                     greater_than   = None,
                     avg_for_vals   = None,
                     values_between = None,
                     **options
                     ):
            self.lesser_than    = lesser_than
            self.greater_than   = greater_than
            self.avg_for_vals   = avg_for_vals
            self.values_between = values_between
            if avg_for_vals:
                assert isinstance(avg_for_vals, dict), "'avg_for_vals' should be of type dict"
            if values_between:
                assert isinstance(values_between, (tuple, list)), "'values_between' should be of type dict'"
