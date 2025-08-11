# Databricks notebook source
from datetime import datetime
import json
import logging
from pyspark.sql import DataFrame
import re
import requests
from typing import List

class EmailClient:
    """
    A client for sending emails via Microsoft Graph API.

    Attributes:
        from_email_addr (str): The sender's email address.
        _email_url (str): The URL for sending emails via Microsoft Graph API.
        _token_url (str): The URL for obtaining the access token.
        access_token (str): The access token for Microsoft Graph API.
    """

    def __init__(
        self, client_id: str, client_secret: str, tenant_id: str, from_email_addr: str
    ):
        self.logger = logging.getLogger("EmailClient")
        self.logger.setLevel(logging.INFO)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.stream_handler.setFormatter(self.formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(self.stream_handler)
        
        self._token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        self.access_token = self._get_token(client_id, client_secret)
        self._email_url = f"https://graph.microsoft.com/v1.0/users/{from_email_addr}/sendMail"
        
    def _get_token(
        self, client_id: str, client_secret: str
    ) -> str:
        """
        Retrieves the access token for Microsoft Graph API.

        Args:
            client_id (str): The client ID for authentication.
            client_secret (str): The client secret for authentication.

        Returns:
            str: The access token.
        """
        payload = {
            "grant_type": "client_credentials",
            "resource": "https://graph.microsoft.com",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        response = requests.post(url=self._token_url, data=payload)
        return response.json()["access_token"]

    def create_email_message(
        self,
        successful_tables: List[str],
        failed_tables: List[str],
        job_group_name: str,
        job_group: str,
        job_order: str,
        job_url: str
    ) -> (str, str):
        """
        Creates an email message based on job status.

        Args:
            successful_tables (list): List of successful table names.
            failed_tables (list): List of failed table names.
            job_group_name (str): Name of the job group.
            job_group (str): Job group identifier.
            job_order (str): Job order identifier.
            job_url (str): Current job url.
        Returns:
            tuple: A tuple containing subject and message for the email.
        """
        subject = f" {str(datetime.now().date())} {job_group_name} with jobGroup {job_group} and jobOrder {job_order}"

        common_message = f"<b>jobGroupName:</b> {job_group_name}<br><b> jobGroup:</b> {job_group}<br><b> jobOrder:</b> {job_order}"

        success_tbl_string, failure_tbl_string = (
            ", ".join(successful_tables),
            ", ".join(failed_tables),
        )


        if not failed_tables:
            job_status = "SUCCESSFUL"
            subject += " is " + job_status
            message_extn = f"<br><b> successfulTables:</b> {success_tbl_string} <br><b> For more details, please visit:</b> {job_url}"
        else:
            job_status = "FAILED"
            subject += " has " + job_status
            message_extn = f"<br><b> successfulTables:</b> {success_tbl_string} <br><b> failedTables:</b> {failure_tbl_string} <br><b> For more details, please visit:</b> {job_url}"

        message = f"{common_message}<br><b> jobStatus:</b> {job_status}{message_extn}"
        return subject, message

    def send_email(
        self, to_email_list: List[str], subject: str, message: str
    ) -> None:
        """
        Sends an email to the specified recipients.

        Args:
            to_email_list (list): List of email addresses to send the email to.
            subject (str): Subject of the email.
            message (str): Body of the email.

        Raises:
            Exception: If an error occurs during sending the email.
        """
        toRecipients = []
        try:
            for email in to_email_list:
                toRecipients.append({"emailAddress": {"address": email}})

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer " + self.access_token,
            }

            payload = json.dumps(
                {
                    "message": {
                        "subject": subject,
                        "body": {"contentType": "HTML", "content": message},
                        "toRecipients": toRecipients,
                    },
                    "saveToSentItems": "false",
                }
            )

            response = requests.post(url=self._email_url, headers=headers, data=payload)
            
            if response.status_code == 202:
                self.logger.info(
                    "Job notification email sent to "
                    + ", ".join((email) for email in to_email_list)
                )
            else:
                self.logger.info("Send email request failed with: " + str(response.json()))

        except Exception as e:
            self.logger.error("Exception occurred in sendEmail : {}".format(str(e)))


# COMMAND ----------


