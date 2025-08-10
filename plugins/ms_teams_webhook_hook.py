"""
MS Teams webhook hook for Airflow
"""
from airflow.hooks.base_hook import BaseHook
import requests
import json
from typing import Dict, List, Optional, Union


class MSTeamsWebhookHook(BaseHook):
    """
    Hook for sending messages to Microsoft Teams using Incoming Webhooks.
    
    :param ms_teams_webhook_conn_id: connection that has MS Teams webhook URL in the password field
    :param message: MS Teams message
    :param title: optional title for the Teams card
    :param subtitle: optional subtitle for the Teams card
    :param theme_color: optional theme color for the Teams card
    :param button_text: optional button text
    :param button_url: optional button URL
    """
    
    def __init__(
        self,
        ms_teams_webhook_conn_id: str = 'ms_teams_webhook_default',
        message: str = '',
        title: Optional[str] = None,
        subtitle: Optional[str] = None,
        theme_color: Optional[str] = None,
        button_text: Optional[str] = None,
        button_url: Optional[str] = None,
    ):
        super().__init__()
        self.ms_teams_webhook_conn_id = ms_teams_webhook_conn_id
        self.message = message
        self.title = title
        self.subtitle = subtitle
        self.theme_color = theme_color
        self.button_text = button_text
        self.button_url = button_url
    
    def get_webhook_url(self) -> str:
        """Get webhook URL from Airflow connection"""
        conn = self.get_connection(self.ms_teams_webhook_conn_id)
        return conn.password
    
    def build_teams_card(self) -> Dict:
        """Build the MS Teams card payload"""
        card = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": self.title or "Airflow Notification",
            "themeColor": self.theme_color or "0078D7",  # Default blue
        }
        
        sections = [{
            "activityTitle": self.title or "Airflow Notification",
            "text": self.message
        }]
        
        if self.subtitle:
            sections[0]["activitySubtitle"] = self.subtitle
        
        card["sections"] = sections
        
        # Add button if provided
        if self.button_text and self.button_url:
            card["potentialAction"] = [{
                "@type": "OpenUri",
                "name": self.button_text,
                "targets": [{"os": "default", "uri": self.button_url}]
            }]
        
        return card
    
    def send_message(self) -> None:
        """Send MS Teams message via webhook"""
        webhook_url = self.get_webhook_url()
        card = self.build_teams_card()
        
        response = requests.post(
            webhook_url,
            data=json.dumps(card),
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code != 200:
            raise ValueError(f"MS Teams webhook request failed: {response.text}")
