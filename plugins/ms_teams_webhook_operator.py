"""
MS Teams webhook operator for Airflow
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.ms_teams_webhook_hook import MSTeamsWebhookHook


class MSTeamsWebhookOperator(BaseOperator):
    """
    This operator allows you to post messages to Microsoft Teams using incoming webhooks.
    
    :param ms_teams_webhook_conn_id: connection that has MS Teams webhook URL in the password field
    :param message: MS Teams message
    :param title: optional title for the Teams card
    :param subtitle: optional subtitle for the Teams card
    :param theme_color: optional theme color for the Teams card
    :param button_text: optional button text
    :param button_url: optional button URL
    """
    
    template_fields = ('message', 'title', 'subtitle', 'button_text', 'button_url')
    
    @apply_defaults
    def __init__(
        self,
        ms_teams_webhook_conn_id='ms_teams_webhook_default',
        message='',
        title=None,
        subtitle=None,
        theme_color=None,
        button_text=None,
        button_url=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ms_teams_webhook_conn_id = ms_teams_webhook_conn_id
        self.message = message
        self.title = title
        self.subtitle = subtitle
        self.theme_color = theme_color
        self.button_text = button_text
        self.button_url = button_url
    
    def execute(self, context):
        """Call the MSTeamsWebhookHook to post the message"""
        self.log.info('Sending MS Teams message: %s', self.message)
        
        # Format title with DAG information if not provided
        if not self.title:
            dag_id = context['dag'].dag_id
            task_id = context['task'].task_id
            exec_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
            self.title = f"Airflow Alert - DAG: {dag_id}"
            self.subtitle = f"Task: {task_id} | Execution Time: {exec_date}"
        
        # Default theme color for success is green
        if not self.theme_color:
            self.theme_color = "00FF00"  # Green
        
        # Add a button to the Airflow UI if not provided
        if not self.button_url and not self.button_text:
            try:
                base_url = context['conf'].get('webserver', 'base_url')
                dag_id = context['dag'].dag_id
                task_id = context['task'].task_id
                execution_date = context['execution_date'].isoformat()
                self.button_url = f"{base_url}/task?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"
                self.button_text = "View in Airflow"
            except:
                self.log.info("Could not create button URL for Airflow UI")
        
        hook = MSTeamsWebhookHook(
            ms_teams_webhook_conn_id=self.ms_teams_webhook_conn_id,
            message=self.message,
            title=self.title,
            subtitle=self.subtitle,
            theme_color=self.theme_color,
            button_text=self.button_text,
            button_url=self.button_url
        )
        
        hook.send_message()
        
        return True
