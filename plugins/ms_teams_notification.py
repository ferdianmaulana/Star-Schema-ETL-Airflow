"""
MS Teams notification plugin for Airflow
"""
from airflow.plugins_manager import AirflowPlugin
from plugins.ms_teams_webhook_hook import MSTeamsWebhookHook
from plugins.ms_teams_webhook_operator import MSTeamsWebhookOperator


class MSTeamsNotificationPlugin(AirflowPlugin):
    """MS Teams notification plugin for Airflow"""
    name = "ms_teams_notification"
    hooks = [MSTeamsWebhookHook]
    operators = [MSTeamsWebhookOperator]
    
    # Define helper functions to make it easier to use in DAGs
    @staticmethod
    def notify_success(context):
        """
        Function to be used as a callback for task success.
        
        Usage:
        ```
        default_args = {
            'on_success_callback': MSTeamsNotificationPlugin.notify_success,
        }
        ```
        """
        task_id = context['task'].task_id
        dag_id = context['dag'].dag_id
        exec_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"Task '{task_id}' in DAG '{dag_id}' succeeded on {exec_date}."
        
        operator = MSTeamsWebhookOperator(
            task_id='ms_teams_success_notification',
            message=message,
            theme_color="00FF00",  # Green
            dag=context['dag']
        )
        
        return operator.execute(context)
    
    @staticmethod
    def notify_failure(context):
        """
        Function to be used as a callback for task failure.
        
        Usage:
        ```
        default_args = {
            'on_failure_callback': MSTeamsNotificationPlugin.notify_failure,
        }
        ```
        """
        task_id = context['task'].task_id
        dag_id = context['dag'].dag_id
        exec_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        exception = context.get('exception')
        
        message = f"Task '{task_id}' in DAG '{dag_id}' failed on {exec_date}."
        if exception:
            message += f"\n\nError: {str(exception)}"
        
        operator = MSTeamsWebhookOperator(
            task_id='ms_teams_failure_notification',
            message=message,
            theme_color="FF0000",  # Red
            dag=context['dag']
        )
        
        return operator.execute(context)
    
    @staticmethod
    def notify_retry(context):
        """
        Function to be used as a callback for task retry.
        
        Usage:
        ```
        default_args = {
            'on_retry_callback': MSTeamsNotificationPlugin.notify_retry,
        }
        ```
        """
        task_id = context['task'].task_id
        dag_id = context['dag'].dag_id
        exec_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        try_number = context['ti'].try_number
        max_tries = context['ti'].max_tries
        
        message = f"Task '{task_id}' in DAG '{dag_id}' is being retried on {exec_date}. "
        message += f"Retry {try_number} of {max_tries}."
        
        operator = MSTeamsWebhookOperator(
            task_id='ms_teams_retry_notification',
            message=message,
            theme_color="FFA500",  # Orange
            dag=context['dag']
        )
        
        return operator.execute(context)
