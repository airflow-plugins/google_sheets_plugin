from airflow.plugins_manager import AirflowPlugin

from google_plugin.hooks.google_hook import GoogleHook
from google_plugin.operators.google_sheets_to_s3_operator import GoogleSheetsToS3Operator


class google_sheets_plugin(AirflowPlugin):
    name = "GoogleSheetsPlugin"
    operators = [GoogleSheetsToS3Operator]
    # Leave in for explicitness
    hooks = [GoogleHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
