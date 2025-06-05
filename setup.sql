USE ROLE ACCOUNTADMIN;
  
-- Using ACCOUNTADMIN, create a new role for this exercise and grant to applicable users
CREATE OR REPLACE ROLE TASK_GRAPH_ROLE;
GRANT ROLE TASK_GRAPH_ROLE to USER ANOWLAN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TASK_GRAPH_ROLE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE TASK_GRAPH_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE TASK_GRAPH_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE TASK_GRAPH_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE TASK_GRAPH_ROLE;

-- create our virtual warehouse
CREATE OR REPLACE WAREHOUSE TASK_GRAPH_WH AUTO_SUSPEND = 60;

GRANT ALL ON WAREHOUSE TASK_GRAPH_WH TO ROLE TASK_GRAPH_ROLE;

-- Next create a new database and schema,
CREATE OR REPLACE DATABASE TASK_GRAPH_DATABASE;
CREATE OR REPLACE SCHEMA TASK_GRAPH_SCHEMA;

GRANT OWNERSHIP ON DATABASE TASK_GRAPH_DATABASE TO ROLE TASK_GRAPH_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE TASK_GRAPH_DATABASE TO ROLE TASK_GRAPH_ROLE COPY CURRENT GRANTS;

--OPTIONAL NOTIFIACTION TESTING--
--create a slack sproc
CREATE OR REPLACE NETWORK RULE slack_webhook_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hooks.slack.com');


CREATE OR REPLACE SECRET slack_app_webhook_url
    type = GENERIC_STRING
    secret_string = <'from slack'>
    comment = 'Slack Webhook URL to the anowlan sandbox for a demo';

CREATE OR REPLACE PROCEDURE send_slack_message(MSG string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (slack_webhook_access_integration)
SECRETS = ('slack_url' = slack_app_webhook_url)
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
import json
import requests
import _snowflake
from datetime import date

def main(session, msg): 
    # Retrieve the Webhook URL from the SECRET object
    webhook_url = _snowflake.get_generic_secret_string('slack_url')

    slack_data = {
     "text": f"Snowflake says: {msg}"
    }

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )
    
    return "SUCCESS"
$$;


//sns topic notifications
USE ROLE ACCOUNTADMIN;


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION slack_webhook_access_integration
  ALLOWED_NETWORK_RULES = (slack_webhook_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (slack_app_webhook_url)
  ENABLED = true;


/* Create the notification integration */
CREATE OR REPLACE NOTIFICATION INTEGRATION task_notifications 
    TYPE = WEBHOOK ENABLED = TRUE 
    WEBHOOK_URL = 'https://hooks.slack.com/services/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'
    WEBHOOK_SECRET = TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.slack_app_webhook_url
    WEBHOOK_BODY_TEMPLATE='{
  "routing_key": "SNOWFLAKE_WEBHOOK_SECRET",
  "event_action": "trigger",
  "payload":
    {
      "summary": "SNOWFLAKE_WEBHOOK_MESSAGE",
      "source": "Snowflake monitoring",
      "severity": "INFO",
    }
  }'
WEBHOOK_HEADERS=('Content-Type'='application/json');


CREATE NOTIFICATION INTEGRATION anowlan_sns_notify_int
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:....:topic-snowpark-events'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::...:role/role-sns-snowpark';

DESC NOTIFICATION INTEGRATION anowlan_sns_notify_int;


/* Grant usage on the integration */
GRANT USAGE ON INTEGRATION task_notifications TO ROLE your_role;
