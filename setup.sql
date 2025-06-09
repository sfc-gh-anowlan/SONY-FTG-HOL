USE ROLE ACCOUNTADMIN;

-- Using ACCOUNTADMIN, create a new role for this exercise and grant to applicable users
CREATE ROLE TASK_GRAPH_ROLE;
set myname = current_user();
grant role TASK_GRAPH_ROLE to user identifier($myname);

--account level priv
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TASK_GRAPH_ROLE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE TASK_GRAPH_ROLE;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE TASK_GRAPH_ROLE;
GRANT MODIFY SESSION LOG LEVEL ON ACCOUNT TO ROLE task_graph_role;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE TASK_GRAPH_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE TASK_GRAPH_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE TASK_GRAPH_ROLE;

-- create our virtual warehouse
CREATE OR REPLACE WAREHOUSE TASK_GRAPH_WH AUTO_SUSPEND = 60;

GRANT ALL ON WAREHOUSE TASK_GRAPH_WH TO ROLE TASK_GRAPH_ROLE;

-- Next create a new database and schema,
CREATE OR REPLACE DATABASE TASK_GRAPH_DATABASE;
CREATE OR REPLACE SCHEMA TASK_GRAPH_SCHEMA;
CREATE EVENT TABLE TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph;


GRANT OWNERSHIP ON DATABASE TASK_GRAPH_DATABASE TO ROLE TASK_GRAPH_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE TASK_GRAPH_DATABASE TO ROLE TASK_GRAPH_ROLE COPY CURRENT GRANTS;
ALTER ACCOUNT SET EVENT_TABLE = TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph;
ALTER DATABASE TASK_GRAPH_DATABASE SET EVENT_TABLE = TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph;
GRANT OWNERSHIP ON TABLE TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph TO ROLE TASK_GRAPH_ROLE;


--OPTIONAL NOTIFICATION TESTING: SNS and/or SLACK--
/*
//AWS SNS integration 
USE ROLE TASK_GRAPH_ROLE;
CREATE IF NOT EXISTS NOTIFICATION INTEGRATION my_sns_notify_int
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:....'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::....';
*/

/*
--if the integration already exists
DESC NOTIFICATION INTEGRATION my_sns_notify_int;

-- Grant usage on the integration 
GRANT USAGE ON INTEGRATION my_sns_notify_int TO ROLE TASK_GRAPH_ROLE;
--monitor the notification history
SELECT count(*) FROM TABLE(INFORMATION_SCHEMA.NOTIFICATION_HISTORY()) WHERE INTEGRATION_NAME='MY_SNS_NOTIFY_INT' ORDER BY PROCESSED DESC Limit 5;
*/




/*
--create a slack sproc
USE ROLE TASK_GRAPH_ROLE;

CREATE OR REPLACE NETWORK RULE slack_webhook_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hooks.slack.com');

CREATE OR REPLACE SECRET slack_app_webhook_url
    type = GENERIC_STRING
    secret_string = <'your slack webhook'>
    comment = 'Slack Webhook URL';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION slack_webhook_access_integration
  ALLOWED_NETWORK_RULES = (slack_webhook_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (slack_app_webhook_url)
  ENABLED = true;

GRANT USAGE ON INTEGRATION slack_webhook_access_integration TO ROLE TASK_GRAPH_ROLE;
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
     "text": f"Task Graph Lab: {msg}"
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

*/
