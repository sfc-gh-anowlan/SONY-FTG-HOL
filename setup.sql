USE ROLE ACCOUNTADMIN;
  
-- Using ACCOUNTADMIN, create a new role for this exercise and grant to applicable users
CREATE OR REPLACE ROLE TASK_GRAPH_ROLE;
GRANT ROLE TASK_GRAPH_ROLE to USER <CURRENT_USER>;
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


--set event table at the database object
USE ROLE TASK_GRAPH_ROLE;
CREATE EVENT TABLE TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph;
ALTER DATABASE TASK_GRAPH_DATABASE SET EVENT_TABLE = TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph;
ALTER ACCOUNT SET EVENT_TABLE = TASK_GRAPH_DATABASE.TASK_GRAPH_SCHEMA.event_table_task_graph;

-- function to generate customer test data 
create or replace function gen_cust_info(num_records number)
returns table (custid number(10), cname varchar(100), spendlimit number(10,2))
language python
runtime_version=3.9
handler='CustTab'
packages = ('Faker')
as $$
from faker import Faker
import random

fake = Faker()
# Generate a list of customers  

class CustTab:
    # Generate multiple customer records
    def process(self, num_records):
        customer_id = 1000 # Starting customer ID                 
        for _ in range(num_records):
            custid = customer_id + 1
            cname = fake.name()
            spendlimit = round(random.uniform(1000, 10000),2)
            customer_id += 1
            yield (custid,cname,spendlimit)

$$;

-- function to generate product inventory data 
create or replace function gen_prod_inv(num_records number)
returns table (pid number(10), pname varchar(100), stock number(10,2), stockdate date)
language python
runtime_version=3.9
handler='ProdTab'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta
fake = Faker()

class ProdTab:
    # Generate multiple product records
    def process(self, num_records):
        product_id = 100 # Starting customer ID                 
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000),0)
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (3 months from now)
            min_date = current_date - timedelta(days=90)
            
            # Generate a random date within the date range
            stockdate = fake.date_between_dates(min_date,current_date)

            product_id += 1
            yield (pid,pname,stock,stockdate)

$$;

create or replace function gen_cust_purchase(num_records number,ndays number)
returns table (custid number(10), purchase variant)
language python
runtime_version=3.9
handler='genCustPurchase'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    # Generate multiple customer purchase records
    def process(self, num_records,ndays):       
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            
            #print(c_id)
            customer_purchase = {
                'custid': c_id,
                'purchased': []
            }
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (days from now)
            min_date = current_date - timedelta(days=ndays)
            
            # Generate a random date within the date range
            pdate = fake.date_between_dates(min_date,current_date)
            
            purchase = {
                'prodid': fake.random_int(min=101, max=199),
                'quantity': fake.random_int(min=1, max=5),
                'purchase_amount': round(random.uniform(10, 1000),2),
                'purchase_date': pdate
            }
            customer_purchase['purchased'].append(purchase)
            
            #customer_purchases.append(customer_purchase)
            yield (c_id,purchase)

$$;



--OPTIONAL NOTIFIACTION TESTING--
/*
//AWS SNS integration 
CREATE OR REPLACE NOTIFICATION INTEGRATION anowlan_sns_notify_int
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:....'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::....';

DESC NOTIFICATION INTEGRATION anowlan_sns_notify_int;

-- Grant usage on the integration 
GRANT USAGE ON INTEGRATION anowlan_sns_notify_int TO ROLE TASK_GRAPH_ROLE;

*/


/*
--create a slack sproc
CREATE OR REPLACE NETWORK RULE slack_webhook_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hooks.slack.com');


CREATE OR REPLACE SECRET slack_app_webhook_url
    type = GENERIC_STRING
    secret_string = ''
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

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION slack_webhook_access_integration
  ALLOWED_NETWORK_RULES = (slack_webhook_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (slack_app_webhook_url)
  ENABLED = true;

GRANT USAGE ON INTEGRATION slack_webhook_access_integration TO ROLE TASK_GRAPH_ROLE;

--Create the notification integration 
CREATE OR REPLACE NOTIFICATION INTEGRATION task_notifications 
      TYPE = WEBHOOK ENABLED = TRUE 
      WEBHOOK_URL = ''
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

-- Grant usage on the integration 
 GRANT USAGE ON INTEGRATION task_notifications TO ROLE TASK_GRAPH_ROLE;
*/


