import psycopg2
import json
import requests
import datetime

config = {
    "dbname": "arkansashealth_prod",
    "host": "arkansashealth-prod-redshift.cg8mcnhmisj5.us-east-1.redshift.amazonaws.com",
    "port": "5439",
    "user": "batman",
    "password": "Test#123"
}

webhook='https://hooks.slack.com/services/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'

class Runner(object):
    @staticmethod
    def runner(file_object):

        try:

            con = psycopg2.connect(dbname=config["dbname"], host=config["host"], port =\
                config["port"], user = config["user"], password = config["password"])

            cur = con.cursor()

            cur.execute("truncate l2.pd_activity;") # sql 1
            con.commit()

            cur.execute("select distinct ingdt from l1.pd_activity_test2 order by 1;")
            fuc = cur.fetchall()
            
            execution_time = datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') ##
            
            test_message = {
                "blocks":
                [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": ":checkered_flag: Movement to pd_activity Starting"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "-----------------x------------------"
                        }
                    },
                    {
                          "type": "section",
                          "text": {
                            "type": "mrkdwn",
                            "text": f":alarm_clock: Execution Time: {execution_time}"
                          }
                    }
                ]
            }
            requests.post(webhook, json.dumps(test_message))

            for f in fuc:
                
                cur.execute(f"select count(1) from l1.pd_activity_test2 where ingdt = '{str(f[0])}';")
                test = str(cur.fetchall()[0][0])

                cur.execute(f"insert into l2.pd_activity (select * from l1.pd_activity_test2 where ingdt = '{str(f[0])}');")
                con.commit()

                cur.execute(f"delete from l1.pd_activity_test2 where ingdt = '{str(f[0])}';")
                con.commit()
                
                cur.execute(f"select date_trunc('minute', '{str(f[0])}'::timestamp);")
                test2 = str(cur.fetchall()[0][0])

                message = {
                  "blocks":
                  [
                    {
                      "type": "context",
                      "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f":white_check_mark: movement for ingdt = {test2} --> count of records = {test}"
                        }
                      ]
                    }
                  ]
                }

                requests.post(webhook, json.dumps(message))
            yield '1'
            
            pest_message = {
                "blocks":
                [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "-----------------x------------------"
                        }
                    },
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": ":no_entry: Movement to pd_activity Ended"
                        }
                    }
                ]
            }
            requests.post(webhook, json.dumps(pest_message))
            
        except Exception as e:
            raise e