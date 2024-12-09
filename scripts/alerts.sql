USE ROLE kamesh_demos;

-- Customer Support Tickets Alert
CREATE SCHEMA IF NOT EXISTS kamesh_demos.alerts_and_notification;

-- Serverless Alert to notify Slack channel on data load into support tickets table
CREATE OR REPLACE ALERT kamesh_demos.alerts_and_notification.customer_support_tickets_alert
  SCHEDULE = '1 MINUTE'
  IF (EXISTS (
    SELECT row_count,
    FROM TABLE(build24_keynote_demo.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'build24_keynote_demo.data.SUPPORT_TICKETS',
        START_TIME=>DATEADD('minutes', -1, CURRENT_TIMESTAMP())
    ))
    WHERE STATUS ilike 'LOADED'
    AND LAST_LOAD_TIME >= DATEADD('minutes', -1, CURRENT_TIMESTAMP())
  ))
  THEN
    DECLARE
        support_ticket_record_count int;
        slack_message varchar;
    BEGIN
            LET cur CURSOR FOR  (SELECT row_count FROM TABLE(RESULT_SCAN(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID())));

            OPEN cur;
            FETCH cur into :support_ticket_record_count;
            CLOSE cur;

            SYSTEM$LOG_INFO('Total number of tickets: ' || TO_VARCHAR(:support_ticket_record_count));
            slack_message := OBJECT_CONSTRUCT('blocks', 
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(
                        'type', 'section',
                        'text', OBJECT_CONSTRUCT(
                            'type', 'mrkdwn',
                            'text', '*Data Refreshed and Ready.* 👍🏽'
                        )
                    ),
                    OBJECT_CONSTRUCT(
                        'type', 'divider'
                    ),
                    OBJECT_CONSTRUCT(
                        'type', 'section',
                        'text', OBJECT_CONSTRUCT(
                            'type', 'mrkdwn',
                            'text', CONCAT('Number of records ingested: ', TO_VARCHAR(:support_ticket_record_count))
                        )
                    )
                )
            )::STRING;
            SYSTEM$LOG_INFO('Slack block:'|| :slack_message);
            CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                (SELECT SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(
                SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(:slack_message))),
                SNOWFLAKE.NOTIFICATION.INTEGRATION('KAMESHS_SLACK_DEMOMATE')
            );
    END;
;

ALTER ALERT kamesh_demos.alerts_and_notification.customer_support_tickets_alert SUSPEND;
ALTER ALERT kamesh_demos.alerts_and_notification.customer_support_tickets_alert RESUME;