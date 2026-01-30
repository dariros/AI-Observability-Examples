//Example parsing AI_OBSERVABILITY_EVENTS: Request, question asked, user, SQL generated, response given and the feedback

WITH agent_interactions AS (
    SELECT
        obs.TIMESTAMP,
        obs.RECORD_ATTRIBUTES:"ai.observability.record_id"::STRING AS request_id,
        obs.VALUE:"snow.ai.observability.request_body":"messages"[0]:content[0]:text::STRING AS user_question,
        obs.VALUE:"snow.ai.observability.response"::STRING AS agent_response_json,
        obs.RECORD_ATTRIBUTES:"snow.ai.observability.object.name"::STRING AS agent_name,
        obs.RESOURCE_ATTRIBUTES:"snow.user.name"::STRING AS user_name,
        obs.RECORD_ATTRIBUTES:"snow.ai.observability.database.name"::STRING AS database_name,
        obs.RECORD_ATTRIBUTES:"snow.ai.observability.schema.name"::STRING AS schema_name
    FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS obs
    WHERE obs.RECORD_ATTRIBUTES:"snow.ai.observability.object.type"::STRING = 'Cortex Agent'
        --AND obs.RECORD_ATTRIBUTES:"snow.ai.observability.object.name"::STRING = '<agent name>' --optionally subset to an agent
        AND obs.VALUE:"snow.ai.observability.response" IS NOT NULL
),
parsed_response AS (
    SELECT
        ai.*,
        TRY_PARSE_JSON(ai.agent_response_json) AS response_parsed,
        response_parsed:content AS content_array
    FROM agent_interactions ai
),
extracted_sql AS (
    SELECT
        pr.TIMESTAMP,
        pr.request_id,
        pr.user_name,
        pr.agent_name,
        pr.database_name,
        pr.schema_name,
        pr.user_question,
        f.value AS content_item,
        content_item:tool_result:content[0]:json:sql::STRING AS sql_statement,
        content_item:text::STRING AS text_response
    FROM parsed_response pr,
    LATERAL FLATTEN(input => pr.content_array) f
),
aggregated_sql AS (
    SELECT
        TIMESTAMP,
        request_id,
        user_name,
        agent_name,
        database_name,
        schema_name,
        user_question,
        ARRAY_AGG(DISTINCT sql_statement) WITHIN GROUP (ORDER BY sql_statement) AS executed_sql_statements,
        LISTAGG(DISTINCT text_response, '\n') WITHIN GROUP (ORDER BY text_response) AS final_text_response
    FROM extracted_sql
    WHERE sql_statement IS NOT NULL OR text_response IS NOT NULL
    GROUP BY TIMESTAMP, request_id, user_name, agent_name, database_name, schema_name, user_question
),
feedback_data AS (
    SELECT
        obs.TIMESTAMP AS feedback_timestamp,
        obs.RECORD_ATTRIBUTES:"ai.observability.record_id"::STRING AS request_id,
        obs.VALUE:"positive"::BOOLEAN AS positive_feedback,
        obs.VALUE:"feedback_message"::STRING AS feedback_message
    FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS obs
    WHERE obs.RECORD:"name"::STRING = 'CORTEX_AGENT_FEEDBACK'
)
SELECT
    asql.TIMESTAMP AS interaction_timestamp,
    asql.request_id,
    asql.user_name,
    asql.agent_name,
    asql.database_name,
    asql.schema_name,
    asql.user_question,
    asql.executed_sql_statements,
    asql.final_text_response AS final_response,
    fd.positive_feedback,
    fd.feedback_message AS user_feedback
FROM aggregated_sql asql
LEFT JOIN feedback_data fd
    ON asql.request_id = fd.request_id
ORDER BY asql.TIMESTAMP DESC;
