import constants
import requests
import pandas as pd

def executeQuery(query):
    payload = {
        "query": query,
        "secret": constants.CARRUM_API_SECRET
    }
    res = requests.post(constants.CARRUM_API_ENDPOINT, json=payload)
    jsonData = res.json()
    return jsonData['data']



def getLeadBankQuery(limit, offset, innerLimit, outerOffset):
    return f'''
                            WITH lead_bank_data AS (SELECT *
                                    FROM lead_bank
                                    ORDER BY phone ASC
                                    LIMIT {innerLimit} OFFSET {outerOffset}
                                )
                               , call_history_filtered AS (
                            SELECT *
                            FROM call_history
                            WHERE
                                dialstatus NOT IN ("ATTEMPT_FAILED"
                                , "PROVIDER_FAILURE"
                                , "FAILED"
                                , "PROVIDER_TEMP_FAILURE"
                                , "CHANUNAVAIL"
                                , "NUMBER_FAILURE")
                               OR dialstatus IS NULL
                                )
                            SELECT lbd.lead_id,
                                   lbd.phone,
                                   lbd.name,
                                   lbd.uploaded_date,
                                   lbd.source,
                                   lbd.city,

                                   MIN(ch.call_date)                                                         AS first_call_date,
                                   MAX(ch.call_date)                                                         AS last_call_data,

                                   SUM(CASE WHEN ch.campaign_id LIKE '%BND' THEN 1 ELSE 0 END)               AS out_bound_count,
                                   SUM(CASE WHEN ch.campaign_id LIKE '%INB' THEN 1 ELSE 0 END)               AS in_bound_count,

                                   MAX(CASE
                                           WHEN ch.status IN ("DROP", "PDROP", "AFTHRS", "TIMEOT") THEN ch.call_date
                                           ELSE NULL END)                                                    AS latest_drop_call_date,
                                   MAX(CASE WHEN ch.campaign_id LIKE '%BND' THEN ch.call_date ELSE NULL END) AS max_out_bound_call_date,

                                   CASE
                                       WHEN MAX(CASE WHEN ch.campaign_id LIKE '%BND' THEN ch.call_date ELSE NULL END) >
                                            MAX(CASE
                                                    WHEN ch.status IN ("DROP", "PDROP", "AFTHRS", "TIMEOT")
                                                        THEN ch.call_date
                                                    ELSE NULL END)
                                           THEN 'yes'
                                       ELSE 'no'
                                       END                                                                   AS outbound_after_drop
                            FROM lead_bank_data lbd
                                     LEFT JOIN call_history_filtered ch ON lbd.phone = ch.phone_number_dialed
                            GROUP BY lbd.phone
                            LIMIT {limit} OFFSET {offset}'''
                            
                            
def getLeadBankDataCountQuery():
    return f'''
    select count(*) as count from lead_bank
'''


def getLeadStatusQuery(innerOffset, innerLimit, outerLimit, outerOffset):
    return f'''
    WITH
    lead_bank_data AS (
        SELECT phone
        FROM lead_bank
        ORDER BY phone ASC
        LIMIT {innerLimit} OFFSET {innerOffset}
    ),
    max_call_dates AS (
        SELECT
        phone_number_dialed,
        MAX(call_date) AS max_call_date
        FROM call_history ch
        JOIN lead_bank_data lbd ON lbd.phone = ch.phone_number_dialed
        WHERE dialstatus NOT IN (
        'ATTEMPT_FAILED', 'PROVIDER_FAILURE', 'FAILED',
        'PROVIDER_TEMP_FAILURE', 'CHANUNAVAIL', 'NUMBER_FAILURE'
        )
        GROUP BY phone_number_dialed
    )

    SELECT
    lbd.phone,
    ch.call_date AS max_call_date,
    ch.status AS last_call_status
    FROM
    lead_bank_data lbd
    left JOIN max_call_dates mcd
        ON lbd.phone = mcd.phone_number_dialed
    left JOIN call_history ch
        ON ch.phone_number_dialed = mcd.phone_number_dialed
        AND ch.call_date = mcd.max_call_date
    WHERE
    ch.dialstatus NOT IN (
        'ATTEMPT_FAILED', 'PROVIDER_FAILURE', 'FAILED',
        'PROVIDER_TEMP_FAILURE', 'CHANUNAVAIL', 'NUMBER_FAILURE'
    )
    LIMIT {outerLimit} OFFSET {outerOffset}   
'''

def getCallHistoryWithDialedCount(innerOffset, innerLimit, outerLimit, outerOffset):
   return f'''
    WITH
        lead_bank_data AS (
            SELECT phone
            FROM lead_bank
            ORDER BY phone ASC
            LIMIT {innerLimit} OFFSET {innerOffset}
        ),
        status_counts AS (
            SELECT
            ch.phone_number_dialed,
            ch.status,
            COUNT(*) AS status_count
            FROM call_history ch
            join lead_bank_data lbd ON lbd.phone = ch.phone_number_dialed
            WHERE ch.dialstatus NOT IN (
            'ATTEMPT_FAILED', 'PROVIDER_FAILURE', 'FAILED',
            'PROVIDER_TEMP_FAILURE', 'CHANUNAVAIL', 'NUMBER_FAILURE'
            )
            GROUP BY ch.phone_number_dialed, ch.status
        )

        SELECT
        lbd.phone AS phone_number_dialed,
        GROUP_CONCAT(CONCAT(sc.status, '(', sc.status_count, ')') ORDER BY sc.status SEPARATOR ', ') AS status_summary
        FROM
        lead_bank_data lbd
        LEFT JOIN status_counts sc ON lbd.phone = sc.phone_number_dialed
        GROUP BY lbd.phone
        LIMIT {outerLimit} OFFSET {outerOffset};
    '''

def getCallingHistoryStageScore(innerOffset, innerLimit, outerLimit, outerOffset):
    return f'''
        WITH
        lead_bank_data AS (
            SELECT phone
            FROM lead_bank
            ORDER BY phone ASC
            LIMIT {innerLimit} OFFSET {innerOffset}
        ),
        status_scores AS (
             SELECT 'NSO' AS status, 30 AS score UNION ALL
            SELECT 'DAP', 30 UNION ALL
            SELECT 'WRGN', 30 UNION ALL
            SELECT 'BT', 30 UNION ALL
            SELECT 'DVNAUD', 30 UNION ALL
            SELECT 'NAR', 30 UNION ALL
            SELECT 'DROP', 30 UNION ALL
            SELECT 'ADCT', 30 UNION ALL
            SELECT 'PDROP', 30 UNION ALL
            SELECT 'NA', 30 UNION ALL
            SELECT '', 30 UNION ALL
            SELECT 'FOLLOW', 60 UNION ALL
            SELECT 'INTVDC', 80 UNION ALL
            SELECT 'WTHINK', 70 UNION ALL
            SELECT 'ISULOC', 50 UNION ALL
            SELECT 'ISUDOC', 50 UNION ALL
            SELECT 'ISUSD', 50 UNION ALL
            SELECT 'ISUID', 50 UNION ALL
            SELECT 'WWCOMP', 40 UNION ALL
            SELECT 'ISUOS', 40 UNION ALL
            SELECT 'OISPTW', 40 UNION ALL
            SELECT 'OWNVAN', 40 UNION ALL
            SELECT 'ABULRB', 40 UNION ALL
            SELECT 'NORESN', 40 UNION ALL
            SELECT 'HND', 40 UNION ALL
            SELECT 'AWC', 10 UNION ALL
            SELECT 'DPSD', 100 UNION ALL
            SELECT 'OPSCC', 10 UNION ALL
            SELECT 'LANGB', 10 UNION ALL
            SELECT 'NEWLEAD', 20 
        )

        SELECT
        lbd.phone AS phone_number_dialed,
        MAX(IFNULL(ss.score, 0)) AS max_score
        FROM
        lead_bank_data lbd
        LEFT JOIN call_history ch
        ON lbd.phone = ch.phone_number_dialed
        LEFT JOIN status_scores ss
        ON ch.status = ss.status
        WHERE
        ch.dialstatus NOT IN (
            'ATTEMPT_FAILED', 'PROVIDER_FAILURE', 'FAILED',
            'PROVIDER_TEMP_FAILURE', 'CHANUNAVAIL', 'NUMBER_FAILURE'
        ) OR ch.dialstatus IS NULL
        GROUP BY
        lbd.phone
        LIMIT {outerLimit} OFFSET {outerOffset};
'''


def updateSheet(data_to_save, leadBankTableTab):
    # print(data_to_save)
    data_to_save = [list(data_to_save.columns)] + data_to_save.replace([float('inf'), float('-inf')], pd.NA).fillna('').astype(str).values.tolist()
    batch_size = int(100000)
    total_rows = int(len(data_to_save))
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        print(f"Updating rows {start+1} to {end}...")
        leadBankTableTab.update(data_to_save[start:end], f"A{start+1}")
    print("✅ Data saved to Google Sheet tab successfully!")