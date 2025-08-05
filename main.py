import gspread
from google.oauth2.service_account import Credentials
import constants
import util
import pandas as pd
import numpy as np
import time 

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
]

def main():
    startTime = time.perf_counter()
    creds = Credentials.from_service_account_file("serviceAccount.json", scopes=scope)
    
    client = gspread.authorize(creds)
    
    print("✅ Authentication successful!")
    
    sheet = client.open_by_key(constants.SHEET_ID)
    print(f"Opened Sheet: {sheet.title}")
    leadBankTableTab = sheet.worksheet(constants.LEAD_BANK_TAB_NAME)
    stageMapTab = sheet.worksheet(constants.STAGE_MAP_TAB_NAME)
    stageMapData  = stageMapTab.get_all_records()
    print("Stage Map Data fetched successfully!")

    leadBankOuterLimit = 2000
    leadBankInnerLimit = 2000
    leadBankInnerOffset = 0
    print("Fetching Lead bank count...")
    leadBankDataCountQuery = util.getLeadBankDataCountQuery()
    leadBankDataCountResult = util.executeQuery(leadBankDataCountQuery)
    leadBankDataCount = leadBankDataCountResult[0]['count'] 
    # leadBankDataCount = 10000;
    print(f"Total Lead Bank Data Count: {leadBankDataCount}")


    print("Fetching Lead Bank Data...")
    """
    [{
        lead_id: string;
        phone: string;
        name: string;
        uploaded_date: string; // or Date
        source: string;
        city: string;
        first_call_date: string | null; // or Date
        last_call_date: string | null; // or Date
        out_bound_count: number;
        in_bound_count: number;
        latest_drop_call_date: string | null; // or Date
        max_out_bound_call_date: string | null; // or Date
        outbound_after_drop: 'yes' | 'no';
    }]
    """
    leadBankData = []
    while leadBankInnerOffset < leadBankDataCount:
        leadBankOuterOffset = 0
        while True:
            query = util.getLeadBankQuery(leadBankOuterLimit, leadBankOuterOffset,leadBankInnerLimit,leadBankInnerOffset )
            queryResult = util.executeQuery(query)
            print(f"innerOffset={leadBankInnerOffset}, outerOffset={leadBankOuterOffset} fetched={len(queryResult)}")
            leadBankData.extend(queryResult)
            print("breaking")
            break
        leadBankInnerOffset += leadBankInnerLimit

    print(f"leadBankData Size = {len(leadBankData)}")
    leadStatusOuterLimit = 2000
    leadStatusInnerLimit = 2000
    leadStatusInnerOffset = 0
    
    """
    [{
        phone: string;
        max_call_date: string | null; // or Date
        last_call_status: string | null;
    }]
    """
    allLeadStatusData = []

    print('Fetching Lead status data...')
    while leadStatusInnerOffset < leadBankDataCount:
        leadStatusOuterOffset = 0
        while True:
            leadBankStatusQuery = util.getLeadStatusQuery(leadStatusInnerOffset, leadStatusInnerLimit, leadStatusOuterLimit, leadStatusOuterOffset)
            leadStatusData = util.executeQuery(leadBankStatusQuery)
            print(f"innerOffset={leadStatusInnerOffset}, outerOffset={leadStatusOuterOffset}, fetched={len(leadStatusData)}")
            if not leadStatusData:
                break
            allLeadStatusData.extend(leadStatusData)
            # if len(leadStatusData) < leadStatusOuterLimit:
            #     break
            # leadStatusOuterOffset += leadStatusOuterLimit
            break;
        leadStatusInnerOffset += leadStatusInnerLimit

    print(f"Total Lead Status Data fetched: {len(allLeadStatusData)}")


    print('Fetching Calling History Stage Score data...')
    callingHistoryOuterLimit = 2000
    callingHistoryInnerLimit = 2000
    callingHistoryInnerOffset = 0
    
    """
    [{
        phone_number_dialed: string;
        max_score: number; 
    }]
    """
    allCallingHistoryStageScoreData = []

    while callingHistoryInnerOffset < leadBankDataCount:
        callingHistoryOuterOffset = 0
        while True:
            callingHistoryStageScoreQuery = util.getCallingHistoryStageScore(
                callingHistoryInnerOffset,
                callingHistoryInnerLimit,
                callingHistoryOuterLimit,
                callingHistoryOuterOffset
            )
            callingHistoryStageScoreData = util.executeQuery(callingHistoryStageScoreQuery)
            print(f"innerOffset={callingHistoryInnerOffset}, outerOffset={callingHistoryOuterOffset}, fetched={len(callingHistoryStageScoreData)}")
            if not callingHistoryStageScoreData:
                break
            allCallingHistoryStageScoreData.extend(callingHistoryStageScoreData)
            # if len(callingHistoryStageScoreData) < callingHistoryOuterLimit:
                # break
            # callingHistoryOuterOffset += callingHistoryOuterLimit
            break;
        callingHistoryInnerOffset += callingHistoryInnerLimit

    print(f"Total Calling History Stage Score Data fetched: {len(allCallingHistoryStageScoreData)}")

    print('Fetching Call History With Dialed Count data...')
    callHistoryOuterLimit = 2000
    callHistoryInnerLimit = 2000
    callHistoryInnerOffset = 0
    
    """
    [{
        phone_number_dialed: string;
        status_summary: string | null;  
    }]
    """
    allCallHistoryWithDialedCountData = []

    while callHistoryInnerOffset < leadBankDataCount:
        callHistoryOuterOffset = 0
        while True:
            callHistoryWithDialedCountQuery = util.getCallHistoryWithDialedCount(
                callHistoryInnerOffset,
                callHistoryInnerLimit,
                callHistoryOuterLimit,
                callHistoryOuterOffset
            )
            callHistoryWithDialedCountData = util.executeQuery(callHistoryWithDialedCountQuery)
            print(f"innerOffset={callHistoryInnerOffset}, outerOffset={callHistoryOuterOffset}, fetched={len(callHistoryWithDialedCountData)}")
            if not callHistoryWithDialedCountData:
                break
            allCallHistoryWithDialedCountData.extend(callHistoryWithDialedCountData)
            # if len(callHistoryWithDialedCountData) < callHistoryOuterLimit:
            #     break
            # callHistoryOuterOffset += callHistoryOuterLimit
            break;
        callHistoryInnerOffset += callHistoryInnerLimit

    print(f"Total Call History With Dialed Count Data fetched: {len(allCallHistoryWithDialedCountData)}")


    leadBankDF = pd.DataFrame(leadBankData)
    leadBankData = None
    stageMapDF = pd.DataFrame(stageMapData)
    stageMapData = None
    allCallHistoryWithDialedCountDataDF = pd.DataFrame(allCallHistoryWithDialedCountData)
    allCallHistoryWithDialedCountData = None    
    allCallingHistoryStageScoreDataDF = pd.DataFrame(allCallingHistoryStageScoreData)
    allCallingHistoryStageScoreData = None
    allLeadStatusDataDF = pd.DataFrame(allLeadStatusData)
    allLeadStatusData = None
    
    print("!!! Data fetched successfully !!!")
    
    if 'uploaded_date' in leadBankDF.columns:
        leadBankDF['uploaded_date'] = pd.to_datetime(leadBankDF['uploaded_date'], errors='coerce').dt.strftime('%d/%m/%Y')

    # Merge all dataframes on phone fields
    # Rename phone_number_dialed to phone for merging
    allCallHistoryWithDialedCountDataDF = allCallHistoryWithDialedCountDataDF.rename(columns={'phone_number_dialed': 'phone'})
    allCallingHistoryStageScoreDataDF = allCallingHistoryStageScoreDataDF.rename(columns={'phone_number_dialed': 'phone'})

    # Merge leadBankDF with allLeadStatusDataDF
    mergedDF = pd.merge(leadBankDF, allLeadStatusDataDF, on='phone', how='left')
    # Merge with allCallingHistoryStageScoreDataDF
    mergedDF = pd.merge(mergedDF, allCallingHistoryStageScoreDataDF, on='phone', how='left')
    # Merge with allCallHistoryWithDialedCountDataDF
    mergedDF = pd.merge(mergedDF, allCallHistoryWithDialedCountDataDF, on='phone', how='left')
    # Save mergedDF to Google Sheet tab
    # Convert DataFrame to list of lists (including header)
    data_to_save = [mergedDF.columns.tolist()] + mergedDF.fillna("").astype(str).values.tolist()
    print("Clearning data on google Sheet tab...")
    leadBankTableTab.clear()
    print("Saving data to Google Sheet tab...")
    
    batch_size = 100000
    total_rows = len(data_to_save)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        print(f"Updating rows {start+1} to {end}...")
        leadBankTableTab.update(data_to_save[start:end], f"A{start+1}")
    print("✅ Data saved to Google Sheet tab successfully!")



    endtime = time.perf_counter()
    print(f"Total execution time: {endtime - startTime} seconds")

main()


# Question with harshit
# 1. clear all data and paste new data every time
