import requests
import time
import json
#import urllib.request
import psycopg2
from datetime import datetime, timedelta
from datetime import date

class OutageData:
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = "client_id=1000.F3ECHYKUK9ASR29PZ3RRKU5H8EE9UJ&client_secret=583fc4a3dd3aed419a479395ad32c0fb168632af94&refresh_token=1000.24a7e879923148a3c8c758c890a4d646.58bcfb73c2395339b0e0a3100de8de1a&grant_type=refresh_token"
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    r = response.json()
    myToken = r.get('access_token')
    print(myToken)
    # startdate = date.today() - timedelta(1)
    # enddate = date.today() - timedelta(1)
    # Test different date ranges to find missing shard monitors
    startdate ="2025-10-21"
    enddate="2025-10-22"

    url = "https://www.site24x7.com/api/reports/outage?period=50&start_date={}&end_date={}".format(startdate,enddate)
    #payload = "period=50&start_date="startdate"&end_date="enddate"
    headers = {
            'Authorization' : "Bearer " +myToken ,
            'Content-Type': "application/json",
            'cache-control': "no-cache",
            'Accept-Charset' : "UTF-8",
            'Version' : "2.0",
              }
    print (url)
    print(startdate)
    print(enddate)
    responseobj = requests.request("GET", url, headers=headers)
    #print (response.json())
    dataobj = json.loads(responseobj.content.decode('utf-8'))
    
    # Debug: Print full API response to understand what's being returned
    print("\n=== FULL API RESPONSE DEBUG ===")
    print(json.dumps(dataobj, indent=2))
    print("=== END API RESPONSE ===\n")
    
    data = dataobj["data"]
    outage_details = data["outage_details"]
    
    # Debug: Show what monitors are being returned
    print(f"API returned {len(outage_details)} monitors with outages:")
    for i, item in enumerate(outage_details):
        print(f"  {i+1}. Monitor: '{item['display_name']}' (ID: {item['monitor_id']}) - {len(item['outages'])} outages")
        # Check if it's a shard monitor
        if 'shard' in item['display_name'].lower():
            print(f"      ^^ This is a SHARD monitor")
    
    print(f"\nStarting database insertion for {len(outage_details)} monitors...\n")
    insertqueries=[]
    templist = ()

    connection = psycopg2.connect(
        host="jira-redash.c5ditj8vhg0k.us-west-1.rds.amazonaws.com",
        database="jira",
        user="redash",
        password="N6ZrFz8KdR",
        port = "5432"
    )
    connection.autocommit = True
    cursor = connection.cursor()
    sqlquery = """ INSERT INTO public.outage_data(monitor_name, outage_id, outages_end_time,outages_start_time,outages_duration,monitor_id,outage_type)
                           VALUES (%s,%s,%s,%s,%s,%s,%s) """
    for item in outage_details:
        display_name = item['display_name']
        print(f"\nProcessing monitor: {display_name}")
        monitor_id = item['monitor_id']
        outages = item['outages']
        
        monitor_insertqueries = []
        
        for outage in outages:
            outage_id = outage['outage_id']
            start_time = outage['start_time']
            print(f"  Outage start_time: {start_time}")
            outage_type = outage['type']
            end_time = outage['end_time']
            duration = outage['duration']
            templist = (display_name,outage_id,end_time,start_time,duration,monitor_id,int(outage_type))
            monitor_insertqueries.append(templist)
            print(f"  Prepared for insertion: {templist}")

        # Insert data for this specific monitor
        try:
            result = cursor.executemany(sqlquery, monitor_insertqueries)
            connection.commit()
            print(f"  ✅ SUCCESS: {cursor.rowcount} records inserted for '{display_name}'")
        except Exception as e:
            print(f"  ❌ DATABASE ERROR for '{display_name}': {e}")
            print(f"  Data that failed: {monitor_insertqueries}")
            connection.rollback()
    connection.close()