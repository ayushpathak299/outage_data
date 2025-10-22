import requests
import time
import json
import psycopg2
from datetime import datetime, timedelta
from datetime import date
import sys

class OutageDataBulkETL:
    def __init__(self):
        # Get OAuth token
        self.get_auth_token()
        
        # Database connection
        self.setup_database()
        
    def get_auth_token(self):
        """Get OAuth token from Zoho"""
        url = "https://accounts.zoho.com/oauth/v2/token"
        payload = "client_id=1000.F3ECHYKUK9ASR29PZ3RRKU5H8EE9UJ&client_secret=583fc4a3dd3aed419a479395ad32c0fb168632af94&refresh_token=1000.24a7e879923148a3c8c758c890a4d646.58bcfb73c2395339b0e0a3100de8de1a&grant_type=refresh_token"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        r = response.json()
        self.myToken = r.get('access_token')
        print(f"✅ Auth Token obtained: {self.myToken[:20]}...")
        
    def setup_database(self):
        """Setup database connection"""
        try:
            self.connection = psycopg2.connect(
                host="jira-redash.c5ditj8vhg0k.us-west-1.rds.amazonaws.com",
                database="jira",
                user="redash",
                password="N6ZrFz8KdR",
                port="5432"
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
            
    def generate_date_ranges(self, start_date, end_date, chunk_days=1):
        """Generate date ranges for processing"""
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        date_ranges = []
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days), end)
            date_ranges.append({
                'start': current.strftime("%Y-%m-%d"),
                'end': chunk_end.strftime("%Y-%m-%d")
            })
            current = chunk_end + timedelta(days=1)
            
        return date_ranges
    
    def fetch_outage_data(self, start_date, end_date):
        """Fetch outage data for a specific date range"""
        url = f"https://www.site24x7.com/api/reports/outage?period=50&start_date={start_date}&end_date={end_date}"
        headers = {
            'Authorization': f"Bearer {self.myToken}",
            'Content-Type': "application/json",
            'cache-control': "no-cache",
            'Accept-Charset': "UTF-8",
            'Version': "2.0",
        }
        
        try:
            response = requests.request("GET", url, headers=headers)
            
            if response.status_code != 200:
                print(f"❌ API Error - Status Code: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
            data = json.loads(response.content.decode('utf-8'))
            
            if data.get("code") != 0:
                print(f"❌ API returned error: {data.get('message', 'Unknown error')}")
                return None
                
            outage_details = data["data"]["outage_details"]
            print(f"📥 Fetched {len(outage_details)} monitors with outages for {start_date} to {end_date}")
            return outage_details
            
        except Exception as e:
            print(f"❌ Error fetching data for {start_date}-{end_date}: {e}")
            return None
    
    def process_outage_data(self, outage_details, date_range):
        """Process and insert outage data into database"""
        if not outage_details:
            print(f"⚠️  No outage data for {date_range['start']} to {date_range['end']}")
            return 0, 0
            
        sqlquery = """INSERT INTO public.outage_data(monitor_name, outage_id, outages_end_time, outages_start_time, outages_duration, monitor_id, outage_type)
                      VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        
        total_monitors = len(outage_details)
        total_outages = 0
        successful_inserts = 0
        
        for monitor_idx, item in enumerate(outage_details, 1):
            display_name = item['display_name']
            monitor_id = item['monitor_id']
            outages = item['outages']
            
            print(f"  📊 Processing monitor {monitor_idx}/{total_monitors}: {display_name}")
            print(f"      Monitor ID: {monitor_id}, Outages: {len(outages)}")
            
            monitor_insertqueries = []
            
            for outage_idx, outage in enumerate(outages, 1):
                try:
                    outage_id = outage['outage_id']
                    start_time = outage['start_time']
                    end_time = outage['end_time']
                    duration = outage['duration']
                    outage_type = int(outage['type'])
                    
                    # FIXED: Correct column order - (monitor_name, outage_id, END_time, START_time, duration, monitor_id, outage_type)
                    templist = (display_name, outage_id, end_time, start_time, duration, monitor_id, outage_type)
                    monitor_insertqueries.append(templist)
                    total_outages += 1
                    
                    print(f"        Outage {outage_idx}: {start_time} to {end_time} (ID: {outage_id})")
                    
                except Exception as e:
                    print(f"        ❌ Error processing outage {outage_idx}: {e}")
                    continue
            
            # Insert outages for this monitor
            if monitor_insertqueries:
                try:
                    self.cursor.executemany(sqlquery, monitor_insertqueries)
                    self.connection.commit()
                    successful_inserts += len(monitor_insertqueries)
                    print(f"      ✅ SUCCESS: {len(monitor_insertqueries)} outage records inserted")
                except Exception as e:
                    print(f"      ❌ DATABASE ERROR for '{display_name}': {e}")
                    self.connection.rollback()
            else:
                print(f"      ⚠️  No valid outage records for {display_name}")
        
        return total_monitors, successful_inserts
    
    def run_bulk_etl(self, start_date="2025-07-12", end_date="2025-09-20"):
        """Run the bulk ETL process"""
        print(f"🚀 Starting Bulk ETL Process")
        print(f"📅 Date Range: {start_date} to {end_date}")
        print("="*60)
        
        # Generate date ranges (processing 1 day at a time to avoid API timeouts)
        date_ranges = self.generate_date_ranges(start_date, end_date, chunk_days=1)
        total_ranges = len(date_ranges)
        
        print(f"📋 Generated {total_ranges} date ranges to process")
        
        # Overall statistics
        total_monitors_processed = 0
        total_outages_inserted = 0
        failed_ranges = []
        
        for range_idx, date_range in enumerate(date_ranges, 1):
            print(f"\n🔄 Processing range {range_idx}/{total_ranges}: {date_range['start']} to {date_range['end']}")
            
            # Fetch data for this date range
            outage_data = self.fetch_outage_data(date_range['start'], date_range['end'])
            
            if outage_data is None:
                print(f"⚠️  Failed to fetch data for {date_range['start']}-{date_range['end']}")
                failed_ranges.append(date_range)
                continue
            
            # Process the data
            monitors_count, outages_count = self.process_outage_data(outage_data, date_range)
            total_monitors_processed += monitors_count
            total_outages_inserted += outages_count
            
            print(f"✅ Completed {date_range['start']}-{date_range['end']}: {monitors_count} monitors, {outages_count} outages inserted")
            
            # Small delay to avoid API rate limiting
            time.sleep(0.5)
        
        # Final summary
        print("\n" + "="*60)
        print("🎉 BULK ETL PROCESS COMPLETED")
        print("="*60)
        print(f"📊 Total date ranges processed: {total_ranges - len(failed_ranges)}/{total_ranges}")
        print(f"📊 Total monitors processed: {total_monitors_processed}")
        print(f"📊 Total outage records inserted: {total_outages_inserted}")
        
        if failed_ranges:
            print(f"⚠️  Failed ranges ({len(failed_ranges)}):")
            for failed_range in failed_ranges:
                print(f"    - {failed_range['start']} to {failed_range['end']}")
        
        self.connection.close()
        print("✅ Database connection closed")

if __name__ == "__main__":
    etl = OutageDataBulkETL()
    etl.run_bulk_etl(start_date="2025-07-12", end_date="2025-09-20")
