import requests
import time
import json
import psycopg2
import sys
import os
from datetime import datetime, timedelta
from datetime import date

class DailyOutageETL:
    def __init__(self):
        # Get OAuth token
        self.get_auth_token()
        
        # Database connection
        self.setup_database()
        
    def get_auth_token(self):
        """Get OAuth token from Zoho"""
        try:
            url = "https://accounts.zoho.com/oauth/v2/token"
            
            # Use environment variables for security (fallback to hardcoded for now)
            client_id = os.getenv('ZOHO_CLIENT_ID', '1000.F3ECHYKUK9ASR29PZ3RRKU5H8EE9UJ')
            client_secret = os.getenv('ZOHO_CLIENT_SECRET', '583fc4a3dd3aed419a479395ad32c0fb168632af94')
            refresh_token = os.getenv('ZOHO_REFRESH_TOKEN', '1000.24a7e879923148a3c8c758c890a4d646.58bcfb73c2395339b0e0a3100de8de1a')
            
            payload = f"client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}&grant_type=refresh_token"
            headers = {
                'Content-Type': "application/x-www-form-urlencoded",
                'cache-control': "no-cache"
            }
            response = requests.request("POST", url, data=payload, headers=headers)
            r = response.json()
            
            if 'access_token' not in r:
                print(f"❌ Auth Error: {r}")
                sys.exit(1)
                
            self.myToken = r.get('access_token')
            print(f"✅ Auth Token obtained successfully")
            
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            sys.exit(1)
        
    def setup_database(self):
        """Setup database connection"""
        try:
            # Use environment variables for database credentials (fallback to hardcoded)
            db_host = os.getenv('DB_HOST', 'jira-redash.c5ditj8vhg0k.us-west-1.rds.amazonaws.com')
            db_name = os.getenv('DB_NAME', 'jira')
            db_user = os.getenv('DB_USER', 'redash')
            db_password = os.getenv('DB_PASSWORD', 'N6ZrFz8KdR')
            db_port = os.getenv('DB_PORT', '5432')
            
            self.connection = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password,
                port=db_port
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
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
            print(f"📡 Fetching data from API: {start_date} to {end_date}")
            response = requests.request("GET", url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                print(f"❌ API Error - Status Code: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
            data = json.loads(response.content.decode('utf-8'))
            
            if data.get("code") != 0:
                print(f"❌ API returned error: {data.get('message', 'Unknown error')}")
                return None
                
            outage_details = data["data"]["outage_details"]
            print(f"📥 Successfully fetched {len(outage_details)} monitors with outages")
            return outage_details
            
        except requests.exceptions.Timeout:
            print(f"❌ API request timeout for {start_date}-{end_date}")
            return None
        except Exception as e:
            print(f"❌ Error fetching data for {start_date}-{end_date}: {e}")
            return None
    
    def check_existing_data(self, start_date, end_date):
        """Check if data already exists for the date range"""
        try:
            query = """
                SELECT COUNT(*) 
                FROM outage_data 
                WHERE DATE(outages_start_time) >= %s 
                  AND DATE(outages_start_time) <= %s
            """
            self.cursor.execute(query, (start_date, end_date))
            count = self.cursor.fetchone()[0]
            
            if count > 0:
                print(f"⚠️  Found {count} existing records for {start_date} to {end_date}")
                return count
            return 0
            
        except Exception as e:
            print(f"❌ Error checking existing data: {e}")
            return 0
    
    def process_outage_data(self, outage_details, date_range):
        """Process and insert outage data into database"""
        if not outage_details:
            print(f"ℹ️  No outage data to process for {date_range}")
            return 0, 0
            
        sqlquery = """INSERT INTO public.outage_data(monitor_name, outage_id, outages_end_time, outages_start_time, outages_duration, monitor_id, outage_type)
                      VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        
        total_monitors = len(outage_details)
        total_outages = 0
        successful_inserts = 0
        
        print(f"🔄 Processing {total_monitors} monitors...")
        
        for monitor_idx, item in enumerate(outage_details, 1):
            try:
                display_name = item['display_name']
                monitor_id = item['monitor_id']
                outages = item['outages']
                
                if monitor_idx <= 5:  # Show details for first 5 monitors
                    print(f"  📊 Monitor {monitor_idx}/{total_monitors}: {display_name} ({len(outages)} outages)")
                elif monitor_idx == 6 and total_monitors > 5:
                    print(f"  ... and {total_monitors - 5} more monitors")
                
                monitor_insertqueries = []
                
                for outage in outages:
                    try:
                        outage_id = outage['outage_id']
                        start_time = outage['start_time']
                        end_time = outage['end_time']
                        duration = outage['duration']
                        outage_type = int(outage['type'])
                        
                        # CORRECT column order: (monitor_name, outage_id, END_time, START_time, duration, monitor_id, outage_type)
                        templist = (display_name, outage_id, end_time, start_time, duration, monitor_id, outage_type)
                        monitor_insertqueries.append(templist)
                        total_outages += 1
                        
                    except Exception as e:
                        print(f"      ❌ Error processing outage: {e}")
                        continue
                
                # Insert outages for this monitor
                if monitor_insertqueries:
                    try:
                        self.cursor.executemany(sqlquery, monitor_insertqueries)
                        self.connection.commit()
                        successful_inserts += len(monitor_insertqueries)
                    except psycopg2.errors.UniqueViolation:
                        # Skip duplicates - this is expected for re-runs
                        print(f"      ℹ️  Skipped {len(monitor_insertqueries)} duplicate records for {display_name}")
                        self.connection.rollback()
                    except Exception as e:
                        print(f"      ❌ Database error for '{display_name}': {e}")
                        self.connection.rollback()
                        
            except Exception as e:
                print(f"    ❌ Error processing monitor {monitor_idx}: {e}")
                continue
        
        return total_monitors, successful_inserts
    
    def run_daily_etl(self, target_date=None):
        """Run ETL for a specific date (defaults to yesterday)"""
        
        # Default to yesterday if no date provided
        if target_date is None:
            yesterday = datetime.now() - timedelta(days=1)
            target_date = yesterday.strftime("%Y-%m-%d")
        
        print(f"🚀 Starting Daily ETL for {target_date}")
        print(f"⏰ ETL started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # Check if data already exists
        existing_count = self.check_existing_data(target_date, target_date)
        if existing_count > 0:
            print(f"ℹ️  Found {existing_count} existing records for {target_date}")
            # In automation mode, skip prompting and continue
            if os.getenv('GITHUB_ACTIONS') == 'true':
                print("🤖 Running in GitHub Actions - skipping duplicate check")
            else:
                user_input = input(f"Data already exists for {target_date}. Continue anyway? (y/N): ")
                if user_input.lower() != 'y':
                    print("❌ ETL cancelled by user")
                    return False
        
        # Fetch data for the target date
        outage_data = self.fetch_outage_data(target_date, target_date)
        
        if outage_data is None:
            print(f"❌ Failed to fetch data for {target_date}")
            return False
        
        if len(outage_data) == 0:
            print(f"ℹ️  No outages found for {target_date}")
            return True
        
        # Process the data
        monitors_count, outages_count = self.process_outage_data(outage_data, target_date)
        
        # Final summary
        print("\n" + "="*60)
        print("🎉 DAILY ETL COMPLETED")
        print("="*60)
        print(f"📅 Date processed: {target_date}")
        print(f"📊 Monitors processed: {monitors_count}")
        print(f"📊 Outage records inserted: {outages_count}")
        print(f"⏰ ETL completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.connection.close()
        print("✅ Database connection closed")
        
        return True

def main():
    """Main function to handle command line arguments"""
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        # Custom date provided
        target_date = sys.argv[1]
        try:
            # Validate date format
            datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Default to yesterday
        target_date = None
    
    try:
        etl = DailyOutageETL()
        success = etl.run_daily_etl(target_date)
        
        if success:
            print("✅ ETL completed successfully")
            sys.exit(0)
        else:
            print("❌ ETL failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n❌ ETL interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

