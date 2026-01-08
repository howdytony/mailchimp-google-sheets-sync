import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

# ============================================
# CONFIGURATION - FILL THESE IN
# ============================================
MAILCHIMP_API_KEY = "edd024360a7ee99d25b6352ea1316052-us1"
MAILCHIMP_SERVER = "us1"
MAILCHIMP_LIST_ID = "3613ca0378"

GOOGLE_CREDENTIALS_FILE = "C:/Users/Anthony/Desktop/mailchimp-audience-growth-automation/gethsemane-1756307666674-8cc15e9b111b.json"
GOOGLE_SHEET_NAME = "MailChimp Year-Over-Year Audience Growth"
WORKSHEET_NAME = "RAW DATA"

# ============================================
# STEP 1: Connect to Mailchimp
# ============================================
print("Connecting to Mailchimp...")
try:
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": MAILCHIMP_API_KEY,
        "server": MAILCHIMP_SERVER
    })
    # Test connection with a simple ping
    client.ping.get()
    print("✓ Successfully connected to Mailchimp")
except ApiClientError as error:
    print(f"✗ Error connecting to Mailchimp: {error.text}")
    print("Check your API key and server prefix")
    exit()
except Exception as error:
    print(f"✗ Unexpected error: {error}")
    exit()

# ============================================
# STEP 2: Fetch all subscribers
# ============================================
print("Fetching subscribers...")
all_members = []
offset = 0
count = 1000  # Max per request

try:
    while True:
        response = client.lists.get_list_members_info(
            MAILCHIMP_LIST_ID,
            count=count,
            offset=offset,
            status="subscribed"
        )
        
        members = response['members']
        if not members:
            break
            
        all_members.extend(members)
        offset += count
        print(f"  Fetched {len(all_members)} subscribers so far...")
        
        # Small delay to avoid rate limiting
        if len(members) == count:
            time.sleep(0.1)
        
        if len(members) < count:
            break
            
except ApiClientError as error:
    print(f"✗ Error fetching subscribers: {error.text}")
    if "404" in str(error.text):
        print("Check your List ID - it may be incorrect")
    exit()
except Exception as error:
    print(f"✗ Unexpected error: {error}")
    exit()

if len(all_members) == 0:
    print("✗ No subscribers found in this list")
    print("Check your List ID and make sure you have subscribers")
    exit()

print(f"✓ Total subscribers fetched: {len(all_members)}")

# ============================================
# STEP 3: Process the data
# ============================================
print("Processing data...")
subscriber_data = []
skipped_records = []

for member in all_members:
    email = member['email_address']
    optin_time = member.get('timestamp_opt', '')
    
    # Skip if no opt-in time
    if not optin_time:
        skipped_records.append({'email': email, 'reason': 'No opt-in timestamp'})
        continue
    
    # Parse the timestamp
    try:
        dt = datetime.strptime(optin_time, '%Y-%m-%dT%H:%M:%S%z')
        
        subscriber_data.append({
            'email': email,
            'optin_time': dt.strftime('%m/%d/%Y %H:%M'),
            'sort_date': dt.strftime('%Y-%m'),
            'month_only': dt.strftime('%m-%b'),
            'year': dt.strftime('%Y')
        })
    except Exception as e:
        skipped_records.append({'email': email, 'reason': f'Invalid date format: {optin_time}'})
        continue

# Validate we have data to process
if len(subscriber_data) == 0:
    print("✗ No subscribers with valid opt-in dates found")
    print("Cannot proceed - your sheet will not be updated")
    exit()

# Create DataFrame
df = pd.DataFrame(subscriber_data)

# Sort by date
df = df.sort_values('optin_time')

print(f"✓ Processed {len(df)} subscribers with valid dates")

# Report skipped records
if len(skipped_records) > 0:
    print(f"⚠ Warning: Skipped {len(skipped_records)} subscribers due to missing/invalid dates")
    if len(skipped_records) <= 10:
        print("  Skipped records:")
        for record in skipped_records[:10]:
            print(f"    - {record['email']}: {record['reason']}")
    else:
        print(f"  (Showing first 10 of {len(skipped_records)} skipped)")
        for record in skipped_records[:10]:
            print(f"    - {record['email']}: {record['reason']}")

# ============================================
# STEP 4: Calculate year-over-year summary
# ============================================
print("Calculating year-over-year metrics...")

# Count by month and year
pivot_data = df.groupby(['month_only', 'year']).size().reset_index(name='count')
pivot_table = pivot_data.pivot(index='month_only', columns='year', values='count').fillna(0)

# Calculate totals by year
yearly_totals = df.groupby('year').size()

# Calculate growth rates
growth_rates = {}
years = sorted(yearly_totals.index)
for i in range(1, len(years)):
    prev_year = years[i-1]
    curr_year = years[i]
    growth = ((yearly_totals[curr_year] - yearly_totals[prev_year]) / yearly_totals[prev_year] * 100)
    growth_rates[f"{curr_year} vs {prev_year}"] = f"{growth:.1f}%"

print("Year-over-year growth:")
for comparison, rate in growth_rates.items():
    print(f"  {comparison}: {rate}")

# ============================================
# STEP 5: Connect to Google Sheets
# ============================================
print("Connecting to Google Sheets...")
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

try:
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_CREDENTIALS_FILE, scope)
    gc = gspread.authorize(credentials)
    print("✓ Authenticated with Google")
except FileNotFoundError:
    print(f"✗ Error: Could not find credentials file at:")
    print(f"  {GOOGLE_CREDENTIALS_FILE}")
    print("Make sure the path is correct")
    exit()
except Exception as error:
    print(f"✗ Error with Google credentials: {error}")
    exit()

# Open the sheet
try:
    sheet = gc.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
    print(f"✓ Found sheet: '{GOOGLE_SHEET_NAME}' / '{WORKSHEET_NAME}'")
except gspread.SpreadsheetNotFound:
    print(f"✗ Error: Could not find spreadsheet '{GOOGLE_SHEET_NAME}'")
    print("Make sure:")
    print("  1. The sheet name is exactly correct (case-sensitive)")
    print("  2. You've shared the sheet with your service account email")
    print(f"     (Check the 'client_email' in {GOOGLE_CREDENTIALS_FILE})")
    exit()
except gspread.WorksheetNotFound:
    print(f"✗ Error: Could not find worksheet '{WORKSHEET_NAME}'")
    print("Make sure the tab name is exactly correct (case-sensitive)")
    exit()
except Exception as error:
    print(f"✗ Unexpected error accessing sheet: {error}")
    exit()

# ============================================
# STEP 6: Update Google Sheets
# ============================================
print("Updating Google Sheet...")

# Final validation before clearing
if len(df) < 10:
    print(f"⚠ Warning: Only {len(df)} records found - this seems low")
    response = input("Continue anyway? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted - sheet not updated")
        exit()

try:
    # Clear existing data
    sheet.clear()
    print("  Cleared existing data")
    
    # Write headers
    headers = ['Email', 'Opt-in Time', 'Sort Date', 'Month Only', 'Year']
    sheet.update('A1:E1', [headers])
    print("  Wrote headers")
    
    # Write data in batches (Google Sheets API limit is 10M cells per request)
    if len(df) > 0:
        data_to_write = df[['email', 'optin_time', 'sort_date', 'month_only', 'year']].values.tolist()
        
        # Write in batches of 1000 rows to avoid timeouts
        batch_size = 1000
        for i in range(0, len(data_to_write), batch_size):
            batch = data_to_write[i:i+batch_size]
            start_row = i + 2  # +2 because row 1 is headers, and we're 0-indexed
            end_row = start_row + len(batch) - 1
            sheet.update(f'A{start_row}:E{end_row}', batch)
            print(f"  Wrote rows {start_row} to {end_row}")
            
            # Small delay between batches
            if i + batch_size < len(data_to_write):
                time.sleep(0.5)
    
    print(f"✓ Successfully updated {len(df)} rows in Google Sheet")
    print(f"✓ Sheet URL: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}")
    
except Exception as error:
    print(f"✗ Error writing to Google Sheets: {error}")
    print("Your data may be partially updated - check the sheet")
    exit()

print("\n" + "="*50)
print("SYNC COMPLETE!")
print("="*50)
print("\nNext steps:")
print("1. Go to your pivot table tab")
print("2. Click Data > Refresh (or right-click > Refresh)")
print("3. Your year-over-year comparison should now be updated")
print("\nTip: Save this output for your records")