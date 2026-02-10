#!/usr/bin/env python3
"""
ACES Power Price Scraper - API-based download approach
"""

import os
import time
import re
import requests
from pathlib import Path
from datetime import datetime
from supabase import create_client
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

# Configuration
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
ACES_USER = os.environ.get('ACES_USERNAME')
ACES_PASS = os.environ.get('ACES_PASSWORD')

def init_browser():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.implicitly_wait(5)
    return driver

def login(driver):
    print("Logging in...")
    driver.get("https://de.acespower.com/Web/Account/Login.htm")
    time.sleep(3)
    
    driver.find_element(By.NAME, "username").send_keys(ACES_USER)
    driver.find_element(By.NAME, "password").send_keys(ACES_PASS)
    
    try:
        driver.find_element(By.ID, "loginSubmit").click()
    except:
        driver.find_element(By.NAME, "password").submit()
    
    time.sleep(5)
    current_url = driver.current_url
    print(f"URL after login: {current_url}")
    
    if "Login" in current_url:
        raise Exception("Login failed")
    print("Login successful!")
    return True

def get_processed_files(supabase):
    try:
        response = supabase.table('processed_files').select('filename').execute()
        return set([f['filename'] for f in response.data])
    except Exception as e:
        print(f"Error fetching processed files: {e}")
        return set()

def scan_files(driver):
    print("Scanning for files...")
    if "/#/" not in driver.current_url:
        driver.get("https://de.acespower.com#/")
        time.sleep(3)
    
    # Scroll to load all
    for i in range(5):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
    
    files = driver.execute_script("""
        var results = [];
        document.querySelectorAll('tr').forEach(function(row) {
            var text = row.textContent || '';
            var match = text.match(/(NIPS\\.WVPA_(da|rt)_price_forecast_(\\d{14})\\.csv)/);
            if (match) {
                results.push({filename: match[1], type: match[2], version: match[3]});
            }
        });
        return results;
    """)
    
    seen = set()
    unique = []
    for f in files:
        if f['filename'] not in seen:
            seen.add(f['filename'])
            unique.append(f)
    
    print(f"Found {len(unique)} unique files")
    return unique

def download_file_api(driver, filename):
    """
    Strategy: Use browser to get cookies/session, then fetch file via HTTP request
    """
    print(f"  Attempting download via API: {filename}")
    
    # Get cookies from selenium
    cookies = driver.get_cookies()
    cookie_dict = {c['name']: c['value'] for c in cookies}
    print(f"  Got {len(cookies)} cookies")
    
    # Try to find download URL or construct it
    # Common patterns for file download APIs
    encoded_filename = requests.utils.quote(filename)
    
    possible_urls = [
        f"https://de.acespower.com/api/files/download/{encoded_filename}",
        f"https://de.acespower.com/api/download?file={encoded_filename}",
        f"https://de.acespower.com/download/{encoded_filename}",
        f"https://de.acespower.com/files/{encoded_filename}",
        f"https://de.acespower.com/api/v1/files/{encoded_filename}",
    ]
    
    # Try to intercept the actual download URL by clicking and monitoring
    print("  Trying to intercept download URL...")
    
    # Click the file to see if we can catch the URL
    try:
        # Execute click and check network (we can't really do this in Selenium easily)
        # Instead, let's try to find any links or buttons with download URLs
        download_url = driver.execute_script("""
            var rows = document.querySelectorAll('tr');
            for (var i = 0; i < rows.length; i++) {
                if (rows[i].textContent.includes(arguments[0])) {
                    // Look for download links
                    var links = rows[i].querySelectorAll('a[href], button[data-url]');
                    for (var j = 0; j < links.length; j++) {
                        var url = links[j].getAttribute('href') || links[j].getAttribute('data-url') || '';
                        if (url && (url.includes('.csv') || url.includes('download'))) {
                            return url;
                        }
                    }
                    // Try onclick handlers
                    var onclick = links[j].getAttribute('onclick') || '';
                    var match = onclick.match(/['\"]([^'\"]*download[^'\"]*)['\"]/);
                    if (match) return match[1];
                }
            }
            return null;
        """, filename)
        
        if download_url:
            print(f"  Found download URL: {download_url}")
            if not download_url.startswith('http'):
                download_url = f"https://de.acespower.com{download_url}"
            possible_urls.insert(0, download_url)
    except Exception as e:
        print(f"  Could not intercept URL: {e}")
    
    # Try each URL
    session = requests.Session()
    for c in cookies:
        session.cookies.set(c['name'], c['value'])
    
    for url in possible_urls:
        print(f"  Trying: {url[:80]}...")
        try:
            response = session.get(url, timeout=30, allow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if 'csv' in content_type or filename in response.headers.get('content-disposition', ''):
                    print(f"  ✓ Downloaded {len(response.content)} bytes")
                    return response.content
                elif len(response.content) > 100 and b',' in response.content[:1000]:
                    # Likely CSV even if content-type is wrong
                    print(f"  ✓ Downloaded {len(response.content)} bytes (detected CSV)")
                    return response.content
        except Exception as e:
            print(f"    Failed: {e}")
            continue
    
    raise Exception("Could not download file via any URL")

def parse_filename(filename):
    match = re.match(r'NIPS\.WVPA_(da|rt)_price_forecast_(\d{14})\.csv', filename)
    if match:
        version_str = match.group(2)
        forecast_time = datetime(
            int(version_str[0:4]), int(version_str[4:6]), int(version_str[6:8]),
            int(version_str[8:10]), int(version_str[10:12]), int(version_str[12:14])
        )
        return {
            'type': match.group(1),
            'version': int(version_str),
            'forecast_timestamp': forecast_time
        }
    return None

def process_csv_content(content, file_info):
    """Process CSV from bytes"""
    try:
        # Save to temp file for pandas
        temp_path = Path('/tmp') / file_info['filename']
        temp_path.write_bytes(content)
        
        df = pd.read_csv(temp_path)
        print(f"    Shape: {df.shape}, Columns: {list(df.columns)}")
        
        # Detect columns
        time_col = None
        price_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if any(x in col_lower for x in ['time', 'date', 'period', 'datetime']):
                time_col = col
            elif any(x in col_lower for x in ['price', 'lmp', 'total']):
                price_col = col
        
        if not time_col:
            time_col = df.columns[0]
        if not price_col:
            price_col = df.columns[1]
        
        print(f"    Using: time={time_col}, price={price_col}")
        
        rows = []
        for _, row in df.iterrows():
            try:
                target_time = pd.to_datetime(row[time_col])
                rows.append({
                    'target_timestamp': target_time.isoformat(),
                    'price': float(row[price_col]) if pd.notna(row[price_col]) else None,
                    'congestion_price': None,
                    'loss_price': None,
                    'energy_price': None,
                    'location': 'NIPS.WVPA',
                    'forecast_timestamp': file_info['forecast_timestamp'].isoformat(),
                    'version': file_info['version'],
                    'filename': file_info['filename']
                })
            except Exception as e:
                print(f"    Row error: {e}")
                continue
        
        temp_path.unlink()
        print(f"    Parsed {len(rows)} rows")
        return rows
        
    except Exception as e:
        print(f"    Parse error: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    print("=" * 60)
    print("ACES Price Scraper - API Approach")
    print("=" * 60)
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    processed = get_processed_files(supabase)
    print(f"Already processed: {len(processed)}")
    
    driver = init_browser()
    
    try:
        login(driver)
        all_files = scan_files(driver)
        new_files = [f for f in all_files if f['filename'] not in processed]
        print(f"New files: {len(new_files)}")
        
        if not new_files:
            print("Nothing to process")
            return
        
        # Test first file
        test_file = new_files[0]
        print(f"\nTesting: {test_file['filename']}")
        
        try:
            # Download via API
            content = download_file_api(driver, test_file['filename'])
            
            # Parse
            file_info = parse_filename(test_file['filename'])
            rows = process_csv_content(content, {**file_info, 'filename': test_file['filename']})
            
            if rows:
                table = 'da_price_forecasts' if file_info['type'] == 'da' else 'rt_price_forecasts'
                print(f"  Inserting {len(rows)} rows into {table}")
                
                response = supabase.table(table).upsert(
                    rows, 
                    on_conflict='target_timestamp,version,location'
                ).execute()
                
                print(f"  Response: {response}")
                
                # Mark as processed
                supabase.table('processed_files').insert({
                    'filename': test_file['filename'],
                    'file_type': file_info['type'],
                    'file_size_bytes': len(content),
                    'row_count': len(rows),
                    'import_status': 'success'
                }).execute()
                
                print("  ✓ SUCCESS")
            else:
                raise Exception("No data parsed")
                
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            
            supabase.table('processed_files').insert({
                'filename': test_file['filename'],
                'file_type': test_file.get('type', 'unknown'),
                'file_size_bytes': 0,
                'row_count': 0,
                'import_status': 'failed'
            }).execute()
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
