#!/usr/bin/env python3
"""
ACES Power Price Scraper - GitHub Actions Version - With Debug
"""

import os
import time
import re
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

DOWNLOAD_DIR = Path('/tmp/aces_downloads')
DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)

def init_browser():
    """Initialize headless Chrome with download settings"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Critical: Allow multiple downloads without popup
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "profile.content_settings.exceptions.automatic_downloads": {
            "*.acespower.com,*": {"setting": 1}
        }
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.implicitly_wait(5)
    return driver

def login(driver):
    """Login to ACES portal"""
    print("Logging in...")
    driver.get("https://de.acespower.com/Web/Account/Login.htm")
    time.sleep(3)
    
    try:
        username_field = driver.find_element(By.NAME, "username")
        password_field = driver.find_element(By.NAME, "password")
    except Exception as e:
        raise Exception(f"Could not find login fields: {e}")
    
    username_field.send_keys(ACES_USER)
    password_field.send_keys(ACES_PASS)
    print("Credentials entered")
    
    try:
        login_btn = driver.find_element(By.ID, "loginSubmit")
        login_btn.click()
    except:
        password_field.submit()
    
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

def download_file(driver, filename):
    print(f"  Downloading: {filename}")
    print(f"  Download dir: {DOWNLOAD_DIR}")
    
    # Clear previous
    for f in DOWNLOAD_DIR.glob('*'):
        f.unlink()
    
    # Click using XPath to find the text directly
    try:
        # Strategy: Find element containing filename text and click it
        result = driver.execute_script("""
            // Try to find by XPath
            var xpath = "//*[contains(text(), '" + arguments[0] + "')]";
            var iter = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
            var el = iter.singleNodeValue;
            if (el) {
                el.scrollIntoView({block: 'center'});
                el.click();
                return 'clicked_text';
            }
            return 'not_found';
        """, filename)
        
        print(f"  Click result: {result}")
        
        if result == 'not_found':
            raise Exception("Could not find file element")
        
    except Exception as e:
        print(f"  Click error: {e}")
        raise
    
    # Wait for download
    print("  Waiting 10s for download...")
    time.sleep(10)
    
    # Check results
    files = list(DOWNLOAD_DIR.iterdir())
    print(f"  Files found: {len(files)}")
    for f in files:
        print(f"    - {f.name} ({f.stat().st_size} bytes)")
    
    csv_files = [f for f in files if f.suffix == '.csv']
    if csv_files:
        print(f"  ✓ Downloaded: {csv_files[0].name}")
        return csv_files[0]
    
    # Check for partial downloads
    partial = [f for f in files if '.crdownload' in f.name]
    if partial:
        raise Exception(f"Download incomplete: {partial[0].name}")
    
    if files:
        raise Exception(f"Wrong file type: {[f.name for f in files]}")
    else:
        raise Exception("No files downloaded")

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

def process_csv(filepath, file_info):
    try:
        print(f"    Reading: {filepath}")
        df = pd.read_csv(filepath)
        print(f"    Shape: {df.shape}, Columns: {list(df.columns)}")
        
        time_col = None
        price_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if any(x in col_lower for x in ['time', 'date', 'period']):
                time_col = col
            elif any(x in col_lower for x in ['price', 'lmp', 'total']):
                price_col = col
        
        if not time_col:
            time_col = df.columns[0]
        if not price_col:
            price_col = df.columns[1]
        
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
            except:
                continue
        
        print(f"    Parsed {len(rows)} rows")
        return rows
    except Exception as e:
        print(f"    Parse error: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    print("=" * 60)
    print("ACES Price Scraper")
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
        
        # Test with just first file
        test_file = new_files[0]
        print(f"\nTesting with: {test_file['filename']}")
        
        try:
            filepath = download_file(driver, test_file['filename'])
            file_info = parse_filename(test_file['filename'])
            rows = process_csv(filepath, {**file_info, 'filename': test_file['filename']})
            
            if rows:
                table = 'da_price_forecasts' if file_info['type'] == 'da' else 'rt_price_forecasts'
                print(f"  Inserting into {table}")
                
                response = supabase.table(table).upsert(rows, on_conflict='target_timestamp,version,location').execute()
                print(f"  Insert response: {response}")
                
                supabase.table('processed_files').insert({
                    'filename': test_file['filename'],
                    'file_type': file_info['type'],
                    'row_count': len(rows),
                    'import_status': 'success'
                }).execute()
                
                print("  ✓ SUCCESS")
            else:
                print("  ✗ No rows to insert")
            
            filepath.unlink()
            
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            
            supabase.table('processed_files').insert({
                'filename': test_file['filename'],
                'file_type': test_file.get('type', 'unknown'),
                'import_status': 'failed',
                'row_count': 0
            }).execute()
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
