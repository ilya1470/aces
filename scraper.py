#!/usr/bin/env python3
"""
ACES Power Price Scraper - GitHub Actions Version
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

# Configuration from environment
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
ACES_USER = os.environ.get('ACES_USERNAME')
ACES_PASS = os.environ.get('ACES_PASSWORD')

DOWNLOAD_DIR = Path('/tmp/aces_downloads')
DOWNLOAD_DIR.mkdir(exist_ok=True)

def init_browser():
    """Initialize headless Chrome"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "profile.default_content_setting_values.automatic_downloads": 1,
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
    driver.get("https://de.acespower.com/#/login")
    time.sleep(2)
    
    inputs = driver.find_elements(By.TAG_NAME, "input")
    if len(inputs) >= 2:
        inputs[0].send_keys(ACES_USER)
        inputs[1].send_keys(ACES_PASS)
    
    buttons = driver.find_elements(By.CSS_SELECTOR, 'button')
    for btn in buttons:
        if btn.is_displayed():
            btn.click()
            break
    
    time.sleep(3)
    
    if "/login" in driver.current_url:
        raise Exception("Login failed")
    
    print("Login successful")
    return True

def get_processed_files(supabase):
    """Get list of already processed files"""
    try:
        response = supabase.table('processed_files').select('filename').execute()
        return set([f['filename'] for f in response.data])
    except Exception as e:
        print(f"Error fetching processed files: {e}")
        return set()

def scan_files(driver):
    """Scan for CSV files in portal"""
    print("Scanning for files...")
    
    if "/#/" not in driver.current_url:
        driver.get("https://de.acespower.com/#/")
        time.sleep(2)
    
    # Scroll to load all
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
    
    # Extract file list
    files = driver.execute_script("""
        var results = [];
        var rows = document.querySelectorAll('tr, .file-row');
        rows.forEach(function(row) {
            var text = row.textContent || '';
            var match = text.match(/(NIPS\\.WVPA_(da|rt)_price_forecast_(\\d{14})\\.csv)/);
            if (match) {
                results.push({
                    filename: match[1],
                    type: match[2],
                    version: match[3]
                });
            }
        });
        return results;
    """)
    
    # Remove duplicates
    seen = set()
    unique = []
    for f in files:
        if f['filename'] not in seen:
            seen.add(f['filename'])
            unique.append(f)
    
    return unique

def parse_filename(filename):
    """Parse version from filename"""
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

def download_file(driver, filename):
    """Download a single file"""
    # Clear previous downloads
    for f in DOWNLOAD_DIR.glob('*.csv'):
        f.unlink()
    
    # Click the file row
    clicked = driver.execute_script("""
        var rows = document.querySelectorAll('tr, .file-row');
        for (var i = 0; i < rows.length; i++) {
            if (rows[i].textContent.includes(arguments[0])) {
                var buttons = rows[i].querySelectorAll('button');
                if (buttons.length > 0) {
                    buttons[buttons.length - 1].click();
                    return true;
                }
            }
        }
        return false;
    """, filename)
    
    if not clicked:
        return None
    
    time.sleep(3)
    
    files = list(DOWNLOAD_DIR.glob('*.csv'))
    return files[0] if files else None

def process_csv(filepath, file_info):
    """Parse CSV and prepare for insertion"""
    try:
        df = pd.read_csv(filepath)
        
        # Detect columns
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
        
        return rows
    except Exception as e:
        print(f"Error parsing CSV: {e}")
        return []

def main():
    print("=" * 60)
    print("ACES Price Scraper Starting")
    print("=" * 60)
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    processed = get_processed_files(supabase)
    print(f"Already processed: {len(processed)} files")
    
    driver = init_browser()
    
    try:
        login(driver)
        
        all_files = scan_files(driver)
        print(f"Found {len(all_files)} files in portal")
        
        new_files = [f for f in all_files if f['filename'] not in processed]
        print(f"New files to process: {len(new_files)}")
        
        if not new_files:
            print("No new files to process")
            return
        
        results = {'success': 0, 'failed': 0, 'da': 0, 'rt': 0}
        
        for file_meta in new_files:
            print(f"\nProcessing: {file_meta['filename']}")
            
            try:
                filepath = download_file(driver, file_meta['filename'])
                if not filepath:
                    raise Exception("Download failed")
                
                file_info = parse_filename(file_meta['filename'])
                if not file_info:
                    raise Exception("Could not parse filename")
                
                rows = process_csv(filepath, {**file_info, 'filename': file_meta['filename']})
                
                if not rows:
                    raise Exception("No data parsed")
                
                table = 'da_price_forecasts' if file_info['type'] == 'da' else 'rt_price_forecasts'
                
                response = supabase.table(table).upsert(
                    rows,
                    on_conflict='target_timestamp,version,location'
                ).execute()
                
                supabase.table('processed_files').insert({
                    'filename': file_meta['filename'],
                    'file_type': file_info['type'],
                    'file_size_bytes': filepath.stat().st_size,
                    'row_count': len(rows),
                    'import_status': 'success'
                }).execute()
                
                print(f"  ✓ Inserted {len(rows)} rows to {table}")
                results['success'] += 1
                results[file_info['type']] += 1
                
                filepath.unlink()
                
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                results['failed'] += 1
                
                try:
                    supabase.table('processed_files').insert({
                        'filename': file_meta['filename'],
                        'file_type': file_meta.get('type', 'unknown'),
                        'import_status': 'failed',
                        'row_count': 0
                    }).execute()
                except:
                    pass
        
        print("\n" + "=" * 60)
        print(f"Results: {results['success']} success, {results['failed']} failed")
        print(f"DA: {results['da']}, RT: {results['rt']}")
        print("=" * 60)
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
