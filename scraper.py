#!/usr/bin/env python3
"""
ACES Power Price Scraper - Advanced download methods
"""

import os
import time
import re
import base64
from pathlib import Path
from datetime import datetime
from supabase import create_client
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

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
    # Disable download prompt in headless
    chrome_options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        "safebrowsing.enabled": False
    })
    
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

def download_file_js(driver, filename):
    """
    Use JavaScript to trigger download and capture as base64
    """
    print(f"  Attempting JS download: {filename}")
    
    # Enable download behavior
    driver.execute_cdp_cmd('Page.setDownloadBehavior', {
        'behavior': 'allow',
        'downloadPath': '/tmp'
    })
    
    # Try clicking with different strategies
    result = driver.execute_script("""
        var filename = arguments[0];
        
        // Strategy 1: Find by text content and click parent
        var allElements = document.querySelectorAll('*');
        for (var i = 0; i < allElements.length; i++) {
            var el = allElements[i];
            if (el.children.length === 0 && el.textContent.trim() === filename) {
                console.log('Found exact text match:', el);
                el.click();
                return 'clicked_exact_text';
            }
        }
        
        // Strategy 2: Find containing element
        var rows = document.querySelectorAll('tr, .file-row, [role="row"]');
        for (var i = 0; i < rows.length; i++) {
            if (rows[i].textContent.includes(filename)) {
                console.log('Found row:', rows[i]);
                
                // Try double click
                var event = new MouseEvent('dblclick', {
                    'view': window,
                    'bubbles': true,
                    'cancelable': true
                });
                rows[i].dispatchEvent(event);
                return 'dblclicked_row';
            }
        }
        
        return 'not_found';
    """, filename)
    
    print(f"  Click result: {result}")
    time.sleep(5)
    
    # Check for downloaded file
    import glob
    files = glob.glob('/tmp/*.csv') + glob.glob('/tmp/*.crdownload')
    print(f"  Files found: {files}")
    
    if files:
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        latest = Path(files[0])
        content = latest.read_bytes()
        latest.unlink()
        return content
    
    return None

def download_file_fetch(driver, filename):
    """
    Use fetch API to download file and return base64
    """
    print(f"  Attempting fetch download: {filename}")
    
    # Get base URL and cookies
    base_url = driver.current_url
    
    # Try to find download URL patterns from the page
    result = driver.execute_async_script("""
        var callback = arguments[arguments.length - 1];
        var filename = arguments[0];
        
        // Look for any elements that might have download URLs
        var elements = document.querySelectorAll('[ng-click], [onclick], [data-download]');
        var urls = [];
        
        elements.forEach(function(el) {
            var onclick = el.getAttribute('onclick') || '';
            var ngClick = el.getAttribute('ng-click') || '';
            var dataDownload = el.getAttribute('data-download') || '';
            
            if (onclick.includes('download') || ngClick.includes('download') || dataDownload) {
                urls.push({
                    onclick: onclick,
                    ngClick: ngClick,
                    dataDownload: dataDownload,
                    text: el.textContent.substring(0, 50)
                });
            }
        });
        
        // Try to trigger download using fetch
        // First, let's look for the file in any exposed JavaScript variables
        var fileData = null;
        if (window.files && window.files[filename]) {
            fileData = window.files[filename];
        }
        
        callback({
            urls: urls,
            fileData: fileData,
            windowKeys: Object.keys(window).filter(k => k.toLowerCase().includes('file')).slice(0, 10)
        });
    """, filename)
    
    print(f"  Page analysis: {result}")
    
    # If we found URL patterns, try to construct download URL
    # Try common patterns for Web Transfer / Angular apps
    encoded = filename.replace('.', '%2E').replace('_', '%5F')
    
    # Use browser's fetch with cookies
    fetch_result = driver.execute_async_script("""
        var callback = arguments[arguments.length - 1];
        var filename = arguments[0];
        
        // Try to download using fetch
        fetch('/api/files/' + filename, {
            method: 'GET',
            credentials: 'include'
        })
        .then(function(response) {
            if (!response.ok) throw new Error('HTTP ' + response.status);
            return response.blob();
        })
        .then(function(blob) {
            var reader = new FileReader();
            reader.onloadend = function() {
                callback({success: true, data: reader.result});
            };
            reader.readAsDataURL(blob);
        })
        .catch(function(error) {
            callback({success: false, error: error.toString()});
        });
    """, filename)
    
    print(f"  Fetch result: {fetch_result}")
    
    if fetch_result and fetch_result.get('success'):
        # Decode base64 data
        data_url = fetch_result['data']
        if ',' in data_url:
            base64_data = data_url.split(',')[1]
            return base64.b64decode(base64_data)
    
    return None

def download_file_direct_click(driver, filename):
    """
    Direct click with mouse simulation
    """
    print(f"  Attempting direct click: {filename}")
    
    # Save screenshot before
    driver.save_screenshot('/tmp/before_click.png')
    print("  Screenshot saved: before_click.png")
    
    # Find element and click using ActionChains
    from selenium.webdriver.common.action_chains import ActionChains
    
    try:
        # Find element containing the text
        element = driver.find_element(By.XPATH, f"//*[contains(text(), '{filename}')]")
        print(f"  Found element: {element.tag_name}")
        
        # Scroll into view
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(1)
        
        # Save screenshot after scroll
        driver.save_screenshot('/tmp/after_scroll.png')
        
        # Click using ActionChains
        actions = ActionChains(driver)
        actions.move_to_element(element).click().perform()
        print("  Clicked element")
        
        time.sleep(5)
        
        # Check for download
        import glob
        files = glob.glob('/tmp/*.csv') + glob.glob('/tmp/*.crdownload')
        print(f"  Files after click: {files}")
        
        if files:
            files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            latest = Path(files[0])
            content = latest.read_bytes()
            latest.unlink()
            return content
            
    except Exception as e:
        print(f"  Direct click error: {e}")
    
    return None

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
    try:
        temp_path = Path('/tmp') / file_info['filename']
        temp_path.write_bytes(content)
        
        df = pd.read_csv(temp_path)
        print(f"    Shape: {df.shape}, Columns: {list(df.columns)}")
        
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
    print("ACES Price Scraper - Advanced Methods")
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
        
        # Test first file with multiple methods
        test_file = new_files[0]
        print(f"\n{'='*60}")
        print(f"Testing: {test_file['filename']}")
        print(f"{'='*60}")
        
        content = None
        
        # Method 1: Direct click with ActionChains
        print("\nMethod 1: Direct element click")
        content = download_file_direct_click(driver, test_file['filename'])
        
        # Method 2: JavaScript click
        if not content:
            print("\nMethod 2: JavaScript click")
            content = download_file_js(driver, test_file['filename'])
        
        # Method 3: Fetch API
        if not content:
            print("\nMethod 3: Fetch API")
            content = download_file_fetch(driver, test_file['filename'])
        
        if not content:
            raise Exception("All download methods failed")
        
        print(f"\n✓ Downloaded {len(content)} bytes")
        
        # Process and insert
        file_info = parse_filename(test_file['filename'])
        rows = process_csv_content(content, {**file_info, 'filename': test_file['filename']})
        
        if rows:
            table = 'da_price_forecasts' if file_info['type'] == 'da' else 'rt_price_forecasts'
            print(f"\nInserting {len(rows)} rows into {table}")
            
            response = supabase.table(table).upsert(
                rows, 
                on_conflict='target_timestamp,version,location'
            ).execute()
            
            print(f"Response: {response}")
            
            supabase.table('processed_files').insert({
                'filename': test_file['filename'],
                'file_type': file_info['type'],
                'file_size_bytes': len(content),
                'row_count': len(rows),
                'import_status': 'success'
            }).execute()
            
            print("\n✓ SUCCESS")
        else:
            raise Exception("No data parsed")
            
    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            supabase.table('processed_files').insert({
                'filename': test_file['filename'],
                'file_type': test_file.get('type', 'unknown'),
                'import_status': 'failed',
                'row_count': 0
            }).execute()
        except:
            pass
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
