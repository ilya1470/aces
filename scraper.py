#!/usr/bin/env python3
"""
ACES Power Price Scraper - Inspecting download mechanism
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

def inspect_download_mechanism(driver, filename):
    """
    Inspect the page to understand how downloads work
    """
    print(f"  Inspecting download mechanism for: {filename}")
    
    # Get row HTML and all attributes
    row_info = driver.execute_script("""
        var rows = document.querySelectorAll('tr');
        for (var i = 0; i < rows.length; i++) {
            if (rows[i].textContent.includes(arguments[0])) {
                var info = {
                    rowHTML: rows[i].outerHTML.substring(0, 500),
                    buttons: [],
                    links: []
                };
                
                // Get all buttons
                var buttons = rows[i].querySelectorAll('button');
                buttons.forEach(function(btn, idx) {
                    info.buttons.push({
                        index: idx,
                        text: btn.textContent,
                        onclick: btn.getAttribute('onclick'),
                        class: btn.className,
                        id: btn.id,
                        dataAttrs: {}
                    });
                    // Get all data-* attributes
                    for (var j = 0; j < btn.attributes.length; j++) {
                        var attr = btn.attributes[j];
                        if (attr.name.startsWith('data-')) {
                            info.buttons[idx].dataAttrs[attr.name] = attr.value;
                        }
                    }
                });
                
                // Get all links
                var links = rows[i].querySelectorAll('a');
                links.forEach(function(link, idx) {
                    info.links.push({
                        index: idx,
                        href: link.getAttribute('href'),
                        text: link.textContent,
                        onclick: link.getAttribute('onclick')
                    });
                });
                
                return info;
            }
        }
        return null;
    """, filename)
    
    print(f"  Row inspection result:")
    print(f"    Buttons found: {len(row_info['buttons'])}")
    for btn in row_info['buttons']:
        print(f"      Button {btn['index']}: text='{btn['text']}', onclick='{btn['onclick']}', data={btn['dataAttrs']}")
    
    print(f"    Links found: {len(row_info['links'])}")
    for link in row_info['links']:
        print(f"      Link {link['index']}: href='{link['href']}', text='{link['text']}'")
    
    # Try to construct download URL from patterns
    # Look for data-file-id or similar
    file_id = None
    for btn in row_info['buttons']:
        if 'data-file-id' in btn['dataAttrs']:
            file_id = btn['dataAttrs']['data-file-id']
            break
        if 'data-id' in btn['dataAttrs']:
            file_id = btn['dataAttrs']['data-id']
            break
    
    return row_info, file_id

def download_file_cdp(driver, filename):
    """
    Use Chrome DevTools Protocol to capture download
    """
    print(f"  Attempting download via CDP: {filename}")
    
    # Enable CDP download handling
    driver.execute_cdp_cmd('Page.setDownloadBehavior', {
        'behavior': 'allow',
        'downloadPath': '/tmp'
    })
    
    # Click using JavaScript
    click_result = driver.execute_script("""
        var rows = document.querySelectorAll('tr');
        for (var i = 0; i < rows.length; i++) {
            if (rows[i].textContent.includes(arguments[0])) {
                // Try clicking the text itself
                var xpath = "//td[contains(text(), '" + arguments[0] + "')]";
                var result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                var el = result.singleNodeValue;
                if (el) {
                    el.click();
                    return 'clicked_xpath';
                }
                
                // Try clicking parent
                rows[i].click();
                return 'clicked_row';
            }
        }
        return 'not_found';
    """, filename)
    
    print(f"  Click result: {click_result}")
    time.sleep(10)
    
    # Check for downloaded file
    import glob
    files = glob.glob('/tmp/*.csv') + glob.glob('/tmp/*.crdownload')
    print(f"  Files in /tmp: {files}")
    
    if files:
        # Get the most recent
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return Path(files[0])
    
    return None

def download_via_requests(driver, filename, file_info):
    """
    Try to construct and use direct download URL
    """
    print(f"  Trying direct HTTP download")
    
    # Get cookies
    cookies = driver.get_cookies()
    session = requests.Session()
    for c in cookies:
        session.cookies.set(c['name'], c['value'])
    
    # Common patterns for Web Transfer Client
    encoded = requests.utils.quote(filename)
    
    # Try different URL patterns
    urls = [
        f"https://de.acespower.com/Files/Download/{encoded}",
        f"https://de.acespower.com/Download/{encoded}",
        f"https://de.acespower.com/api/Files/{encoded}",
        f"https://de.acespower.com/api/files/{file_info['version']}",
        f"https://de.acespower.com/handlers/download.ashx?file={encoded}",
        f"https://de.acespower.com/download.aspx?file={encoded}",
    ]
    
    for url in urls:
        print(f"    Trying: {url}")
        try:
            r = session.get(url, timeout=30, allow_redirects=True)
            print(f"    Status: {r.status_code}, Size: {len(r.content)}")
            if r.status_code == 200 and len(r.content) > 100:
                content_type = r.headers.get('content-type', '')
                content_disp = r.headers.get('content-disposition', '')
                if 'csv' in content_type.lower() or 'csv' in content_disp.lower() or filename in content_disp:
                    print(f"    ✓ Success!")
                    return r.content
                # Check if content looks like CSV
                if b',' in r.content[:1000] and len(r.content) > 500:
                    print(f"    ✓ Looks like CSV")
                    return r.content
        except Exception as e:
            print(f"    Error: {e}")
    
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
        if isinstance(content, bytes):
            temp_path.write_bytes(content)
        else:
            temp_path.write_text(content)
        
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
    print("ACES Price Scraper - Inspection Mode")
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
        print(f"\n{'='*60}")
        print(f"Testing: {test_file['filename']}")
        print(f"{'='*60}")
        
        # Step 1: Inspect the download mechanism
        row_info, file_id = inspect_download_mechanism(driver, test_file['filename'])
        
        if file_id:
            print(f"\n  Found file_id: {file_id}")
        
        # Step 2: Try CDP download
        print(f"\n  Attempting CDP download...")
        downloaded = download_file_cdp(driver, test_file['filename'])
        
        content = None
        if downloaded:
            print(f"  ✓ CDP download worked: {downloaded}")
            content = downloaded.read_bytes()
            downloaded.unlink()
        else:
            print(f"  ✗ CDP failed, trying HTTP...")
            # Step 3: Try HTTP requests
            file_info = parse_filename(test_file['filename'])
            content = download_via_requests(driver, test_file['filename'], file_info)
        
        if not content:
            raise Exception("All download methods failed")
        
        # Process and insert
        file_info = parse_filename(test_file['filename'])
        rows = process_csv_content(content, {**file_info, 'filename': test_file['filename']})
        
        if rows:
            table = 'da_price_forecasts' if file_info['type'] == 'da' else 'rt_price_forecasts'
            print(f"\n  Inserting {len(rows)} rows into {table}")
            
            response = supabase.table(table).upsert(
                rows, 
                on_conflict='target_timestamp,version,location'
            ).execute()
            
            print(f"  Response: {response}")
            
            supabase.table('processed_files').insert({
                'filename': test_file['filename'],
                'file_type': file_info['type'],
                'file_size_bytes': len(content),
                'row_count': len(rows),
                'import_status': 'success'
            }).execute()
            
            print("\n  ✓ SUCCESS")
        else:
            raise Exception("No data parsed")
            
    except Exception as e:
        print(f"\n  ✗ FAILED: {e}")
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
