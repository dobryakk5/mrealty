#!/usr/bin/env python3
"""
Diagnostic script to identify why Avito appears to process the same listing twice during Excel export
"""

import sys
import os
import asyncio
from datetime import datetime

# Add path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class DiagnosticLogger:
    """Logger to track function calls and identify duplicates"""
    
    def __init__(self):
        self.call_log = []
        self.start_time = datetime.now()
    
    def log_call(self, function_name, url, additional_info=""):
        """Log a function call with timestamp"""
        timestamp = datetime.now()
        elapsed = (timestamp - self.start_time).total_seconds()
        
        entry = {
            'timestamp': timestamp,
            'elapsed_seconds': elapsed,
            'function': function_name,
            'url': url,
            'info': additional_info
        }
        
        self.call_log.append(entry)
        print(f"[{elapsed:6.1f}s] {function_name}: {url} {additional_info}")
    
    def analyze_duplicates(self):
        """Analyze the call log for potential duplicates"""
        print("\n" + "="*80)
        print("🔍 DUPLICATE ANALYSIS")
        print("="*80)
        
        # Group by URL
        url_calls = {}
        for entry in self.call_log:
            url = entry['url']
            if url not in url_calls:
                url_calls[url] = []
            url_calls[url].append(entry)
        
        duplicates_found = False
        
        for url, calls in url_calls.items():
            if len(calls) > 1:
                duplicates_found = True
                print(f"\n❌ DUPLICATE PROCESSING DETECTED for URL:")
                print(f"   {url}")
                print(f"   Processed {len(calls)} times:")
                
                for i, call in enumerate(calls, 1):
                    print(f"     {i}. [{call['elapsed_seconds']:6.1f}s] {call['function']} - {call['info']}")
                
                # Calculate time gaps
                if len(calls) > 1:
                    for i in range(1, len(calls)):
                        gap = calls[i]['elapsed_seconds'] - calls[i-1]['elapsed_seconds']
                        print(f"        Gap between calls {i} and {i+1}: {gap:.1f} seconds")
        
        if not duplicates_found:
            print("✅ No duplicate processing detected")
        
        print(f"\n📊 SUMMARY:")
        print(f"   Total function calls: {len(self.call_log)}")
        print(f"   Unique URLs processed: {len(url_calls)}")
        print(f"   Total processing time: {(datetime.now() - self.start_time).total_seconds():.1f} seconds")

# Global diagnostic logger
diagnostic_logger = DiagnosticLogger()

async def test_excel_export_with_diagnostics():
    """Test Excel export with diagnostic logging to identify duplication"""
    
    print("🧪 AVITO EXCEL EXPORT DUPLICATION DIAGNOSTIC")
    print("="*60)
    
    # Test with a simple Avito URL (may be inactive, but that's OK for diagnosis)
    test_urls = [
        "https://www.avito.ru/moskva/kvartiry/1-k._kvartira_35_m_416_et._3573419659"
    ]
    
    print(f"Testing with URL: {test_urls[0]}")
    print("Monitoring for duplicate function calls...\n")
    
    try:
        # Patch the functions to add diagnostic logging
        from listings_processor import ListingsProcessor, export_listings_to_excel
        
        original_parse_avito = ListingsProcessor.parse_avito_listing
        original_export_function = export_listings_to_excel
        
        async def logged_parse_avito(self, url, skip_photos=True):
            """Wrapper for parse_avito_listing with logging"""
            diagnostic_logger.log_call("parse_avito_listing", url, f"skip_photos={skip_photos}")
            result = await original_parse_avito(self, url, skip_photos)
            status = "SUCCESS" if result else "FAILED"
            diagnostic_logger.log_call("parse_avito_listing_result", url, status)
            return result
        
        # Apply the patches
        ListingsProcessor.parse_avito_listing = logged_parse_avito
        
        # Run the Excel export
        print("🚀 Starting Excel export...")
        diagnostic_logger.log_call("export_listings_to_excel", test_urls[0], "START")
        
        user_id = 999999
        bio, request_id = await export_listings_to_excel(test_urls, user_id)
        
        diagnostic_logger.log_call("export_listings_to_excel", test_urls[0], "COMPLETED")
        
        if bio:
            print("✅ Excel export completed successfully")
            
            # Read the Excel to check results
            import pandas as pd
            bio.seek(0)
            df = pd.read_excel(bio)
            print(f"📊 Excel contains {len(df)} rows")
            
            # Check for duplicate rows
            if len(df) > 1:
                print("❌ MULTIPLE ROWS DETECTED - This could indicate duplication!")
                for i, row in df.iterrows():
                    print(f"   Row {i+1}: {row.get('URL', 'No URL')}")
            else:
                print("✅ Single row as expected")
        else:
            print("❌ Excel export failed")
        
        # Restore original functions
        ListingsProcessor.parse_avito_listing = original_parse_avito
        
    except Exception as e:
        print(f"❌ Diagnostic test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Analyze the results
        diagnostic_logger.analyze_duplicates()

async def test_simple_avito_parser():
    """Test just the Avito parser directly to isolate the issue"""
    
    print("\n" + "="*60)
    print("🧪 TESTING AVITO PARSER DIRECTLY")
    print("="*60)
    
    try:
        from listings_processor import ListingsProcessor
        
        processor = ListingsProcessor()
        test_url = "https://www.avito.ru/moskva/kvartiry/1-k._kvartira_35_m_416_et._3573419659"
        
        print(f"Testing direct Avito parser call...")
        diagnostic_logger.log_call("direct_parse_avito", test_url, "SINGLE_CALL")
        
        result = await processor.parse_avito_listing(test_url, skip_photos=True)
        
        if result:
            print("✅ Direct parser call successful")
            diagnostic_logger.log_call("direct_parse_avito_result", test_url, "SUCCESS")
        else:
            print("❌ Direct parser call failed")
            diagnostic_logger.log_call("direct_parse_avito_result", test_url, "FAILED")
        
    except Exception as e:
        print(f"❌ Direct parser test failed: {e}")

if __name__ == "__main__":
    print("AVITO DUPLICATION DIAGNOSTIC TOOL")
    print("="*50)
    
    asyncio.run(test_excel_export_with_diagnostics())
    asyncio.run(test_simple_avito_parser())
    
    print("\n🎯 RECOMMENDATIONS:")
    print("1. Check the analysis above for duplicate function calls")
    print("2. If duplicates are found, check the time gaps to understand the cause")
    print("3. Look for browser restoration or retry logic in the call stack")
    print("4. Verify that the user is not calling the function multiple times")
    print("\n💡 If no duplicates are detected, the issue may be:")
    print("   - User perception due to verbose logging")
    print("   - Browser restoration appearing as re-processing")
    print("   - Network timeouts causing retry attempts")