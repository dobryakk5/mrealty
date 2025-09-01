#!/usr/bin/env python3
"""
Test script to check all Avito fields parsing and display results
"""

import sys
import os
import asyncio
from datetime import datetime

# Add path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def test_avito_field_extraction():
    """Tests Avito field extraction and displays all parsed data"""
    
    print("🧪 AVITO FIELDS EXTRACTION TEST")
    print("="*60)
    
    try:
        from avito_parser_integration import AvitoCardParser
        
        # Test URL
        test_url = "https://www.avito.ru/moskva/kvartiry/4-k._kvartira_95_m_514_et._4805953955"
        
        print(f"🔗 Test URL: {test_url}")
        print("-"*60)
        
        # Create parser
        parser = AvitoCardParser(skip_photos=True)  # Skip photos for faster testing
        
        # Parse the page
        print("🚀 Starting parsing...")
        parsed_data = parser.parse_avito_page(test_url)
        
        if not parsed_data:
            print("❌ No data parsed!")
            return False
        
        print("\n📊 PARSING RESULTS:")
        print("="*60)
        
        # Display all fields in organized categories
        categories = {
            "🏠 BASIC INFO": [
                'url', 'title', 'price', 'rooms', 'total_area', 'floor', 'total_floors'
            ],
            "📍 LOCATION": [
                'address_data', 'metro_time'
            ],
            "🏢 APARTMENT PARAMS": [
                'apartment_params'
            ],
            "🏘️ HOUSE PARAMS": [
                'house_params'
            ],
            "📅 PUBLICATION INFO": [
                'publication_info'
            ],
            "📸 PHOTOS": [
                'photos'
            ],
            "📝 DESCRIPTION": [
                'description'
            ],
            "⚡ STATUS": [
                'status_info'
            ]
        }
        
        for category, fields in categories.items():
            print(f"\n{category}")
            print("-" * 40)
            
            for field in fields:
                if field in parsed_data:
                    value = parsed_data[field]
                    
                    if field == 'address_data' and isinstance(value, dict):
                        print(f"  📍 Address: {value.get('address', 'Not found')}")
                        print(f"  🚇 Metro stations: {len(value.get('metro_stations', []))}")
                        for i, station in enumerate(value.get('metro_stations', [])[:3], 1):
                            if isinstance(station, dict):
                                name = station.get('name', 'Unknown')
                                time_min = station.get('time_minutes', 'N/A')
                                print(f"    {i}. {name} - {time_min} min")
                    
                    elif field == 'apartment_params' and isinstance(value, dict):
                        print(f"  🏠 Apartment parameters ({len(value)} items):")
                        for key, val in list(value.items())[:5]:  # Show first 5
                            print(f"    • {key}: {val}")
                        if len(value) > 5:
                            print(f"    ... and {len(value) - 5} more")
                    
                    elif field == 'house_params' and isinstance(value, dict):
                        print(f"  🏘️ House parameters ({len(value)} items):")
                        for key, val in list(value.items())[:5]:  # Show first 5
                            print(f"    • {key}: {val}")
                        if len(value) > 5:
                            print(f"    ... and {len(value) - 5} more")
                    
                    elif field == 'publication_info' and isinstance(value, dict):
                        print(f"  📅 Publication date: {value.get('publication_date', 'Not found')}")
                        print(f"  👁️ Today views: {value.get('today_views', 'Not found')}")
                    
                    elif field == 'status_info' and isinstance(value, dict):
                        status = value.get('status', 'Unknown')
                        reason = value.get('reason', 'No reason')
                        # Visual status indication with clear indicators
                        if status == 'active':
                            status_icon = '✅'
                        elif status == 'inactive':
                            status_icon = '⚠️'
                        else:
                            status_icon = '❓'
                        print(f"  {status_icon} Status: {status}")
                        print(f"  📝 Reason: {reason}")
                    
                    elif field == 'photos' and isinstance(value, list):
                        print(f"  📸 Photos: {len(value)} found")
                        if value:
                            print(f"    First photo: {value[0][1][:60]}..." if len(value[0][1]) > 60 else value[0][1])
                    
                    elif field == 'description':
                        desc_preview = value[:100] + "..." if len(str(value)) > 100 else str(value)
                        print(f"  📝 Description: {desc_preview}")
                    
                    else:
                        # Simple fields
                        if isinstance(value, str) and len(value) > 50:
                            display_value = value[:50] + "..."
                        else:
                            display_value = value
                        print(f"  {field}: {display_value}")
                else:
                    print(f"  {field}: ❌ NOT FOUND")
        
        # Test data preparation for database
        print(f"\n💾 DATABASE PREPARATION TEST:")
        print("-" * 40)
        
        try:
            db_data = parser.prepare_data_for_db(parsed_data)
            if db_data:
                print("✅ Database preparation successful")
                print(f"📊 DB fields prepared: {len(db_data)}")
                
                # Show key DB fields
                key_db_fields = ['url', 'title', 'price', 'rooms', 'total_area', 'floor', 'metro_time', 'today_views']
                for field in key_db_fields:
                    if field in db_data:
                        value = db_data[field]
                        if isinstance(value, str) and len(value) > 40:
                            display_value = value[:40] + "..."
                        else:
                            display_value = value
                        print(f"  {field}: {display_value}")
            else:
                print("❌ Database preparation failed")
        except Exception as e:
            print(f"❌ Database preparation error: {e}")
        
        # Summary
        print(f"\n📈 SUMMARY:")
        print("-" * 40)
        total_fields = len([f for f in parsed_data if parsed_data[f] is not None])
        print(f"✅ Total fields parsed: {total_fields}")
        print(f"🏠 Rooms: {parsed_data.get('rooms', 'N/A')}")
        print(f"💰 Price: {parsed_data.get('price', 'N/A')}")
        print(f"📏 Area: {parsed_data.get('total_area', 'N/A')} m²")
        print(f"🏢 Floor: {parsed_data.get('floor', 'N/A')}/{parsed_data.get('total_floors', 'N/A')}")
        
        # Check view count specifically
        pub_info = parsed_data.get('publication_info', {})
        if isinstance(pub_info, dict):
            today_views = pub_info.get('today_views')
            if today_views is not None and today_views > 0:
                print(f"👁️ Views today: {today_views} ✅")
            elif today_views == 0:
                print(f"👁️ Views today: 0 ⚠️ (May need selector update)")
            else:
                print(f"👁️ Views today: Not found ❌")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 AVITO FIELD EXTRACTION DIAGNOSTIC")
    print("="*60)
    
    success = asyncio.run(test_avito_field_extraction())
    
    print(f"\n{'='*60}")
    if success:
        print("🎉 Test completed successfully!")
    else:
        print("❌ Test failed - check logs above")
    
    print("="*60)
    exit(0 if success else 1)