# forexfactory_xml.py
import os
import time
import random
import xml.etree.ElementTree as ET
import requests
from typing import List, Dict, Optional
from datetime import datetime

XML_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"

# Free proxy API endpoints (multiple fallbacks)
PROXY_APIS = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
    "https://www.proxy-list.download/api/v1/get?type=http",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
]

_proxy_cache = []
_last_proxy_fetch = 0

def _fetch_free_proxies() -> List[str]:
    """Fetch a list of free HTTP proxies from multiple sources."""
    global _proxy_cache, _last_proxy_fetch
    now = time.time()
    # Refresh cache every 15 minutes (free proxies die fast)
    if _proxy_cache and (now - _last_proxy_fetch) < 900:
        return _proxy_cache
    
    proxies = []
    for api_url in PROXY_APIS:
        try:
            resp = requests.get(api_url, timeout=10)
            if resp.status_code == 200:
                if "json" in api_url:
                    # JSON response handling (for proxy-list.download)
                    data = resp.json()
                    if isinstance(data, list):
                        proxies = data
                        break
                else:
                    # Plain text list
                    raw_proxies = [p.strip() for p in resp.text.split('\n') if p.strip() and ':' in p]
                    if raw_proxies:
                        proxies = raw_proxies
                        break
        except Exception as e:
            print(f"Proxy fetch failed from {api_url}: {e}")
            continue
    
    if proxies:
        _proxy_cache = proxies
        _last_proxy_fetch = now
        print(f"✅ Fetched {len(proxies)} free proxies")
        # Keep only first 100 to avoid huge lists
        if len(_proxy_cache) > 100:
            _proxy_cache = _proxy_cache[:100]
    else:
        print("⚠️ No free proxies fetched, will retry direct connection")
    
    return _proxy_cache

def _get_random_proxy() -> Optional[Dict[str, str]]:
    """Returns a random proxy dict or None."""
    proxies = _fetch_free_proxies()
    if not proxies:
        return None
    # Test proxy before using? (too slow, skip)
    proxy = random.choice(proxies)
    # Ensure format: IP:PORT
    if not proxy.startswith("http://"):
        proxy_url = f"http://{proxy}"
    else:
        proxy_url = proxy
    return {"http": proxy_url, "https": proxy_url}

def fetch_xml_with_retry(timeout: int = 25, retries: int = 5, backoff: int = 5) -> Optional[ET.Element]:
    """
    Fetch XML with retry logic and fallback to free proxies.
    Strategy:
      1. Try direct connection (fastest)
      2. If rate limited, retry with exponential backoff
      3. After 3 failures, try with random free proxy
      4. Finally, return None
    """
    # First, try direct (no proxy)
    for attempt in range(retries):
        try:
            response = requests.get(
                XML_FEED_URL,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
            )
            if response.status_code == 200:
                return ET.fromstring(response.content)
            elif response.status_code == 429:
                wait = backoff * (attempt + 1)
                print(f"⚠️ Rate limited (429) - direct attempt {attempt+1}/{retries}, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"HTTP {response.status_code} - retrying...")
                time.sleep(backoff)
        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt+1}")
            time.sleep(backoff)
        except requests.exceptions.RequestException as e:
            print(f"Request error (attempt {attempt+1}): {e}")
            time.sleep(backoff)
        except ET.ParseError as e:
            print(f"XML parse error: {e}")
            return None

    # If direct fails, try with free proxies
    print("🔄 Direct connection failed, trying with free proxy...")
    for proxy_attempt in range(3):  # try up to 3 different proxies
        proxy = _get_random_proxy()
        if not proxy:
            print("No free proxy available, aborting")
            break
        try:
            proxy_url = proxy.get("http", "")
            print(f"🔄 Trying proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
            response = requests.get(
                XML_FEED_URL,
                timeout=timeout + 5,
                proxies=proxy,
                headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
            )
            if response.status_code == 200:
                print("✅ XML fetched successfully via proxy")
                return ET.fromstring(response.content)
            else:
                print(f"Proxy returned status {response.status_code}")
        except Exception as e:
            print(f"Proxy attempt {proxy_attempt+1} failed: {e}")
        time.sleep(2)
    
    print("❌ All fetch methods failed.")
    return None

def _convert_ff_time(t: str) -> tuple:
    if not t or t in ("All Day", "Tentative"):
        return ("All Day", "")
    try:
        clean = t.replace("<![CDATA[", "").replace("]]>", "").strip().lower()
        dt = datetime.strptime(clean, "%I:%M%p")
        return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
    except:
        return (t, "")

def parse_events(root: ET.Element, currency_filter: str = "USD", impact_filter: str = "High") -> List[Dict]:
    events = []
    if root is None:
        return events
    for event in root.findall("event"):
        title = event.findtext("title") or ""
        country = event.findtext("country") or ""
        date = event.findtext("date") or ""
        time_raw = event.findtext("time") or ""
        impact = event.findtext("impact") or ""
        forecast = event.findtext("forecast") or "—"
        previous = event.findtext("previous") or "—"

        if country.upper() != currency_filter.upper():
            continue
        if impact.lower() != impact_filter.lower():
            continue

        time_12h, time_24h = _convert_ff_time(time_raw)
        events.append({
            "name": title,
            "currency": country,
            "date": date,
            "time_raw": time_raw,
            "time_12h": time_12h,
            "time_24h": time_24h,
            "impact": "red",
            "forecast": forecast,
            "previous": previous,
        })
    return events

def fetch_and_filter_events(currency="USD", impact="High") -> List[Dict]:
    root = fetch_xml_with_retry()
    return parse_events(root, currency, impact)
