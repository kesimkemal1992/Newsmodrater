# forexfactory_xml.py
import os
import time
import random
import xml.etree.ElementTree as ET
import requests
from typing import List, Dict, Optional
from datetime import datetime
from requests.exceptions import ProxyError, Timeout, ConnectionError

XML_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"

# የተሰጡት ሶክስ5 ፕሮክሲዎች (ከዚህ በታች ሙሉ ዝርዝር)
STATIC_PROXIES = [
    "socks5://72.49.49.11:31034",
    "socks5://208.102.51.6:58208",
    "socks5://69.61.200.104:36181",
    "socks5://66.42.224.229:41679",
    "socks5://192.111.137.37:18762",
    "socks5://192.252.208.67:14287",
    "socks5://192.252.208.70:14282",
    "socks5://192.111.135.18:18301",
    "socks5://192.111.129.145:16894",
    "socks5://192.252.214.20:15864",
    "socks5://174.77.111.198:49547",
    "socks5://98.178.72.21:10919",
    "socks5://184.178.172.28:15294",
    "socks5://184.178.172.25:15291",
    "socks5://184.178.172.18:15280",
    "socks5://184.178.172.5:15303",
    "socks5://70.166.167.38:57728",
    "socks5://184.178.172.13:15311",
    "socks5://192.252.215.5:16137",
    "socks5://72.205.0.93:4145",
    "socks5://162.253.68.97:4145",
    "socks5://134.199.159.23:1080",
    "socks5://103.174.122.197:8199",
    "socks5://121.169.46.116:1090",
    "socks5://184.95.220.42:1080",
    "socks5://5.255.117.127:1080",
    "socks5://5.255.113.177:1080",
    "socks5://203.189.135.73:1080",
    "socks5://5.255.99.75:1080",
    "socks5://206.123.156.217:5836",
    "socks5://206.123.156.213:6410",
    "socks5://206.123.156.217:6198",
    "socks5://206.123.156.215:18136",
    "socks5://206.123.156.213:6264",
    "socks5://206.123.156.213:25003",
    "socks5://109.201.65.228:1080",
    "socks5://195.19.50.44:1080",
    "socks5://195.19.50.57:1080",
    "socks5://206.123.156.213:6357",
    "socks5://206.123.156.213:6383",
    "socks5://206.123.156.213:6343",
    "socks5://206.123.156.211:4553",
    "socks5://206.123.156.211:5875",
    "socks5://206.123.156.215:4240",
    "socks5://212.58.132.5:1080",
    "socks5://206.123.156.217:6182",
    "socks5://206.123.156.201:4136",
    "socks5://93.177.116.84:1080",
    "socks5://206.123.156.207:8315",
    "socks5://129.150.55.165:1080",
    "socks5://206.123.156.215:5144",
    "socks5://206.123.156.213:4048",
    "socks5://151.241.109.212:1080",
    "socks5://206.123.156.215:7036",
    "socks5://206.123.156.217:6425",
    "socks5://206.123.156.217:7045",
    "socks5://206.123.156.215:7063",
    "socks5://206.123.156.215:5299",
    "socks5://206.123.156.215:6497",
    "socks5://206.123.156.215:7180",
    "socks5://206.123.156.215:5534",
    "socks5://206.123.156.207:5470",
    "socks5://77.239.112.110:1080",
    "socks5://206.123.156.236:5550",
    "socks5://206.123.156.217:7648",
    "socks5://206.123.156.217:5825",
    "socks5://206.123.156.213:6167",
    "socks5://206.123.156.211:7702",
    "socks5://206.123.156.231:7389",
    "socks5://206.123.156.231:4409",
    "socks5://206.123.156.213:6461",
    "socks5://206.123.156.213:24668",
    "socks5://206.123.156.217:5215",
    "socks5://206.123.156.215:4717",
    "socks5://206.123.156.231:5725",
    "socks5://206.123.156.215:7280",
    "socks5://206.123.156.215:5263",
    "socks5://206.123.156.215:6601",
    "socks5://206.123.156.231:5841",
    "socks5://206.123.156.215:7293",
    "socks5://206.123.156.213:4761",
    "socks5://206.123.156.231:5750",
    "socks5://206.123.156.231:4699",
    "socks5://206.123.156.215:6018",
    "socks5://206.123.156.215:4274",
    "socks5://206.123.156.231:5938",
    "socks5://206.123.156.231:5632",
    "socks5://206.123.156.231:8431",
    "socks5://206.123.156.231:7651",
    "socks5://206.123.156.231:6076",
    "socks5://206.123.156.215:4586",
    "socks5://206.123.156.207:9126",
    "socks5://206.123.156.231:6954",
    "socks5://206.123.156.207:6772",
    "socks5://5.255.103.55:1080",
    "socks5://206.123.156.217:23032",
    "socks5://206.123.156.217:22993",
    "socks5://206.123.156.217:22879",
    "socks5://206.123.156.217:22781",
    "socks5://206.123.156.217:22604",
]

# ከአካባቢ ተለዋዋጭ ወይም ከላይ ካለው ዝርዝር ፕሮክሲ አምጣ
def _get_proxy_list() -> List[str]:
    env_proxies = os.getenv("PROXY_LIST")
    if env_proxies:
        return [p.strip() for p in env_proxies.split(",") if p.strip()]
    return STATIC_PROXIES.copy()

_proxy_cache = _get_proxy_list()
_last_proxy_index = 0

def _get_random_proxy() -> Optional[Dict[str, str]]:
    """Return a random SOCKS5 proxy dict for requests."""
    if not _proxy_cache:
        return None
    proxy_url = random.choice(_proxy_cache)
    return {
        "http": proxy_url,
        "https": proxy_url,
    }

def fetch_xml_with_retry(timeout: int = 25, retries: int = 5, backoff: int = 5) -> Optional[ET.Element]:
    """
    Fetch XML using direct first, then fallback to rotating SOCKS5 proxies.
    """
    # First attempt: direct (no proxy) – fastest
    for attempt in range(2):
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
                print(f"Rate limited (429) direct – waiting {wait}s")
                time.sleep(wait)
            else:
                print(f"HTTP {response.status_code}, retrying...")
                time.sleep(backoff)
        except (Timeout, ConnectionError) as e:
            print(f"Direct attempt {attempt+1} failed: {e}")
            time.sleep(backoff)
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(backoff)

    # Now try with rotating SOCKS5 proxies (up to 20 attempts)
    for proxy_attempt in range(20):
        proxy = _get_random_proxy()
        if not proxy:
            print("No proxy available, aborting")
            break
        try:
            proxy_url = proxy["http"]
            # Mask password if present
            display_proxy = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
            print(f"🔄 Trying proxy {proxy_attempt+1}: {display_proxy}")
            response = requests.get(
                XML_FEED_URL,
                timeout=timeout + 5,
                proxies=proxy,
                headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
            )
            if response.status_code == 200:
                print(f"✅ XML fetched via proxy {display_proxy}")
                return ET.fromstring(response.content)
            else:
                print(f"Proxy returned {response.status_code}, skipping")
        except (ProxyError, Timeout, ConnectionError) as e:
            print(f"Proxy {proxy_attempt+1} error: {e}")
        time.sleep(1)

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
