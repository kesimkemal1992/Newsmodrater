# forexfactory_xml.py
import os
import time
import random
import xml.etree.ElementTree as ET
import requests
from typing import List, Dict, Optional
from datetime import datetime

XML_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"

# ሙሉ የሶክስ5 ፕሮክሲ ዝርዝር (ከሰጠኸው)
STATIC_PROXIES = [
    "socks5://72.49.49.11:31034", "socks5://208.102.51.6:58208", "socks5://69.61.200.104:36181",
    "socks5://66.42.224.229:41679", "socks5://192.111.137.37:18762", "socks5://192.252.208.67:14287",
    "socks5://192.252.208.70:14282", "socks5://192.111.135.18:18301", "socks5://192.111.129.145:16894",
    "socks5://192.252.214.20:15864", "socks5://174.77.111.198:49547", "socks5://98.178.72.21:10919",
    "socks5://184.178.172.28:15294", "socks5://184.178.172.25:15291", "socks5://184.178.172.18:15280",
    "socks5://184.178.172.5:15303", "socks5://70.166.167.38:57728", "socks5://184.178.172.13:15311",
    "socks5://192.252.215.5:16137", "socks5://72.205.0.93:4145", "socks5://162.253.68.97:4145",
    "socks5://134.199.159.23:1080", "socks5://103.174.122.197:8199", "socks5://121.169.46.116:1090",
    "socks5://184.95.220.42:1080", "socks5://5.255.117.127:1080", "socks5://5.255.113.177:1080",
    "socks5://203.189.135.73:1080", "socks5://5.255.99.75:1080", "socks5://206.123.156.217:5836",
    "socks5://206.123.156.213:6410", "socks5://206.123.156.217:6198", "socks5://206.123.156.215:18136",
    "socks5://206.123.156.213:6264", "socks5://206.123.156.213:25003", "socks5://109.201.65.228:1080",
    "socks5://195.19.50.44:1080", "socks5://195.19.50.57:1080", "socks5://206.123.156.213:6357",
    "socks5://206.123.156.213:6383", "socks5://206.123.156.213:6343", "socks5://206.123.156.211:4553",
    "socks5://206.123.156.211:5875", "socks5://206.123.156.215:4240", "socks5://212.58.132.5:1080",
    "socks5://206.123.156.217:6182", "socks5://206.123.156.201:4136", "socks5://93.177.116.84:1080",
    "socks5://206.123.156.207:8315", "socks5://129.150.55.165:1080", "socks5://206.123.156.215:5144",
    "socks5://206.123.156.213:4048", "socks5://151.241.109.212:1080", "socks5://206.123.156.215:7036",
    "socks5://206.123.156.217:6425", "socks5://206.123.156.217:7045", "socks5://206.123.156.215:7063",
    "socks5://206.123.156.215:5299", "socks5://206.123.156.215:6497", "socks5://206.123.156.215:7180",
    "socks5://206.123.156.215:5534", "socks5://206.123.156.207:5470", "socks5://77.239.112.110:1080",
    "socks5://206.123.156.236:5550", "socks5://206.123.156.217:7648", "socks5://206.123.156.217:5825",
    "socks5://206.123.156.213:6167", "socks5://206.123.156.211:7702", "socks5://206.123.156.231:7389",
    "socks5://206.123.156.231:4409", "socks5://206.123.156.213:6461", "socks5://206.123.156.213:24668",
    "socks5://206.123.156.217:5215", "socks5://206.123.156.215:4717", "socks5://206.123.156.231:5725",
    "socks5://206.123.156.215:7280", "socks5://206.123.156.215:5263", "socks5://206.123.156.215:6601",
    "socks5://206.123.156.231:5841", "socks5://206.123.156.215:7293", "socks5://206.123.156.213:4761",
    "socks5://206.123.156.231:5750", "socks5://206.123.156.231:4699", "socks5://206.123.156.215:6018",
    "socks5://206.123.156.215:4274", "socks5://206.123.156.231:5938", "socks5://206.123.156.231:5632",
    "socks5://206.123.156.231:8431", "socks5://206.123.156.231:7651", "socks5://206.123.156.231:6076",
    "socks5://206.123.156.215:4586", "socks5://206.123.156.207:9126", "socks5://206.123.156.231:6954",
    "socks5://206.123.156.207:6772", "socks5://5.255.103.55:1080", "socks5://206.123.156.217:23032",
    "socks5://206.123.156.217:22993", "socks5://206.123.156.217:22879", "socks5://206.123.156.217:22781",
    "socks5://206.123.156.217:22604",
]

def _get_proxy_list() -> List[str]:
    env_proxies = os.getenv("PROXY_LIST")
    if env_proxies:
        return [p.strip() for p in env_proxies.split(",") if p.strip()]
    return STATIC_PROXIES.copy()

_proxy_cache = _get_proxy_list()

def _get_random_proxy() -> Optional[Dict[str, str]]:
    if not _proxy_cache:
        return None
    proxy_url = random.choice(_proxy_cache)
    return {"http": proxy_url, "https": proxy_url}

def fetch_xml_with_retry(timeout: int = 25, retries_direct: int = 2, retries_proxy: int = 20) -> Optional[ET.Element]:
    # Direct attempts
    for attempt in range(retries_direct):
        try:
            resp = requests.get(XML_FEED_URL, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                return ET.fromstring(resp.content)
            elif resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"Rate limit, waiting {wait}s")
                time.sleep(wait)
            else:
                time.sleep(3)
        except Exception:
            time.sleep(3)
    # Proxy attempts
    for attempt in range(retries_proxy):
        proxy = _get_random_proxy()
        if not proxy:
            break
        try:
            proxy_url = proxy["http"]
            display = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
            print(f"Proxy attempt {attempt+1}: {display}")
            resp = requests.get(XML_FEED_URL, timeout=timeout+5, proxies=proxy, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                print(f"Success via proxy {display}")
                return ET.fromstring(resp.content)
        except Exception:
            pass
        time.sleep(1)
    print("All fetch methods failed")
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
        t12, t24 = _convert_ff_time(time_raw)
        events.append({
            "name": title, "currency": country, "date": date, "time_raw": time_raw,
            "time_12h": t12, "time_24h": t24, "impact": "red",
            "forecast": forecast, "previous": previous,
        })
    return events

def fetch_and_filter_events(currency="USD", impact="High") -> List[Dict]:
    root = fetch_xml_with_retry()
    return parse_events(root, currency, impact)
