# forexfactory_xml.py
import xml.etree.ElementTree as ET
import requests
from typing import List, Dict, Optional
from datetime import datetime

XML_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"

def fetch_xml(timeout: int = 15) -> Optional[ET.Element]:
    try:
        response = requests.get(XML_FEED_URL, timeout=timeout)
        response.raise_for_status()
        return ET.fromstring(response.content)
    except Exception as e:
        print(f"XML fetch error: {e}")
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
    root = fetch_xml()
    return parse_events(root, currency, impact)
