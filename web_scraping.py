import re
import pandas as pd
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError, Error as PWError
import mysql.connector
from dotenv import load_dotenv
import os
import math
from sqlalchemy import create_engine
from typing import List, Tuple, Optional
from urllib.parse import urlencode, quote_plus


load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "phpmyadmin")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "oilwell_pdf_extraction")

# read well information table from database containing information extracted from PDF
def read_table():
    conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    cur.execute(f"USE {DB_NAME}")
    
    sql = """
        SELECT well_name, api FROM well_header;
    """

    df = pd.read_sql(sql, conn)

    conn.commit()
    cur.close()
    conn.close()

    return df

df = read_table()

# create a list contains all well name and API
well_list = []
for i in range(len(df)):
    well_name = df.iloc[i, 0]
    well_api = df.iloc[i, 1]
    well_list.append((well_name, well_api))

# extract well_status, well_type, closest_city, oil_badge, gas_badge from web and store in a pandas DataFrame
# if the field value is not exist, it will use N/A to represent the missing value
FAST_NAV_TIMEOUT   = 6000  
FAST_CLICK_TIMEOUT = 1500  
PER_WELL_TIMEOUT   = 18.0  

# regex for number tokens like 2.1k
NUM_TOKEN = r"[0-9][0-9.,]*\s*[kKmMbB]?"
MEMBERS_ONLY = re.compile(r"^\s*Members?\s+Only\s*$", re.I)

def _norm(s: Optional[str]) -> str:
    if not s: return "N/A"
    s = s.strip()
    return "N/A" if (not s or MEMBERS_ONLY.match(s)) else s

# expected output table's columns
OUT_COLS = ["well_name","api","well_status","well_type","closest_city","oil_badge","gas_badge"]

# create a dictionary to store initial values
def blank_row(well_name: str, api: str) -> dict:
    return {
        "well_name": well_name,
        "api": api,
        "well_status": "N/A",
        "well_type": "N/A",
        "closest_city": "N/A",
        "oil_badge": "N/A",
        "gas_badge": "N/A",
    }

# extract field values in the table on the web page
async def get_table_value_any(page, label: str) -> str:
    td = page.locator(f"xpath=(//th[normalize-space()='{label}']/following-sibling::td[1])[1]").first
    if await td.count():
        return _norm(await td.inner_text())
    td2 = page.locator(f"xpath=(//th[contains(normalize-space(), '{label}')]/following-sibling::td[1])[1]").first
    if await td2.count():
        return _norm(await td2.inner_text())
    return "N/A"

# extract numeric values fo barrels of oil produce
async def get_oil_badge(page) -> str:
    loc = page.locator("p.block_stat:has-text('Barrels of Oil Produced') span.dropcap").first
    if await loc.count():
        v = (await loc.inner_text()).strip()
        return v if v else "N/A"
    loc = page.locator(
        "xpath=(//p[contains(@class,'block_stat')][contains(., 'Barrels of Oil Produced')]"
        "//span[contains(@class,'dropcap')])[1]"
    )
    return (await loc.inner_text()).strip() if await loc.count() else "N/A"

# extract numeric values fo barrels of gas produce
async def get_gas_badge(page) -> str:
    loc = page.locator("p.block_stat:has-text('MCF of Gas Produced') span.dropcap").first
    if await loc.count():
        v = (await loc.inner_text()).strip()
        return v if v else "N/A"
    loc = page.locator(
        "xpath=(//p[contains(@class,'block_stat')][contains(., 'MCF of Gas Produced')]"
        "//span[contains(@class,'dropcap')])[1]"
    )
    return (await loc.inner_text()).strip() if await loc.count() else "N/A"

# call extraction functions above and collect all required field values from web
async def extract_required_fields(page) -> dict:
    vals = await asyncio.gather(
        get_table_value_any(page, "Well Status"),
        get_table_value_any(page, "Well Type"),
        get_table_value_any(page, "Closest City"),
        get_oil_badge(page),
        get_gas_badge(page),
 )
    return {
        "well_status":  _norm(vals[0]),
        "well_type":    _norm(vals[1]),
        "closest_city": _norm(vals[2]),
        "oil_badge":    _norm(vals[3]),
        "gas_badge":    _norm(vals[4]),
    }

# open search results page by changing the URL of the pages with prefilled parameters (well_name  and api)
# click the first matching link to open the detail page
async def search_and_open_detail(page, well_name: str, api_num: str) -> bool:
    page.set_default_timeout(FAST_NAV_TIMEOUT)
    q = {
        "type": "wells",
        "operator_name": "",
        "well_name": well_name,
        "api_no": api_num,
        "lease_key": "",
        "state": "",
        "county": "",
        "section": "",
        "township": "",
        "range": "",
        "min_boe": "",
        "max_boe": "",
        "min_depth": "",
        "max_depth": "",
        "field_formation": "",
    }
    url = "https://www.drillingedge.com/search?" + urlencode(q, quote_via=quote_plus)
    await page.goto(url, wait_until="domcontentloaded")

    for sel in ("button:has-text('Accept')", "button:has-text('Agree')", "button:has-text('Close')"):
        try:
            btn = page.locator(sel).first
            if await btn.count(): await btn.click(timeout=800)
        except Exception:
            pass

    candidates = [
        f"a[href*='/{api_num}']:visible",
        "a[href*='/wells/']:visible",
        f"a:has-text('{well_name}')",
        "ul.search-results a:visible",
        ".results a:visible",
    ]
    for css in candidates:
        link = page.locator(css).first
        try:
            if await link.count():
                await link.click(timeout=FAST_CLICK_TIMEOUT)
                await page.wait_for_load_state("domcontentloaded", timeout=FAST_NAV_TIMEOUT)
                return True
        except Exception:
            continue

    link = page.locator(f"a:has-text('{api_num}')").first
    try:
        if await link.count():
            await link.click(timeout=FAST_CLICK_TIMEOUT)
            await page.wait_for_load_state("domcontentloaded", timeout=FAST_NAV_TIMEOUT)
            return True
    except Exception:
        pass

    return False

# organize and call functions above to execute launch the browser, navigate through search URL, extract required fields, return a dictionary containing values
async def fetch_one(well_name: str, api_num: str, per_well_timeout: float = PER_WELL_TIMEOUT) -> dict:
    safe_prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", well_name)[:40]

    async def _inner():
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"]
            )
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 900},
                java_script_enabled=True,
                bypass_csp=True,
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/122.0.0.0 Safari/537.36"),
                ignore_https_errors=True,
            )
            page = await ctx.new_page()
            try:
                base = blank_row(well_name, api_num)  # default row with all lowercase keys

                ok = await search_and_open_detail(page, well_name, api_num)
                if not ok:
                    return base

                try:
                    await page.wait_for_selector("text=Well Details", timeout=1800)
                except Exception:
                    pass

                data = await extract_required_fields(page)
                base.update(data)  # merge into the lowercase template
                return base

            except Exception as e:
                try:
                    await page.screenshot(path=f"fail_{safe_prefix}.png", full_page=True)
                    html = await page.content()
                    with open(f"fail_{safe_prefix}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                except Exception:
                    pass
                return blank_row(well_name, api_num)
            finally:
                if not page.is_closed(): await page.close()
                await ctx.close(); await browser.close()

    return await asyncio.wait_for(_inner(), timeout=per_well_timeout)

# iterate through the dataframe for each pair of well name and api and repeat above process
async def run_to_dataframe(wells: List[Tuple[str, str]], per_well_timeout: float = PER_WELL_TIMEOUT) -> pd.DataFrame:
    rows = []
    for name, api in wells:
        rows.append(await fetch_one(name, api, per_well_timeout=per_well_timeout))
        
    df = pd.DataFrame(rows)
    return df.reindex(columns=OUT_COLS)

# final table contains well information from the webl
web_df = asyncio.run(run_to_dataframe(well_list))
web_df = web_df.replace("N/A", pd.NA)
print(web_df.head())

# convert this dataframe to a sql table
cols_def = ", ".join(f"`{c}` TEXT" for c in web_df.columns)

def to_str(x):
    if x is None: return ""
    if isinstance(x, float) and math.isnan(x): return ""
    return str(x)

rows = [tuple(to_str(v) for v in rec) for rec in web_df.itertuples(index=False, name=None)]

placeholders = ", ".join(["%s"] * len(web_df.columns))
colnames = ", ".join(f"`{c}`" for c in web_df.columns)
insert_sql = f"INSERT INTO web_table ({colnames}) VALUES ({placeholders})"

conn = mysql.connector.connect(
    host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
)
cur1 = conn.cursor(buffered=True)
cur1.execute("DROP TABLE IF EXISTS web_table")
cur1.execute(f"CREATE TABLE web_table ({cols_def})")
conn.commit()
cur1.close()
       
if rows:
    cur2 = conn.cursor(buffered=True) 
    cur2.executemany(insert_sql, rows)
    conn.commit()
cur2.close()


# join the table of pdf information with the table of web information and store it as a new table
cur3 = conn.cursor(buffered=True)
cur3.execute("DROP TABLE IF EXISTS well_info") 
sql = """
    CREATE TABLE well_info AS
    SELECT a.*, b.well_status, b.well_type, b.closest_city, b.oil_badge, b.gas_badge
    FROM well_header AS a
    LEFT JOIN web_table AS b
        ON a.well_name = b.well_name AND a.api = b.api
"""
cur3.execute(sql)
conn.commit()
cur3.close()

cur4 = conn.cursor(buffered=True) 
cur4.execute("SELECT * FROM well_info")
for r in cur4:
    print(r)
cur4.close()
conn.close()



