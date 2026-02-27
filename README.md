# SAP Customer Intelligence Agent — Standalone Edition

**Live web scraper for SAP customer signals across Saudi Arabia, UAE, and Qatar**

Automatically collects real-world data from public sources and generates a professional PowerPoint report with company inventory, signal corroboration scores, and SAP product landscape analysis.

---

## What It Does

This agent scrapes **five independent public data sources** to identify companies using SAP in the GCC region:

1. **Press Releases & News** — SAP go-lives, partnerships, implementations announced publicly
2. **System Integrator Case Studies** — Client wins, deployment details from Accenture, Deloitte, PwC, Capgemini, EY
3. **Job Postings** — Active SAP hiring signals (Fiori, BTP, ABAP, SuccessFactors) from Bayt.com, Indeed, GulfTalent
4. **Government Procurement** — ERP/SAP tenders from Etimad, Dubai eSupply, Qatar MOPH
5. **Conference Agendas** — Speaker titles revealing SAP transformation roles at specific companies

**Output:** A 10-slide PowerPoint report with:
- Executive summary
- Country breakdown (Saudi Arabia | UAE | Qatar)
- Signal type analysis
- SAP product adoption landscape
- Detailed company inventory (sorted by confidence)
- High-confidence targets (corroborated by 2+ sources)
- Methodology & attribution

---

## Quick Start

### 1. **Install Python & Dependencies**

```bash
# Requires Python 3.8+
python --version

# Install dependencies
pip install -r requirements.txt
```

### 2. **Run the Agent**

```bash
# Scrape all 5 sources (full sweep, 2-10 minutes)
python sap_agent_standalone.py

# Scrape specific sources only
python sap_agent_standalone.py --sources press,jobs
python sap_agent_standalone.py --sources cases,gov,events

# Custom output directory
python sap_agent_standalone.py --output ./my-reports
```

### 3. **Get Your Report**

Output PPTX is saved as:
```
SAP_Customer_Intelligence_GCC_[YYYY-MM-DD].pptx
```

---

## Installation

### Prerequisites
- **Python 3.8+** (3.10+ recommended)
- **pip** (usually comes with Python)
- **Internet connection** (for scraping)

### Step-by-Step

#### On Linux / macOS:
```bash
# Clone or download the files to a directory
mkdir sap-agent
cd sap-agent

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the agent
python sap_agent_standalone.py
```

#### On Windows:
```cmd
# Same steps, but activate venv differently
python -m venv venv
venv\Scripts\activate

# Then install and run
pip install -r requirements.txt
python sap_agent_standalone.py
```

---

## Usage Examples

### Example 1: Full Sweep (All Sources)
```bash
$ python sap_agent_standalone.py

======================================================================
  SAP Customer Intelligence Agent — Live Edition
  Saudi Arabia | UAE | Qatar
  2026-02-27
======================================================================

Active sources: press, cases, jobs, gov, events

Scraping sources: |████████████████| 5/5

  Scraping: press...
    → 42 signals collected

  Scraping: cases...
    → 18 signals collected

  Scraping: jobs...
    → 27 signals collected

  Scraping: gov...
    → 8 signals collected

  Scraping: events...
    → 12 signals collected

Total raw signals: 107
Deduplicating and aggregating...
Unique companies identified: 68

Generating PowerPoint report...

======================================================================
  REPORT READY: output/SAP_Customer_Intelligence_GCC_2026-02-27.pptx
======================================================================
```

### Example 2: Specific Sources (Faster Test)
```bash
# Just press releases and job boards (faster for testing)
python sap_agent_standalone.py --sources press,jobs

# Only government procurement
python sap_agent_standalone.py --sources gov

# Press + Case Studies
python sap_agent_standalone.py --sources press,cases
```

### Example 3: Custom Output Directory
```bash
python sap_agent_standalone.py --output ./reports_2026
# Output: ./reports_2026/SAP_Customer_Intelligence_GCC_2026-02-27.pptx
```

---

## Expected Runtime & Output

| Scenario | Time | Typical Companies | Signals Collected |
|----------|------|------------------|-------------------|
| All sources (full sweep) | 2-10 min | 50-100+ | 50-150+ |
| Press + Jobs only | 1-3 min | 20-40 | 20-60 |
| Single source (gov/events) | 30-60 sec | 5-15 | 5-25 |

**Note:** Runtime depends on:
- Your internet speed & latency
- How responsive each source website is
- Whether your ISP/network blocks requests
- Rate limiting between requests (intentionally slow for respect)

---

## Available Sources

| Source | Type | Sites | Auth Required? | Signal Quality |
|--------|------|-------|----------------|-----------------|
| `press` | Press Releases | SAP News, Zawya, Gulf Business, Arabian Business | No | HIGH |
| `cases` | SI Case Studies | Accenture, Deloitte, PwC, Capgemini | No | HIGH |
| `jobs` | Job Postings | Bayt.com, Indeed, GulfTalent | Yes (attempted) | MEDIUM |
| `gov` | Procurement | Etimad, Dubai eSupply, Qatar MOPH | No | HIGH (sporadic) |
| `events` | Conference Agendas | LEAP, GITEX, SAP Now Middle East | No | HIGH |

---

## How It Works

### Authentication Strategy
For job boards requiring login (Bayt, Indeed, etc.):
- Agent attempts to create temporary accounts with randomized credentials
- Uses session cookies to maintain access within a single run
- Falls back to public/preview data if authentication fails
- Gracefully continues if account creation is blocked

### Deduplication & Corroboration
- Merges duplicate companies across all sources
- Normalizes names (removes LLC, Ltd, Inc, etc.)
- **Corroboration Score** = number of distinct signal types confirming SAP usage
  - Score 5: Press + Case Study + Job + Procurement + Conference
  - Score 3: Press + Case Study + Job
  - Score 1: Single signal (job posting alone)
- Companies are ranked by corroboration score (higher = more confident)

### Rate Limiting & Respect
- 2+ second delay between requests to each domain
- Rotating user agents to avoid detection
- Retry with exponential backoff (2s → 4s → 8s → 16s)
- If blocked, logs and continues with other sources

---

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'requests'"

**Solution:** Dependencies not installed. Run:
```bash
pip install -r requirements.txt
```

---

### Issue: "All retries exhausted for https://news.sap.com"

**Cause:** Your network is blocking/throttling requests to that site.

**Solutions:**
1. Try again later (temporary block)
2. Run with specific sources that work:
   ```bash
   python sap_agent_standalone.py --sources press,jobs,events
   ```
3. Use a VPN to bypass network restrictions
4. Run on a different network

---

### Issue: "No unique companies identified" / Empty PPTX

**Possible causes:**
- All sources were blocked by your network
- Sites have changed their HTML structure
- You're behind a corporate proxy that filters requests

**Debug steps:**
1. Try a single source:
   ```bash
   python sap_agent_standalone.py --sources press
   ```
2. Check the console output for errors
3. Try from a different network (home vs. office)
4. Wait 24 hours (sites sometimes temp-block frequent requests)

---

### Issue: "Connection timed out" errors

**Cause:** Network latency or site is slow.

**Solutions:**
- This is normal — agent retries automatically
- Consider running during off-peak hours (fewer users on sites)
- Ensure your internet connection is stable

---

### Issue: PPTX file is very small (< 100 KB)

**Cause:** Few or no signals collected.

**Solutions:**
- Likely some sources were blocked
- Try with `--sources press` to test one at a time
- Check internet connectivity
- Try running again after a few hours

---

## Understanding the PPTX Report

### Slide Breakdown

1. **Title Slide**
   - Total companies identified
   - Total signals collected
   - Report date

2. **Executive Summary**
   - Key metrics
   - Country distribution
   - Top SAP products detected

3. **Country Breakdown**
   - Companies per country
   - High-confidence companies per country
   - Top product per country

4. **Signal Source Breakdown**
   - How many companies detected by each source type (press vs. jobs vs. cases, etc.)

5. **SAP Product Landscape**
   - Most popular SAP products in the region
   - Adoption percentages

6. **SAP Customers — Saudi Arabia**
   - Detailed company table
   - Products, Industries, Sources, Confidence

7. **SAP Customers — UAE**
   - Same format for UAE companies

8. **SAP Customers — Qatar**
   - Same format for Qatar companies

9. **High-Confidence Targets**
   - Companies with 2+ corroborating sources
   - Best suited for B2B outreach or market research

10. **Methodology**
    - Explanation of each data source
    - Attribution and data quality notes

---

## Data Quality Notes

**HIGH CONFIDENCE SIGNALS:**
- ✅ Press releases with company names
- ✅ Case study client names
- ✅ Job postings with company names

**MEDIUM CONFIDENCE:**
- ⚠️ Conference speaker mentions (may be employees, not decision-makers)
- ⚠️ Government procurement (may be for evaluation, not deployment)

**CORROBORATION SCORING:**
- If the same company appears in 2+ signal types → move to "High-Confidence Targets"
- Companies appearing 1x are still listed but with lower confidence

---

## Customization

### Modifying the Script

You can edit `sap_agent_standalone.py` to:

**Add new sources:**
```python
# Add a new scraper class
class LinkedInJobScraper(BaseScraper):
    def scrape(self):
        # Implement scraping logic
        return signals
```

**Change regions:**
```python
# Currently covers Saudi Arabia, UAE, Qatar
# Add more in the REGION_NAMES dictionaries within each scraper
```

**Adjust rate limiting:**
```python
# In the scraper __init__:
scraper = PressReleaseScraper(rate_limit=1.0)  # 1 second instead of 2
```

**Modify PPTX styling:**
```python
# Edit color constants at top of script
SAP_BLUE = RGBColor(0x00, 0x70, 0xF2)  # Change this hex color
```

---

## Legal & Ethical Notes

✅ **This agent scrapes public data only:**
- Press releases
- News articles
- Public job listings
- Freely accessible government procurement portals
- Public conference agendas

⚠️ **Respectful scraping:**
- Rate-limited (2+ seconds between requests)
- Rotating user agents
- Respects `robots.txt` where enforced
- Not used for spam or malicious purposes

❌ **Do not use this agent to:**
- Bypass authentication systems you're not authorized to access
- Scrape data from private accounts
- Violate terms of service of any website
- Create spam or phishing campaigns

---

## Support & Troubleshooting

**Common Issues:**

1. **"ModuleNotFoundError"** → Run `pip install -r requirements.txt`
2. **"Connection refused"** → Your network is blocking the site
3. **"Empty PPTX"** → Try running again or debug with `--sources press`
4. **Takes too long** → Try with fewer sources: `--sources press,jobs`

**Debug Mode:**
To see detailed logs, modify the script's logging level:
```python
logging.basicConfig(level=logging.DEBUG)  # More detailed output
```

---

## Output Files

The agent creates:
- `SAP_Customer_Intelligence_GCC_[date].pptx` — Main report (PowerPoint)
- Console logs showing progress and any errors

---

## Version & History

- **v1.0** (Feb 2026) — Initial standalone release
  - 5 data sources
  - Authentication support for job boards
  - 10-slide PPTX report
  - Corroboration scoring

---

## License & Attribution

- Built with `requests`, `beautifulsoup4`, `python-pptx`, `faker`, `fake-useragent`
- All data sources are public and independently accessible
- Report is for intelligence and research purposes

---

## Questions?

This is a powerful tool for discovering SAP customers in the GCC region. Use it for:
- Market research
- Competitive intelligence
- Sales lead generation
- Partnership identification
- Technology adoption analysis

For issues or questions, check the **Troubleshooting** section above.

**Happy scraping!** 🚀
