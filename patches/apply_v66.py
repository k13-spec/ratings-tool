import re, sys, json
P='assets/debt_financing_ideas.html'
html=open(P,encoding='utf-8').read()
orig=html

def card_span(name):
    i=html.find('name:"'+name)
    if i<0: i=html.find('"name":"'+name)
    if i<0: raise SystemExit('card not found: '+name)
    s=html.rfind('{',0,i)
    depth=0;j=s;instr=None
    while j<len(html):
        c=html[j]
        if instr:
            if c=='\\': j+=2; continue
            if c==instr: instr=None
        elif c in '"\'`': instr=c
        elif c=='{': depth+=1
        elif c=='}':
            depth-=1
            if depth==0: return s,j+1
        j+=1
    raise SystemExit('unbalanced '+name)

def field_value_span(cs,ce,key):
    """return (vs,ve) absolute span of the value for key inside card [cs,ce)"""
    seg=html[cs:ce]
    m=re.search(r'(?<![\w"])"?'+re.escape(key)+r'"?\s*:\s*',seg)
    if not m: return None
    vs=cs+m.end()
    c=html[vs]
    if c=='"':
        j=vs+1
        while True:
            if html[j]=='\\': j+=2; continue
            if html[j]=='"': return vs,j+1
            j+=1
    if c in '[{':
        openc=c; closec=']' if c=='[' else '}'
        depth=0;j=vs;instr=None
        while True:
            ch=html[j]
            if instr:
                if ch=='\\': j+=2; continue
                if ch==instr: instr=None
            elif ch in '"\'`': instr=ch
            elif ch==openc: depth+=1
            elif ch==closec:
                depth-=1
                if depth==0: return vs,j+1
            j+=1
    raise SystemExit('unexpected value type for '+key)

def esc(s): return s.replace('\\','\\\\').replace('"','\\"')

def append_rationale(name,text):
    global html
    cs,ce=card_span(name); vs,ve=field_value_span(cs,ce,'rationale')
    html=html[:ve-1]+' '+esc(text)+html[ve-1:]

def add_kv(name,d):
    global html
    cs,ce=card_span(name); vs,ve=field_value_span(cs,ce,'kv')
    ins=''.join(',"%s":"%s"'%(esc(k),esc(v)) for k,v in d.items())
    html=html[:ve-1]+ins+html[ve-1:]

def add_list(name,key,items):
    global html
    cs,ce=card_span(name); sp=field_value_span(cs,ce,key)
    if sp is None:
        # add field before srcs
        vs,ve=field_value_span(cs,ce,'srcs')
        m=re.search(r'(?<![\w"])"?srcs"?\s*:\s*',html[cs:ce]); ks=cs+m.start()
        arr='['+','.join('"%s"'%esc(i) for i in items)+']'
        html=html[:ks]+key+':'+arr+', '+html[ks:]
        return
    vs,ve=sp
    ins=''.join(',"%s"'%esc(i) for i in items)
    html=html[:ve-1]+ins+html[ve-1:]

def add_srcs(name,items):
    global html
    cs,ce=card_span(name); vs,ve=field_value_span(cs,ce,'srcs')
    ins=''.join(',["%s","%s"]'%(esc(t),esc(u)) for t,u in items)
    html=html[:ve-1]+ins+html[ve-1:]

def set_rating(name,newr):
    global html
    cs,ce=card_span(name); vs,ve=field_value_span(cs,ce,'rating')
    html=html[:vs]+'"'+esc(newr)+'"'+html[ve:]

def replace_in_card(name,old,new,count=1):
    global html
    cs,ce=card_span(name); seg=html[cs:ce]
    if seg.count(old)!=count: raise SystemExit('replace_in_card %s: %d occurrences of %r'%(name,seg.count(old),old[:60]))
    html=html[:cs]+seg.replace(old,new)+html[ce:]

def replace_watch(n_prefix,append_text):
    global html
    i=html.find('{n:"'+n_prefix)
    if i<0: raise SystemExit('watch not found '+n_prefix)
    # find w:" ... closing
    ws=html.find('w:"',i)+3
    j=ws
    while True:
        if html[j]=='\\': j+=2; continue
        if html[j]=='"': break
        j+=1
    html=html[:j]+' '+esc(append_text)+html[j:]

# ---------------- CARD UPDATES ----------------
U='<b>Update (3-Sep-26):</b> '

# 1 Vedanta
append_rationale("Vedanta Ltd",U+"the entity-level re-papering has started printing. Vedanta Aluminium Metal closed a ₹13,500 cr 6.5–7-yr term-loan refinancing of pre-demerger debt at ~7.9–8.0% (Axis ₹5,500 cr; HDFC Bank + ICICI Bank ₹8,000 cr; reported 6-Aug-26), and India Ratings upgraded its NCDs IND AA− → IND AA+/Stable (14-Aug-26) on sub-2.0x leverage. At the parent, Vedanta Ltd is preparing a third rupee bond of 2026 — at least ₹1,000 cr, 3–7-yr tenor, largely refinancing, launch guided within weeks (27-Aug-26; bankers not yet named) after ₹7,575 cr across two March sales. Vedanta Resources formally denied stake-sale speculation in VEDL/Vedanta Aluminium (26-Aug-26); Agarwal frames a VRL relisting as a ~3-yr option. Still open: the buyback tender results and any Vedanta Oil & Gas / Iron & Steel debut prints ⚑.")
add_kv("Vedanta Ltd",{"VAML refi (Aug-26)":"₹13,500 cr TL, 6.5–7-yr @~7.9–8.0% — Axis ₹5,500 cr; HDFC+ICICI ₹8,000 cr","Next parent NCD":"≥₹1,000 cr, 3–7-yr, refi — launch guided Sep-26 (third of 2026 after ₹7,575 cr in Mar)"})
add_list("Vedanta Ltd","maturities",["Vedanta Ltd third 2026 NCD (≥₹1,000 cr, 3–7-yr) — mandate window open now (27-Aug-26)","VAML pre-demerger debt refinanced via ₹13,500 cr 6.5–7-yr TL (Aug-26) — bank-led; NCD take-out optionality later"])
add_list("Vedanta Ltd","finhist",["Aug 2026: VAML ₹13,500 cr term-loan refi (Axis/HDFC/ICICI) at ~7.9–8.0%; parent lines up ≥₹1,000 cr NCD (third of 2026)","Mar 2026: two Vedanta Ltd bond sales totalling ₹7,575 cr (incl. ₹2,575 cr NCD with ICICI/Kotak among investors)"])
add_list("Vedanta Ltd","ratingevo",["14-Aug-26: India Ratings upgrades Vedanta Aluminium Metal NCDs IND AA− → IND AA+/Stable (leverage <2.0x post-demerger)"])
add_srcs("Vedanta Ltd",[["Vedanta plans third rupee bond of 2026 (≥₹1,000 cr) — Business Standard, 27-Aug-26","https://www.business-standard.com/companies/news/vedanta-plans-third-rupee-bond-issue-seeks-to-raise-1-000-crore-126082700777_1.html"],["Vedanta Aluminium ₹13,500 cr loan post-demerger — Business Standard, 6-Aug-26","https://www.business-standard.com/companies/news/vedanta-aluminium-raises-13-500-crore-loan-after-group-s-demerger-126080600226_1.html"],["Ind-Ra upgrades VAML NCDs to IND AA+/Stable — Upstox, 14-Aug-26","https://upstox.com/news/market-news/stocks/vedanta-aluminium-metal-india-ratings-upgrades-nc-ds-to-ind-aa-with-stable-outlook-know-key-points/article-198646/"],["Vedanta denies stake-sale reports — Business Today, 26-Aug-26","https://www.businesstoday.in/latest/corporate/story/vedanta-denies-reports-of-stake-sale-in-vedanta-ltd-vedanta-aluminium-metal-or-group-companies-551543-2026-08-26"]])

# 2 Reliance
append_rationale("Reliance Industries",U+"the Jio Platforms IPO cleared its regulatory gate — SEBI issued the observation letter on 28-Aug-26 (DRHP filed 19-Jun-26): fresh issue of up to 27 crore shares, no OFS, sized at ~₹37,700 cr (India's largest), with up to ₹27,500 cr earmarked for debt repayment at JPL — a deleveraging event for the Jio borrowing entities rather than new supply, but it re-opens the group's capital-allocation calendar (launch/pricing not yet set). A Fitch upgrade of RIL's local-currency issuer rating to 'A−'/Stable is reported (~29-Aug-26) ⚑ headline only — prior level and rationale unverified. No new parent NCD print in the window.")
add_kv("Reliance Industries",{"Jio Platforms IPO":"SEBI observation letter 28-Aug-26 — fresh issue ≤27 cr shares, ~₹37,700 cr; up to ₹27,500 cr for debt repayment; RIL holds 66.43%"})
add_list("Reliance Industries","maturities",["Jio Platforms IPO proceeds (up to ₹27,500 cr) earmarked for debt repayment — JPL-level facilities to be prepaid/refinanced post-listing ⚑ which lines"])
add_list("Reliance Industries","finhist",["Aug 2026: Jio Platforms IPO receives SEBI observations (28-Aug); JPL FY26 revenue ₹1,46,885 cr (+14.6%), 524.4mn subscribers"])
add_list("Reliance Industries","ratingevo",["~29-Aug-26: Fitch upgrades RIL local-currency issuer rating to A−/Stable ⚑ (headline only; prior level unverified)"])
add_srcs("Reliance Industries",[["Jio Platforms receives SEBI observation letter — Business Standard, 29-Aug-26","https://www.business-standard.com/markets/capital-market-news/ril-says-jio-platforms-receives-sebi-observation-letter-for-proposed-ipo-126082900085_1.html"],["Fitch upgrades RIL LC issuer rating to A− — ScanX (⚑ headline)","https://scanx.trade/stock-market-news/companies/fitch-upgrades-ril-local-currency-issuer-rating-outlook-stable/49454889"]])

# 3 KRT
append_rationale("Knowledge Realty",U+"sponsor sell-down has arrived: Blackstone launched an OFS of up to 25.03% of KRT (base 16.69% ~₹7,990 cr + greenshoe 8.34% ~₹3,996 cr; ~1.11bn units; floor ₹108/unit, ~4.7% below the prior close and ~13% below NAV) — up to ~₹11,988 cr (~US$1.25bn), opening 31-Aug (non-retail) / 1-Sep (retail). If fully exercised Blackstone drops from 46.51% to ~21.5% and Sattva (32%) becomes the largest unitholder. Credit read-through: the trust's balance sheet is untouched (net debt ₹12,100 cr, ~18% LTV), but sponsor strength is an explicit AAA rating factor — watch for ICRA/CRISIL commentary on the sponsor-mix change ⚑; the wider float should deepen the unit-holder base ahead of the next NCD/CP tranche. Outcome not yet reported.")
add_kv("Knowledge Realty",{"Blackstone OFS (31-Aug/1-Sep-26)":"up to 25.03% (~₹11,988 cr / US$1.25bn); floor ₹108/unit; Blackstone → ~21.5%, Sattva becomes largest unitholder"})
add_list("Knowledge Realty","finhist",["Aug-26: Blackstone OFS of up to 25.03% at ₹108 floor (~₹11,988 cr) — first sponsor sell-down since listing"])
add_list("Knowledge Realty","ratingevo",["Watch item (Aug-26): sponsor mix shifts Blackstone → Sattva-led after the OFS — AAA sponsor-linkage factor to be re-tested by ICRA/CRISIL ⚑"])
add_srcs("Knowledge Realty",[["Blackstone to sell up to US$1.25bn of KRT via OFS — Business Standard, 28-Aug-26","https://www.business-standard.com/companies/news/blackstone-to-sell-1-25-billion-stake-in-india-s-knowledge-realty-trust-126082801344_1.html"]])

# 4 AESL
append_rationale("Adani Energy Solutions",U+"another TBCB SPV package: AESL won the Satara Power Transmission project in Maharashtra (₹4,700 cr; 562 ckm + 9,000 MVA incl. a 765/400 kV Satara substation and the Kolhapur–Satara 765 kV D/C line; 36-month build; evacuates ~4,500 MW of RE/pumped storage) — cumulative network 29,739 ckm / 143,425 MVA, and each win is a fresh ~70:30 debt-funded SPV package.")
add_kv("Adani Energy Solutions",{"Satara TBCB win (26-Aug-26)":"₹4,700 cr; 562 ckm + 9,000 MVA; 36-month build"})
add_list("Adani Energy Solutions","maturities",["Satara (₹4,700 cr) + Andhra (~₹8,500 cr) SPVs — construction facilities to be tied up FY27 (~70:30 debt-funded)"])
add_list("Adani Energy Solutions","finhist",["Aug 2026: Satara Power Transmission TBCB SPV won (₹4,700 cr); network 29,739 ckm / 143,425 MVA"])
add_srcs("Adani Energy Solutions",[["AESL wins ₹4,700 cr Satara transmission project — Business Standard, 26-Aug-26","https://www.business-standard.com/companies/news/adani-energy-solutions-wins-4-700-cr-transmission-project-in-maharashtra-126082600192_1.html"]])


# 5 TPREL

append_rationale("Tata Power Renewable",U+"ICRA reaffirmed [ICRA]AA+ (Stable) on 20-Aug-26 (ratings DB row; rationale ⚑ not yet read). Execution is converting to operating assets: 190.5 MW of the 460 MW SJVN FDRE tranche-1 (with BESS) commissioned at Kalasar, Bikaner (25-Aug-26) and a 72.5 MW captive solar plant for Tata Steel (27-Aug-26) — portfolio now ~12.3 GW with 7.0 GW operational (5.7 GW solar / 1.3 GW wind) and ~5.3 GW under development over 6–24 months, i.e. a rolling COD → take-out calendar. No fresh TPREL/parent NCD print in the window.")
add_kv("Tata Power Renewable",{"Portfolio (Aug-26)":"~12.3 GW — 7.0 GW operational (5.7 solar / 1.3 wind), ~5.3 GW under development"})
add_list("Tata Power Renewable","finhist",["Aug 2026: 190.5 MW FDRE (SJVN tranche-1, with BESS) + 72.5 MW Tata Steel captive solar commissioned; ICRA AA+/Stable reaffirmed (20-Aug)"])
add_list("Tata Power Renewable","ratingevo",["20-Aug-26: ICRA reaffirms [ICRA]AA+ (Stable) — ratings DB row ⚑ rationale"])
add_srcs("Tata Power Renewable",[["TPREL 72.5 MW captive solar for Tata Steel — Tata Power release, 27-Aug-26","https://www.tatapower.com/news-and-media/media-releases/tata-power-renewables-powers-tata-steel-s-decarbonization-journey-with-72-5-mw-captive-solar-project-in-rajasthan"],["TPREL commissions 190.5 MW FDRE — Renewable Watch, 25-Aug-26","https://renewablewatch.in/2026/08/25/tata-power-commissions-190-5-mw-fdre-project-in-rajasthan/"]])

# 6 IndiGrid
append_rationale("IndiGrid",U+"the repricing window printed: IndiGrid allotted ₹1,100 cr of AAA senior secured NCDs on 20-Aug-26 — Series AI ₹350 cr 5-yr (mat. 20-Aug-31) + Series AJ ₹750 cr 10-yr (mat. 20-Aug-36), both at 7.44% p.a. quarterly, partly paid (₹220 cr / 20% paid up at allotment). CRISIL and ICRA assigned AAA/Stable to the tranche on 17-Aug-26 while reaffirming ₹14,017 cr of existing NCDs, ₹5,120 cr of bank loans, a ₹950 cr TL and ₹500 cr CP (A1+) — the stack itemisation ⚑ is now largely closed at ~₹20,600 cr of rated debt.")
add_kv("IndiGrid",{"Aug-26 NCD":"₹1,100 cr @7.44% — ₹350 cr 5-yr + ₹750 cr 10-yr; partly paid (20% at allotment)","Rated stack (Aug-26)":"NCDs ₹14,017 cr + ₹1,100 cr new · bank loans ₹5,120 cr · TL ₹950 cr · CP ₹500 cr"})
add_list("IndiGrid","maturities",["Series AI ₹350 cr 7.44% due 20-Aug-2031 · Series AJ ₹750 cr 7.44% due 20-Aug-2036 (allotted 20-Aug-26; balance calls on partly-paid NCDs to follow)"])
add_list("IndiGrid","finhist",["Aug 2026: ₹1,100 cr AAA NCDs allotted at 7.44% (5-yr/10-yr, partly paid)"])
add_list("IndiGrid","ratingevo",["17-Aug-26: CRISIL + ICRA AAA/Stable assigned to ₹1,100 cr NCDs; reaffirmed on ₹14,017 cr NCDs, ₹5,120 cr bank loans, ₹950 cr TL, ₹500 cr CP (A1+)"])
add_srcs("IndiGrid",[["IndiGrid allots ₹1,100 cr AAA NCDs @7.44% — ScanX, 20-Aug-26","https://scanx.trade/stock-market-news/debt-markets/indigrid-infrastructure-trust-allots-1-100-crore-aaa-rated-ncds/48774803"],["CRISIL/ICRA AAA on IndiGrid proposed NCDs — SolarQuarter, 17-Aug-26","https://solarquarter.com/2026/08/17/indigrid-receives-aaa-stable-ratings-from-crisil-and-icra-for-proposed-debt-instruments/"]])

# 7 ABReL
append_rationale("Aditya Birla Renewables",U+"the acquisition-finance leg is now sized: MUFG is sole MLA, underwriter and bookrunner on a US$1.6bn (~₹15,317 cr) term loan to ABReL funding the purchase of Solenergi Power (Sprng Energy) from Shell Overseas Investment — EV ~₹17,322 cr, balance equity from Grasim and GIP (BlackRock); portfolio moves to ~9.3 GWp; close expected before end-CY26 (reported 24-Aug-26; a 9fin note of 31-Jul-26 had ABG sounding local banks for a ~₹14,000 cr loan). The MUFG underwrite is the sell-down/refi to chase — INR take-out of the acquisition TL into SPV pools and platform NCDs as Sprng assets are re-papered. The SPV Watch Developing (22-Jul-26) is still unresolved ⚑ — a larger, more leveraged platform post-Sprng is now part of that resolution.")
add_kv("Aditya Birla Renewables",{"Sprng acquisition (Aug-26)":"Solenergi Power from Shell — EV ~₹17,322 cr; US$1.6bn (~₹15,317 cr) MUFG-underwritten TL + Grasim/GIP equity; portfolio → ~9.3 GWp; close by end-CY26"})
add_list("Aditya Birla Renewables","maturities",["US$1.6bn Sprng acquisition TL (MUFG underwrite) — syndication/sell-down FY27, INR take-out thereafter"])
add_list("Aditya Birla Renewables","finhist",["Aug-26: US$1.6bn MUFG-underwritten acquisition loan for Sprng Energy (Solenergi) — EV ~₹17,322 cr; platform → ~9.3 GWp"])
add_srcs("Aditya Birla Renewables",[["ABReL secures US$1.6bn MUFG loan for Sprng Energy — Yahoo Finance, 24-Aug-26","https://finance.yahoo.com/energy/articles/aditya-birla-renewables-secures-1-111118938.html"],["Aditya Birla banks ~₹14,000 cr Sprng acquisition loan — 9fin, 31-Jul-26","https://www.9fin.com/insights/aditya-birla-banks-inr140bn-sprng-acquisition-loan"]])

# 8 L&T
append_rationale("Larsen & Toubro",U+"order momentum keeps the working-capital and capex engines fed: an 'ultra-mega' (>₹15,000 cr; ~US$1.6–1.7bn) offshore EPCIC award in the Middle East (17-Aug-26), a 'large' (₹2,500–5,000 cr) Automated People Mover package at Al Maktoum International, Dubai with Mitsubishi Heavy (20-Aug-26) and a 'major' (₹1,000–2,500 cr) Middle-East BESS order for L&T Renewables (25-Aug-26); Bloomberg tallies ~US$11.6bn of wins as Gulf demand rebounds (26-Aug-26 ⚑). Portfolio reshaping: a 'mega' (₹10,000–15,000 cr) NVIDIA B300 AI-factory build order from Together AI and the ₹1,400 cr sale of the data-centre business to Vyoma.AI (both ~11–13-Aug-26 ⚑) — note the DC divestment trims the ₹10,000 cr data-centre capex line in this card's thesis. No NCD print or rating action in the window.")
add_kv("Larsen & Toubro",{"Aug-26 orders":"Middle-East offshore ultra-mega (>₹15,000 cr) · Dubai APM large (₹2,500–5,000 cr) · ME BESS major (₹1,000–2,500 cr)","DC vertical":"data-centre business sold to Vyoma.AI for ₹1,400 cr (Aug-26 ⚑) — capex line to be re-based"})
add_list("Larsen & Toubro","finhist",["FY26: revenue ₹2,85,874 cr; record order inflows ₹4,35,590 cr","2026: debut ESG bond ₹500 cr 3-yr @6.35%; ₹9,800 cr NCDs outstanding (2028–2035)","Aug 2026: >₹15,000 cr Middle-East offshore EPCIC + Dubai APM + BESS wins; DC business sold to Vyoma.AI (₹1,400 cr ⚑)"])
add_srcs("Larsen & Toubro",[["L&T ultra-mega offshore order, Middle East — L&T release, 17-Aug-26","https://www.larsentoubro.com/pressreleases/2026/2026-08-17-lt-secures-ultra-mega-order-for-strategic-offshore-development-project-in-the-middle-east"],["L&T Renewables major BESS order — L&T release, 25-Aug-26","https://www.larsentoubro.com/pressreleases/2026/2026-08-25-lt-renewables-business-wins-major-order-for-battery-energy-storage-system-in-the-middle-east"],["L&T wins Dubai airport APM order — Whalesbook, 20-Aug-26","https://www.whalesbook.com/corporate-news/English/industrial-goodsservices/Larsen-and-Toubro-Wins-Large-Dubai-Airport-APM-Order/6a8683c46817e697e7d2e49c"]])

# 9 Tata Steel — correct NCD dating
append_rationale("Tata Steel",U+"a contingent-liability item surfaced — the District Mining Office, Ramgarh (Jharkhand) raised a ₹1,755.1 cr demand alleging excess coal extraction at West Bokaro (FY2000-01 to FY2006-07); the Revisional Authority admitted Tata Steel's revision application on 24-Aug-26 and directed no coercive steps (disclosed 25-Aug-26) — immaterial to the ₹84,173 cr net-debt stack but a rationale talking point. Q1FY27 also carried board approval of the ₹33,873 cr NINL 4.8 MTPA expansion — the next multi-year capex draw behind the FY27 ~₹20,000 cr guidance.")
add_kv("Tata Steel",{"Q1FY27 balance sheet":"net debt ₹84,173 cr · liquidity ₹45,950 cr · Q1 capex ₹3,579 cr","NINL expansion":"₹33,873 cr for 4.8 MTPA — board-approved 30-Jul-26","West Bokaro demand":"₹1,755.1 cr mining demand — revision admitted 24-Aug-26, no coercive steps"})
add_list("Tata Steel","finhist",["Q1FY27 (30-Jul-26): revenue ₹60,794 cr, EBITDA ₹9,370 cr (15.4%), PAT ₹2,318 cr; net debt ₹84,173 cr","Feb 2025: ₹3,000 cr 5-yr NCD @7.65% (mat. 21-Feb-2030) — the last domestic print"])
add_srcs("Tata Steel",[["Tata Steel ₹3,000 cr NCD @7.65% allotted 21-Feb-2025 — Investing.com","https://in.investing.com/news/company-news/tata-steel-secures-3000-crore-via-ncds-at-765-coupon-rate-93CH-4678906"],["West Bokaro ₹1,755 cr demand — Reg 30 disclosure via TradingView/Reuters, 25-Aug-26","https://www.tradingview.com/news/reuters.com,2026-08-25:newsml_RSY0350Sa:0-reg-tata-steel-limited-disclosure-under-reg30-51-sebi-listing-regulations/"],["Q1FY27 + ₹33,873 cr NINL expansion — Business Standard, 30-Jul-26","https://www.business-standard.com/companies/quarterly-results/tata-steel-q1-net-profit-up-11-6-approves-33-873-cr-neelachal-expansion-126073001546_1.html"]])

# 10 Hindalco
append_rationale("Hindalco",U+"Q1FY27 (7-Aug-26) re-based the numbers: consolidated revenue ₹84,825 cr (+32%), EBITDA ₹14,989 cr (+73%), PAT ₹7,013 cr; consolidated net debt ₹77,495 cr at 1.95x ND/EBITDA. Novelis net debt US$7.9bn at 4.5x (target <4x by FY27-end, FCF-positive by Q4FY27) after drawing a US$500mn unsecured term loan (27-Jul-26, matures Jul-28) — the two-year bullet is the near-term refi hook on the Novelis side. Bay Minette is commissioning with the ramp in H2 CY26. India's first superfine PPT ATH plant was commissioned 27-Aug-26 (size undisclosed). No India NCD print or rating action in the window.")
add_kv("Hindalco",{"Q1FY27":"revenue ₹84,825 cr · EBITDA ₹14,989 cr · PAT ₹7,013 cr","Net debt (Jun-26)":"₹77,495 cr consolidated — 1.95x; Novelis US$7.9bn @4.5x (target <4x FY27-end)"})
add_list("Hindalco","maturities",["Novelis US$500mn unsecured TL drawn 27-Jul-26 — matures Jul-2028 (2-yr bullet; refi hook)"])
add_list("Hindalco","finhist",["Q1FY27: EBITDA ₹14,989 cr (+73%); consolidated ND ₹77,495 cr (1.95x); Novelis ND US$7.9bn (4.5x)"])
add_srcs("Hindalco",[["Hindalco Q1FY27 results — company release","https://www.hindalco.com/media/press-releases/hindalco-results-q1fy27"],["Novelis deleveraging path post-Q1 — Sahi","https://www.sahi.com/news/hindalco-projects-substantial-novelis-net-debt-reduction-and-ebitda-per-ton-improvement-post-q1-137-PE1_CORP"]])

# 11 Zydus
append_rationale("Zydus Lifesciences",U+"treasury plumbing for the offshore M&A book: Zydus incorporated a wholly-owned 'Zydus Global Treasury Centre IFSC Ltd' in GIFT City (₹5 cr paid-up; disclosed 22–26-Aug-26) to run global/regional treasury — a natural conduit for ECB/USD-bridge take-outs alongside INR NCDs. FY26 strategy update (23-Aug-26) reiterates revenue US$3.02bn, EBITDA US$959mn and Amplitude Surgical (~€100mn revenue) as the medtech platform. Minor: a High Court order barred a Dabrafenib launch (Novartis patent, 18-Aug-26). No NCD print or rating action in the window.")
add_kv("Zydus Lifesciences",{"GIFT City treasury (Aug-26)":"Zydus Global Treasury Centre IFSC Ltd incorporated — offshore funding conduit"})
add_list("Zydus Lifesciences","finhist",["Aug 2026: GIFT City treasury WOS incorporated; FY26 revenue US$3.02bn / EBITDA US$959mn reiterated"])
add_srcs("Zydus Lifesciences",[["Zydus incorporates GIFT City treasury WOS — EquityBulls, Aug-26","https://www.equitybulls.com/category.php?id=374513"]])

# 12 UPL
append_rationale("UPL",U+"the Dec-26 US$500mn is now inside four months without a take-out print — the dollar-bond-or-bridge decision is the single event to watch ⚑; no NCLT order on the composite scheme reported yet. Management change carried from Q1: CEO Mike Frank resigned with the 4-Aug-26 results (stock −6.9% on the day) — FY27 guidance (revenue +7–11%, EBITDA +10–14%) reiterated; Advanta revenue +26%.")
add_list("UPL","finhist",["Aug 2026: CEO Mike Frank resigns alongside Q1FY27 (4-Aug); FY27 guidance held"])
add_srcs("UPL",[["UPL Q1FY27 — CEO exit, guidance held — Business Standard, 4-Aug-26","https://www.business-standard.com/amp/markets/news/upl-stays-on-growth-path-despite-geopolitical-risks-demand-uncertainty-126080401612_1.html"]])

# 13 Suzlon
append_rationale("Suzlon",U+"the re-leveraging paths are getting concrete. (1) Devco/state build: ground-breaking with the Andhra Pradesh government for 1,325 MW of wind projects at Ananthapuramu — ₹10,500 cr of investment, ~1,600 jobs — plus an expanded blade plant (two added lines, 1,260 MW p.a.) with ~1,600 MW under execution in AP (25-Aug-26); funding/offtake structure undisclosed ⚑ — if Suzlon carries any of this on balance sheet it is the project-debt trigger this card waits for. (2) Orders: 250 MW from Torrent Green (76 × S144 3.3 MW; sixth Torrent order, relationship 1,306 MW cumulative; 25-Aug-26) takes FY27 intake past 1.1 GW. No rating action in the window.")
add_kv("Suzlon",{"AP programme (Aug-26)":"1,325 MW wind at Ananthapuramu — ₹10,500 cr investment; blade plant +1,260 MW p.a.; ~1,600 MW under execution in AP","Torrent Green order":"250 MW S144 (25-Aug-26) — cumulative 1,306 MW with Torrent"})
add_list("Suzlon","maturities",["AP 1,325 MW / ₹10,500 cr programme — funding structure ⚑ (Devco vs. customer-owned); first potential balance-sheet project debt"])
add_list("Suzlon","finhist",["Aug 2026: AP ₹10,500 cr wind-programme ground-breaking; 250 MW Torrent Green order; FY27 intake >1.1 GW"])
add_srcs("Suzlon",[["Suzlon ₹10,500 cr Andhra wind projects on track — Business Standard, 25-Aug-26","https://www.business-standard.com/companies/news/suzlon-s-10-500-crore-andhra-pradesh-wind-energy-projects-on-track-126082500960_1.html"],["Suzlon 250 MW Torrent Green order — Windinsider, 26-Aug-26","https://windinsider.com/2026/08/26/suzlon-secures-250-mw-order-from-torrent-green-pushing-partnership-past-1-3-gw/"]])

# 14 Torrent Green
append_rationale("Torrent Green Energy",U+"build-out continues to convert into orders: TGEPL placed a 250 MW S144 order on Suzlon (sixth order; Torrent–Suzlon relationship now 1,306 MW; 25-Aug-26) and incorporated Torrent Urja 50 and 51 as fresh RE SPVs (19-Aug-26) — each SPV a future project-loan borrower. Parent treasury changes hands: CFO Saurabh Mashruwala retired 20-Aug-26 and Vikas Poddar took over 21-Aug-26 — re-establish the relationship. Q1FY27 (3-Aug-26): parent PAT ₹662 cr (−11%), FY27 capex guided ~₹10,000 cr, Nabha Power acquisition completed. No NCD/QIP print or rating action in the window.")
add_kv("Torrent Green Energy",{"Aug-26":"250 MW Suzlon order (cum. 1,306 MW); SPVs Torrent Urja 50/51 incorporated; new parent CFO Vikas Poddar (21-Aug)","FY27 capex guide":"~₹10,000 cr (parent, Q1FY27 call)"})
add_list("Torrent Green Energy","finhist",["Aug 2026: 250 MW Suzlon order; Torrent Urja 50/51 SPVs formed; parent Q1FY27 PAT ₹662 cr, FY27 capex ~₹10,000 cr"])
add_srcs("Torrent Green Energy",[["Suzlon 250 MW order from Torrent Green — Windinsider, 26-Aug-26","https://windinsider.com/2026/08/26/suzlon-secures-250-mw-order-from-torrent-green-pushing-partnership-past-1-3-gw/"],["Torrent Power announcements (CFO change, SPV incorporations) — Screener","https://www.screener.in/company/TORNTPOWER/consolidated/"]])

# 15 IRB
append_rationale("IRB Infrastructure",U+"the take-out machine is being re-geared. The IRB board approved investing up to ₹351 cr in IRB InvIT Fund via a preferential issue (up to 54mn units at ₹65) to part-fund the public InvIT's acquisition of two project SPVs from the private IRB Infrastructure Trust — unitholder EGM 21-Sep-26; the InvIT's 23-Aug notice also flagged fund-raising via units, NCDs and other debt instruments for the same board meeting (26-Aug-26) — the acquisition-debt leg at the public trust is the mandate to chase ⚑ quantum. Separately, a scheme merges nine wholly-owned subsidiaries (ATR Infrastructure, Aryan Toll Road, Aryan Hospitality, IRB Goa Tollway, IRB Infra Industries, GE1 Expressway, IRB PS Highway, Mhaiskar Infrastructure, Thane Ghodbunder Toll Road) into IRB IDL — no consideration; NCLT Mumbai approval pending — simplifying the parent-level security/borrower map. No rating action in the window.")
add_kv("IRB Infrastructure",{"IRB InvIT drop-down (Aug-26)":"2 SPVs from the private trust → public InvIT; IRB to subscribe ≤₹351 cr pref units @₹65; EGM 21-Sep-26; InvIT NCD/debt raise flagged ⚑","Amalgamation":"9 WOS into IRB IDL — NCLT pending"})
add_list("IRB Infrastructure","maturities",["IRB InvIT Fund acquisition funding — units (₹351 cr from IRB) + NCDs/debt ⚑ size, post-21-Sep-26 EGM"])
add_list("IRB Infrastructure","finhist",["Aug 2026: ₹351 cr pref investment in IRB InvIT approved; 9-subsidiary amalgamation scheme filed"])
add_srcs("IRB Infrastructure",[["IRB — investment in IRB InvIT Fund units (26-Aug-26)","https://www.irb.co.in/home/2026/08/26/intimation-about-approval-for-investment-in-the-units-of-irb-invit-fund-august-26-2026/"],["IRB — scheme of amalgamation of WOS (26-Aug-26)","https://www.irb.co.in/home/2026/08/26/scheme-of-amalgamation-of-certain-wholly-owned-subsidiaries-with-irb-infrastructure-developers-limited-august-26-2026/"]])

# 16 Hinduja Renewables
append_rationale("Hinduja Renewables",U+"a state-level commitment adds to the pipeline: the Hinduja group committed ₹2,500 cr in Tamil Nadu (13-Aug-26) — Hinduja Renewables Energy to build 200+ MW of solar/wind/BESS across Tirunelveli, Thoothukudi, Virudhunagar, Madurai and Coimbatore, the balance via OHM Global Mobility (e-buses); no financing terms disclosed ⚑. No NCD or rating action in the window.")
add_kv("Hinduja Renewables",{"Tamil Nadu commitment (Aug-26)":"group ₹2,500 cr — HREPL 200+ MW solar/wind/BESS across five districts (share ⚑)"})
add_list("Hinduja Renewables","finhist",["Aug-26: Hinduja group ₹2,500 cr Tamil Nadu commitment — HREPL 200+ MW hybrid/BESS leg"])
add_srcs("Hinduja Renewables",[["Hinduja group ₹2,500 cr Tamil Nadu investment — Business Standard, 13-Aug-26","https://www.business-standard.com/companies/news/hinduja-group-to-invest-2-500-cr-in-tn-across-renewables-ev-ecosystem-126081301808_1.html"]])

# 17 IndiGo
append_rationale("InterGlobe",U+"the ownership pivot got its authorisation: the 20-Aug-26 AGM approved a sharp increase in the borrowing limit (99.97% in favour; notice sought ~₹1.1 lakh cr ⚑ figure from summary) for fleet expansion — the enabling resolution behind the owned/finance-leased build. A 'CRISIL rating update' filing dated 18-Aug-26 sits on the exchange feed ⚑ content unread — pull it to see whether a second agency now prints alongside ICRA. ICRA's Watch Negative remained unresolved at end-Aug-26. Context carried: Willie Walsh took over as CEO 3-Aug-26; Q1FY27 net loss ₹238 cr (revenue +19.9%, fuel +86%).")
add_kv("InterGlobe",{"Borrowing limit (AGM 20-Aug-26)":"raised for fleet expansion — ~₹1.1 lakh cr sought ⚑; 99.97% approval","CEO":"Willie Walsh (from 3-Aug-26)"})
add_list("InterGlobe","finhist",["Q1FY27: net loss ₹238 cr; revenue +19.9%; fuel cost +86%","Aug-26: AGM approves higher borrowing limit for fleet ownership; CEO transition to Willie Walsh"])
add_list("InterGlobe","ratingevo",["18-Aug-26: 'CRISIL rating update' exchange filing ⚑ unread — possible second-agency print","Watch Negative (ICRA) still unresolved at end-Aug-26"])
add_srcs("InterGlobe",[["IndiGo AGM approves borrowing-limit hike — ScanX, 20-Aug-26","https://scanx.trade/stock-market-news/companies/interglobe-aviation-agm-approves-borrowing-limit-hike/48781758"]])

# 18 Maple

append_rationale("Maple Infrastructure",U+"a fresh NCD programme is live: the InvIT committee approved up to ₹700 cr of senior secured rated listed NCDs (70,000 × ₹1 lakh; private placement, one or more tranches; Axis Trustee) on 17-Aug-26; ICRA reaffirmed [ICRA]AAA (Stable) (credit-rating update filed 19-Aug; DB row 21-Aug-26), and an 'Allotment of NCDs – Aug 2026' disclosure followed on 28-Aug-26 — tranche size/coupon/tenor ⚑ not yet read. The Feb-26 rationale's downgrade triggers (DSCR <1.70x; debt-funded acquisitions) frame how much of the ₹700 cr lands as acquisition versus refi paper.")
add_kv("Maple Infrastructure",{"Aug-26 NCD programme":"up to ₹700 cr senior secured NCDs approved 17-Aug; allotment disclosed 28-Aug ⚑ terms"})
add_list("Maple Infrastructure","maturities",["₹700 cr NCD programme (Aug-26) — first tranche allotted 28-Aug-26 ⚑ size/tenor"])
add_list("Maple Infrastructure","finhist",["FY26: toll revenue ₹1,290.84 cr; ₹1,799 cr Ashoka five-asset acquisition completed","Aug 2026: ₹700 cr NCD programme approved; ICRA AAA/Stable reaffirmed; first allotment 28-Aug"])
add_list("Maple Infrastructure","ratingevo",["Aug-26: ICRA AAA/Stable reaffirmed (filing 19-Aug; DB row 21-Aug-26)"])
add_srcs("Maple Infrastructure",[["Maple InvIT committee to consider ₹700 cr NCDs (17-Aug-26) — ScanX","https://scanx.trade/stock-market-news/companies/maple-infra-trust-consider-ncd-issuance-aug-17-meeting/48105025"],["Maple Highways investor relations — Aug-26 rating update + NCD allotment filings","https://www.maplehighways.com/investor-relations"]])

# 19 Mindspace

append_rationale("Mindspace Business Parks",U+"two rolling-window prints in a month: ₹600 cr 2-yr NCDs at 7.4913% (allotted 3-Aug-26, mat. 3-Aug-28) and ₹500 cr (50,000 NCDs) at 7.6335% maturing 26-Sep-28 (allotted 24-Aug-26) — short-dated AAA rent-backed paper at ~7.5–7.6%, the repricing benchmark for the ₹5,590 cr NCD book. ICRA re-printed AAA/Stable on 21-Aug-26 (new rating-assigned rationale ⚑ instrument size unread). Sponsor housekeeping: K Raheja Corp released a pledge on ~1.21 cr units (~₹600 cr) on 24/25-Aug-26. Q1FY27 (5-Aug-26): NOI ₹788 cr (+27.8%), record DPU, ₹442 cr distributed.")
add_kv("Mindspace Business Parks",{"Aug-26 NCDs":"₹600 cr @7.4913% 2-yr (3-Aug-26) · ₹500 cr @7.6335% due 26-Sep-28 (24-Aug-26)","Q1FY27":"NOI ₹788 cr (+27.8%); distribution ₹442 cr"})
add_list("Mindspace Business Parks","maturities",["₹600 cr 7.4913% NCD due 3-Aug-2028","₹500 cr 7.6335% NCD due 26-Sep-2028"])
add_list("Mindspace Business Parks","finhist",["Aug 2026: ₹600 cr + ₹500 cr short-dated NCDs at 7.49–7.63%; Q1FY27 NOI ₹788 cr (+27.8%)"])
add_list("Mindspace Business Parks","ratingevo",["21-Aug-26: ICRA AAA/Stable re-printed (rating assigned on fresh instruments ⚑ size)"])
add_srcs("Mindspace Business Parks",[["Mindspace REIT announcements — ₹500 cr NCD @7.6335% (24-Aug-26), rating disclosure (21-Aug-26) — Screener","https://www.screener.in/company/MINDSPACE/consolidated/"]])

# 20 BKT
append_rationale("Balkrishna",U+"the programme is drawing in NCD form: the finance committee approved ₹550 cr of senior unsecured rated listed NCDs (55,000 × ₹1 lakh; multiple tranches; BSE) on 18-Aug-26, following the ₹750 cr NCD allotted 23-Mar-26 — ratings for the issue are in hand per market reports (27-Aug-26); coupon/tenor at allotment ⚑. That is ₹1,300 cr of fresh bond funding inside six months against the ₹3,500 cr capex plan — the repeat-issuer cadence this card anticipated.")
add_kv("Balkrishna",{"NCDs 2026":"₹750 cr allotted 23-Mar-26 · ₹550 cr approved 18-Aug-26 (tranches; terms at allotment ⚑)"})
add_list("Balkrishna","maturities",["₹550 cr NCD (approved 18-Aug-26) — allotment pending; ₹750 cr Mar-26 NCD ⚑ tenor"])
add_list("Balkrishna","finhist",["2026: ₹750 cr NCD (Mar) + ₹550 cr NCD approved (Aug) — capex funding via bonds"])
add_srcs("Balkrishna",[["BKT approves ₹550 cr NCD private placement — ScanX, 18-Aug-26","https://scanx.trade/stock-market-news/companies/balkrishna-industries-approves-550-crore-ncd-private-placement/48599250"]])

# 21 Apollo Tyres
append_rationale("Apollo Tyres",U+"first bond leg of the ₹5,810 cr programme: Apollo Tyres has lined up a ₹500 cr NCD with ratings secured (reported 27-Aug-26 alongside the Vedanta/BKT pipeline; India Ratings IND AA+/Stable on the NCDs); size/coupon/tenor at launch ⚑. Small relative to the programme, but it establishes the domestic curve for the larger FY27–29 draws.")
add_kv("Apollo Tyres",{"NCD (Aug-26)":"₹500 cr planned — IND AA+/Stable on NCDs; terms ⚑"})
add_list("Apollo Tyres","maturities",["₹500 cr NCD launch (late Aug/Sep-26) ⚑ terms — first bond leg of the AP expansion funding"])
add_list("Apollo Tyres","ratingevo",["Aug-26: India Ratings IND AA+/Stable on the proposed NCDs"])
add_srcs("Apollo Tyres",[["Vedanta / Apollo Tyres / BKT bond pipeline — Business Standard, 27-Aug-26","https://www.business-standard.com/companies/news/vedanta-plans-third-rupee-bond-issue-seeks-to-raise-1-000-crore-126082700777_1.html"],["India Ratings press release — Apollo Tyres","https://www.indiaratings.co.in/pressrelease/81643"]])

# 22 Prestige
append_rationale("Prestige Estates",U+"ICRA re-printed on 19-Aug-26 (ratings DB rows): Prestige Estates [ICRA]A1 on CP and Prestige Hospitality Ventures [ICRA]A+(CE)/Stable — no upgrade to AA−; rationales ⚑ unread. The ₹2,000 cr NCD remains unplaced ⚑ and no Prestige Hospitality IPO relaunch surfaced in the window — the demote-to-watch flag from 5-Aug stands; kept for the fortnightly full pass.")
add_list("Prestige Estates","ratingevo",["19-Aug-26: ICRA A1 (CP) re-printed on Prestige Estates; Prestige Hospitality Ventures A+(CE)/Stable — DB rows ⚑ rationale"])

# 23 Godrej Properties
append_rationale("Godrej Properties",U+"a ₹16,000 cr Haryana investment commitment by FY28 (Godrej Properties) plus ₹3,500 cr of Grade-A+ Gurugram offices via Godrej Ventures — part of a ₹20,000 cr group pledge announced 24-Aug-26 ⚑ (roundup-sourced; primary filing unread) — extends the land/JDA spend that the CP+NCD stack funds; no fresh NCD print in the window.")
add_kv("Godrej Properties",{"Haryana commitment (Aug-26)":"₹16,000 cr by FY28 (GPL) + ₹3,500 cr offices (Godrej Ventures) ⚑"})
add_srcs("Godrej Properties",[["Godrej group ₹20,000 cr Haryana commitment — funding roundup, 24-Aug-26 (⚑)","https://startuptalky.com/news/daily-indian-funding-roundup-key-news-24-august-2026/"]])

# 24 Rain
append_rationale("Rain Group",U+"Q2 CY26 call (12–13-Aug-26): management confirmed the second-lien notes trade below coupon and a refinancing is being worked with banking advisers, but an early refi carries meaningful costs and needs an attractive window; target net debt/EBITDA ~3x, no equity raise; India CTP distillation phase-1 slated for early 2028. Working capital jumped ~3x in the quarter. That keeps the onshore INR take-out thesis live without a dated trigger ⚑. No rating action in the window.")
add_kv("Rain Group",{"Refi stance (Aug-26)":"second-lien notes refi under discussion — timing window-dependent; target ND/EBITDA ~3x"})
add_list("Rain Group","finhist",["Q2 CY26 (Aug-26): working capital ~3x QoQ; refi of second-lien notes being evaluated; India CTP distillation phase-1 early 2028"])
add_srcs("Rain Group",[["Rain Industries Q2 CY26 — leverage target, refi commentary — ScanX","https://scanx.trade/stock-market-news/companies/rain-industries-q2-results-working-capital-surges-3x-leverage-target/48107342"]])

# 25 Sun Pharma
append_rationale("Sun Pharma",U+"quiet window — no new financing, regulatory-approval or take-out print; close still guided early 2027. Carried from early August: CRISIL reaffirmed AAA/Stable and removed the rating watch in place since Apr-26 (4–5-Aug-26), so both domestic AAA prints now stand clean post-deal; Q1FY27 (31-Jul-26) revenue ₹15,300 cr (+10.5%), EBITDA ₹4,419 cr, PAT ₹2,895 cr (+27%), US sales US$427mn (−9.7%).")
add_list("Sun Pharma","finhist",["Q1FY27 (31-Jul-26): revenue ₹15,300 cr (+10.5%), EBITDA ₹4,419 cr, PAT ₹2,895 cr (+27%); US US$427mn (−9.7%)"])
add_list("Sun Pharma","ratingevo",["4–5-Aug-26: CRISIL AAA/Stable reaffirmed; rating watch (since Apr-26) removed"])
add_srcs("Sun Pharma",[["CRISIL AAA/Stable reaffirmed, watch removed — Whalesbook, Aug-26","https://www.whalesbook.com/corporate-news/English/bankingfinance/Sun-Pharmas-CRISIL-AAAStable-Rating-Reaffirmed-Watch-Removed/6a72b32a10bce1fd203db967"],["Sun Pharma Q1FY27 — Kotak Neo","https://www.kotakneo.com/news/stocks/sun-pharma-q1-fy27-result-net-profit-rises-27-percent/"]])

# ---------- finhist back-fill for cards missing the field ----------
add_list("Citius","finhist",["FY25: revenue ₹1,987 cr across 10 BOT/annuity roads (EV ₹10,494 cr)","Apr-26: ₹1,105 cr IPO (20x subscribed); unitholders approve 49% debt ceiling","Post-IPO debt ~₹4,000–4,500 cr ⚑"])
add_list("Coforge","finhist",["Apr-26: Encora (US$2.35bn EV) and Cigniti closes","Funding: ~US$1.89bn preferential equity + QIP up to US$550mn; Cigniti debt ~₹2,000–2,300 cr ⚑ refi candidate"])
add_list("Apollo Hospitals","finhist",["FY26: EBITDA ₹3,769 cr (+25%), PAT ₹1,942 cr (+34%)","Gross debt ~₹4,000 cr; net debt/EBITDA ~1.3x","Capex FY26 ~₹2,000 cr; ~₹5,100 cr / ~4,400-bed pipeline over 5 yrs"])
add_list("Exide","finhist",["Cumulative ₹3,947 cr equity into Exide Energy Solutions (₹645 cr in FY26)","Bengaluru gigafactory phase-1 ~₹5,000–6,000 cr; 6→12 GWh","Production targeted FY26-end → FY27 ramp"])

# light clean of process phrasing in ratingevo
html=html.replace('"Promoted via news-mode screen 5-Aug-26"','"Carded 5-Aug-26 on the verified rating print"')

# ---------------- WATCH UPDATES ----------------
replace_watch("Escorts Kubota","Re-test 31-Aug-26: the greenfield programme firmed up — ground-breaking 19-Aug-26 at Sector-10 YEIDA (UP): ₹2,025 cr phased investment on 154 acres, capacity up to 60,000 tractors + 15,000 construction-equipment units. Clears the ₹2,000 cr capex bar but the balance sheet is net-cash and no debt funding is indicated ⚑ — kept on watch; promote if term-debt/NCD funding is disclosed.")
replace_watch("DCM Shriram","Re-test 31-Aug-26: CRISIL published a fresh rationale dated 20-Aug-26 (action ⚑ unread — page inaccessible); no capex/NCD event surfaced. Kept.")
replace_watch("Bharat Forge","Re-test 31-Aug-26: a ₹25,000 cr enabling fund-raise resolution is out for postal ballot (notice 13-Aug-26) and Q1 commentary guided ~₹2,500 cr capex incl. an Andhra Pradesh facility — ICRA's 10-Mar-26 reaffirmation (AA+/Stable; ₹700 cr NCD withdrawn on repayment) framed capex at ₹1,400–1,600 cr p.a. Kept; re-test after the ballot outcome (mid-Sep-26).")

# new watch entry: Gabriel India
gab='{n:"Gabriel India (Anand group — Project Jupiter)", w:"Debut bond issuer with a live debt-funded acquisition: the board approved ₹1,000 cr of senior unsecured rated listed NCDs (24-Aug-26; coupon/tenor via a finance committee) to part-fund ‘Project Jupiter’ — a ₹3,166 cr outlay buying 28.99% of HL Mando Anand (₹350 cr cash + ₹1,881 cr preferential shares at ₹1,305.89) and 30%-minus-one-share of HL Klemove India from HL Klemove Corp for US$98.44mn (~₹935 cr; agreement 24–26-Aug-26). Rating band ⚑ unverified (historically around the AA−/A+ line) and FY26 EBITDA likely under the ₹500 cr gate ⚑ — hence watch, not card. Promote once the NCD prints with an A+-or-better rating and the consolidated EBITDA leg is verified. Sources: https://www.sahi.com/news/gabriel-india-board-approves-1-000-crore-ncd-issue-and-constitutes-finance-committee-101-PE1_CORP · https://scanx.trade/stock-market-news/companies/gabriel-india-executes-deal-acquire-30-stake-hl-klemove-india/48851568"},\n'
wi=html.find('const WATCH = [\n')+len('const WATCH = [\n')
html=html[:wi]+gab+html[wi:]

# ---------------- HERO + LOG ----------------
old_hero='compiled 1 Sep 2026 (v6.5 · news-refresh mode — no promotions/demotions · JSPL ICRA AA+ upgrade (JSOL too) · AESL Aug-26 refi confirmed: US$500mn 15-yr Apollo private placement · Tata Steel ₹3,000 cr NCD dating corrected (Feb-25 vintage) · HCCB 2027 listing official)'
assert html.count(old_hero)==1
html=html.replace(old_hero,'compiled 3 Sep 2026 (v6.6 · news refresh, scan window 15–31 Aug — no promotions/demotions · ABReL US$1.6bn Sprng acquisition loan · Blackstone US$1.25bn KRT OFS · Jio Platforms IPO cleared by SEBI · IndiGrid ₹1,100 cr @7.44% + Mindspace ₹500 cr @7.63% NCDs · Vedanta VAML ₹13,500 cr TL refi + third 2026 NCD lined up · AESL ₹4,700 cr Satara win)')

log='''<details style="margin-top:10px" open><summary style="cursor:pointer;font-weight:700;color:var(--indigo-dark)">News refresh — 3 Sep 2026 (news-refresh mode · v6.6 · scan window 15–31 Aug)</summary><div style="font-size:13px;color:var(--muted);margin-top:8px;line-height:1.7"><p><b>Card updates (25):</b> Vedanta (VAML ₹13,500 cr 6.5–7-yr TL refi @~7.9–8.0%; Ind-Ra VAML NCDs → IND AA+; third 2026 parent NCD ≥₹1,000 cr lined up 27-Aug; VRL stake-sale denial) · Reliance (Jio Platforms IPO SEBI observations 28-Aug — ~₹37,700 cr, up to ₹27,500 cr debt repayment; Fitch LC IDR → A− ⚑) · Knowledge Realty Trust (Blackstone OFS up to 25.03% / ~₹11,988 cr, floor ₹108, 31-Aug/1-Sep; sponsor-mix watch) · AESL (₹4,700 cr Satara TBCB win) · TPREL (ICRA AA+ reaffirmed 20-Aug; 190.5 MW FDRE + 72.5 MW captive commissioned; 7.0 GW operational) · IndiGrid (₹1,100 cr NCDs @7.44% 5/10-yr allotted 20-Aug; AAA on ₹20,600 cr stack) · ABReL (US$1.6bn MUFG-underwritten Sprng/Solenergi acquisition TL; EV ~₹17,322 cr; Watch Developing unresolved) · L&amp;T (>₹15,000 cr ME offshore + Dubai APM + BESS orders; DC business sold ₹1,400 cr ⚑) · Tata Steel (₹1,755 cr West Bokaro mining demand admitted for revision; NINL ₹33,873 cr expansion logged) · Hindalco (Q1FY27 ND ₹77,495 cr 1.95x; Novelis US$500mn TL Jul-28) · Zydus (GIFT City treasury WOS) · UPL (Dec-26 US$500mn still unrefinanced ⚑; CEO exit carried) · Suzlon (AP 1,325 MW / ₹10,500 cr programme; 250 MW Torrent order) · Torrent Green (250 MW Suzlon order; new SPVs; parent CFO change) · IRB (₹351 cr pref into IRB InvIT for 2-SPV drop-down, EGM 21-Sep; InvIT NCD raise flagged; 9-WOS amalgamation) · Hinduja Renewables (₹2,500 cr TN group commitment) · IndiGo (AGM borrowing-limit hike ~₹1.1 lakh cr ⚑; CRISIL filing 18-Aug ⚑ unread; ICRA watch unresolved) · Maple (₹700 cr NCD programme; ICRA AAA reaffirmed; allotment 28-Aug ⚑ terms) · Mindspace (₹500 cr @7.6335% due Sep-28 + ₹600 cr @7.4913% Aug-28; ICRA AAA 21-Aug) · BKT (₹550 cr NCD approved after ₹750 cr Mar-26) · Apollo Tyres (₹500 cr NCD, IND AA+) · Prestige (ICRA A1 CP + PHV A+(CE) re-printed 19-Aug; NCD still unplaced) · Godrej Properties (₹16,000 cr Haryana commitment ⚑) · Rain (second-lien refi under evaluation) · Sun Pharma (CRISIL watch removal + Q1FY27 carried).</p><p><b>Rating actions found (window 15–31 Aug):</b> ICRA reaffirmations — TPREL AA+ (20-Aug), Maple AAA (21-Aug), Mindspace AAA (21-Aug), Prestige A1/PHV A+(CE) (19-Aug), Uno Minda AA+ (17-Aug; not carded); CRISIL/ICRA AAA on IndiGrid's new tranche (17-Aug); Ind-Ra VAML → IND AA+ (14-Aug); Fitch RIL LC → A− (~29-Aug ⚑); CRISIL DCM Shriram rationale 20-Aug (⚑ unread). Outside the universe: CARE upgrades Adani Power AA → AA+ (18-Aug; thermal — excluded), CRISIL upgrades Premier Energies A → A+/Positive (18-Aug; net debt/EBITDA ~0.4x, no trigger), ICRA upgrades iValue A → A+ (24-Aug; sub-scale). No downgrades or watch placements found on carded names.</p><p><b>Watchlist:</b> Escorts Kubota (₹2,025 cr UP greenfield ground-breaking 19-Aug — clears the capex bar but net-cash, no debt indicated; kept) · DCM Shriram (CRISIL 20-Aug rationale ⚑ unread; kept) · Bharat Forge (₹25,000 cr enabling resolution on postal ballot; kept) · ABFRL, KLJ, STL, SP group, Aarti, AB Real Estate, Cohance — no trigger; kept · <b>New watch entry:</b> Gabriel India (₹1,000 cr debut NCD for the ₹3,166 cr HL Mando/HL Klemove ‘Project Jupiter’ — rating band and EBITDA leg ⚑). Not re-tested this pass (search budget): Birla Carbon, Epigral, Godrej &amp; Boyce, JSW Hydro, KPIL, Meril, Redington, Sona BLW, Syngene, Supreme, Superform, Air India, Anzen, Energy Infra Trust, Greaves, Grasim.</p><p><b>No material change (scanned):</b> Sun Pharma · Greenko · Dorf-Ketal (Italmatch unresolved) · Jubilant Bev/Bevco · Tata Electronics · Adani Airports · Intas · Biocon Biologics. <b>Carried unchanged, not scanned this pass:</b> Genus · Adani Green · Altius · JSW Neo · JSW Paints · Avaada · Torrent Pharma · M&amp;M · CtrlS · Sembcorp GI · JSW Infra/Steel · Manipal — all on the fortnightly full-pass list. Overlap note: the 1-Sep v6.5 pass separately covered TMCV Q1, JSPL/JSOL upgrades, AESL refi confirmation, Vertis 22-Aug CRISIL action, HCCB listing and the Tata Steel NCD dating — not repeated here. Market sweep also logged: DMart weighing a record rupee bond (⚑ Bloomberg, net-cash — no fit), TCS–MHP ~US$373mn (AAA, cash), Netweb ₹1,200 cr QIP.</p><p><b>Ratings DB cross-check</b> (ratings_current.csv refreshed 22-Aug-26; 13 high-grade rows since 15-Aug): TPREL, Maple, Mindspace, Prestige/PHV rows applied above; Uno Minda AA+, CESC AA, Rostrum Realty AAA, H.G. Infra AA−, TCS AAA — no trigger/outside scope; BFSI rows excluded (Vistaar, Niva Bupa, Repco).</p></div></details>'''
old_open='<details style="margin-top:10px" open><summary style="cursor:pointer;font-weight:700;color:var(--indigo-dark)">News refresh — 1 Sep 2026'
assert html.count(old_open)==1
html=html.replace(old_open, log+'<details style="margin-top:10px"><summary style="cursor:pointer;font-weight:700;color:var(--indigo-dark)">News refresh — 1 Sep 2026')

open(P,'w',encoding='utf-8').write(html)
print('written; delta chars',len(html)-len(orig))

# post-fixes
h=open(P,encoding='utf-8').read()
for a,b in [("the J&K SPV surfaced in batch 8: 7.63 lakh meters","the J&K SPV: 7.63 lakh meters"),("Maithon Gemstar, batch 9)","Maithon Gemstar)"),("JKPL Packaging — all surfaced in batch 8. World","JKPL Packaging. World")]:
    assert h.count(a)==1,a; h=h.replace(a,b)
open(P,'w',encoding='utf-8').write(h)
print('post-fixes applied')
