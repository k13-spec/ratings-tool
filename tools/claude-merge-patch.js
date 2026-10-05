// v6.6 pass-2 merge: layers the cloud news-refresh findings on top of the
// 5-Oct v6.6 refresh already on master (37fd6c9). Every operation asserts an
// exact unique match; the script exits non-zero without writing on any miss.
const fs=require('fs');
const FILE=process.argv[2]||'assets/debt_financing_ideas.html';
let src=fs.readFileSync(FILE,'utf8');
let errors=[];
function count(h,n){let c=0,i=0;while((i=h.indexOf(n,i))!==-1){c++;i+=n.length;}return c;}
function lit(old,neu,label){const c=count(src,old);if(c!==1){errors.push(`[lit ${label}] matches=${c}`);return;}src=src.replace(old,neu);}
function cardSpan(frag){
  const re=new RegExp('"?(?:name|n)"?\\s*:\\s*"[^"]*'+frag.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'[^"]*"');
  const m=re.exec(src); if(!m)return null;
  let s=src.lastIndexOf('{',m.index);
  let d=0,q=false,e=false,end=s;
  for(let i=s;i<src.length;i++){const c=src[i];
    if(e){e=false;continue}if(c==='\\'){e=true;continue}
    if(c==='"'){q=!q;continue}if(q)continue;
    if(c==='{')d++;else if(c==='}'){d--;if(!d){end=i+1;break}}}
  return [s,end];
}
function fieldSpan(cs,ce,f){
  const seg=src.slice(cs,ce);
  const m=new RegExp('"?'+f+'"?\\s*:\\s*').exec(seg); if(!m)return null;
  let k=m.index+m[0].length; const open=seg[k];
  if(open==='"'){let e=false;for(let i=k+1;i<seg.length;i++){if(e){e=false;continue}if(seg[i]==='\\'){e=true;continue}if(seg[i]==='"')return{type:'str',vs:cs+k+1,ve:cs+i};}}
  else if(open==='['||open==='{'){let d=0,q=false,e=false;for(let i=k;i<seg.length;i++){const c=seg[i];
    if(e){e=false;continue}if(c==='\\'){e=true;continue}if(c==='"'){q=!q;continue}if(q)continue;
    if(c==='['||c==='{')d++;else if(c===']'||c==='}'){d--;if(!d)return{type:open==='['?'arr':'obj',vs:cs+k+1,ve:cs+i};}}}
  return null;
}
function op(frag,f,kind,text,label){
  const sp=cardSpan(frag); if(!sp){errors.push(`[${label}] card not found`);return;}
  const fd=fieldSpan(sp[0],sp[1],f);
  if(!fd){errors.push(`[${label}] field ${f} missing`);return;}
  if(kind==='appendStr'){if(fd.type!=='str'){errors.push(`[${label}] not str`);return;}src=src.slice(0,fd.ve)+text+src.slice(fd.ve);}
  else if(kind==='appendArr'){if(fd.type!=='arr'){errors.push(`[${label}] not arr`);return;}src=src.slice(0,fd.ve)+','+text+src.slice(fd.ve);}
  else if(kind==='prependArr'){if(fd.type!=='arr'){errors.push(`[${label}] not arr`);return;}src=src.slice(0,fd.vs)+text+','+src.slice(fd.vs);}
  else if(kind==='setStr'){if(fd.type!=='str'){errors.push(`[${label}] not str`);return;}src=src.slice(0,fd.vs)+text+src.slice(fd.ve);}
}

// ---- Aster DM: merged-entity CRISIL AA+ (29-Jul-26) ----
op('Aster DM','rating','setStr',`CRISIL AA+/Stable/A1+ — merged entity assigned 29-Jul-26 (was A+ pre-merger)`,'aster-rating');
op('Aster DM','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> The re-rating landed — CRISIL assigned AA+/Stable/A1+ to merged Aster DM Quality Care on 29-Jul-26 (merger effective 1-Jul-26; cash >₹3,000 cr; ~21% proforma FY26 EBITDA margin; 3,600+ bed expansion to 2029), vindicating the ratings-migration thesis; an ICRA unit-level upgrade to AA followed ~mid-Sep-26 ⚑.`,'aster-rat');
lit(`"Rating":"A+ pre-merger (DB) — re-rating catalyst ⚑"`,`"Rating":"CRISIL AA+/Stable/A1+ (29-Jul-26) — migration delivered"`,'aster-kv');
lit(`"2026-27: merged-entity rating assignment is the event to trade around"`,`"29-Jul-26: CRISIL assigns AA+/Stable/A1+ to the merged entity — the migration ride delivered; ICRA unit upgrade to AA ~mid-Sep-26 ⚑"`,'aster-evo');
op('Aster DM','srcs','appendArr',`["CRISIL rationale — merged-entity AA+ (29-Jul-26)","https://www.crisil.com/mnt/winshare/Ratings/RatingList/RatingDocs/AsterDMQualityCareLimited_July%2029_%202026_RR_400641.html"]`,'aster-srcs');

// ---- Juniper: IPO completed + ICRA platform upgrade detail ----
op('Juniper Green Energy", city','rating','setStr',`ICRA AA−/Stable/A1+ platform — upgraded from A+ 17-Sep-26 post-IPO; SPVs upgraded 29-Sep-26`,'juniper-rating');
op('Juniper Green Energy", city','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Failed-equity flag RESOLVED — the IPO launched and completed: ₹1,800 cr raised in Aug-26 (~14% dilution of Juniper Renewable Holdings Pte). ICRA's upgrade press release (17-Sep-26) moved the platform [ICRA]A+ (Positive) → AA−/Stable + A1+ across ₹4,886.7 cr of facilities on IPO-led deleveraging (₹600 cr mezzanine prepaid; ₹811.9 cr of project loans refinanced), followed by SPV upgrades on 29-Sep-26 (Gamma One A+→AA−; Kite, Beam Eight, ETA Five and Power Five A−→A). Target >3 GWp + 1.4 GWh BESS by Mar-27 — the migration trade has paid; the residual trade is COD refis and BESS build debt.`,'juniper-rat');
lit(`"Next event":"IPO planned (~₹3,000 cr) ⚑"`,`"Next event":"IPO DONE — ₹1,800 cr (Aug-26); ICRA AA− platform upgrade 17-Sep-26"`,'juniper-kv');
op('Juniper Green Energy", city','ratingevo','appendArr',`"17-Sep-26 PR: upgrade driven by the ₹1,800 cr Aug-26 IPO (₹600 cr mezz prepaid; ₹811.9 cr refinanced)","29-Sep-26: SPV upgrades — Gamma One A+→AA−; four SPVs A−→A"`,'juniper-evo');
op('Juniper Green Energy", city','srcs','appendArr',`["ICRA upgrades Juniper platform to AA− (17-Sep-26)","https://www.icra.in/Rating/GetRationalReportFilePdf?id=126718"],["SPV upgrades after the ₹1,800 cr IPO (29-Sep-26)","https://solarquarter.com/2026/09/29/juniper-green-energy-subsidiaries-get-icra-rating-upgrades-after-inr-1800-crore-ipo/"]`,'juniper-srcs');

// ---- AAHL: first INR bonds ----
op('Adani Airport Holdings','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> The INR leg has begun — AAHL raised ₹1,000 cr via 3-yr bonds at 8.96% (quarterly coupon) for airport expansion/modernisation, reported 29-Sep-26 ⚑ aggregator-sourced — domestic bond investors are now entering the stack.`,'aahl-rat');
op('Adani Airport Holdings','maturities','appendArr',`"₹1,000 cr 3-yr 8.96% domestic bonds (29-Sep-26 ⚑) — first sizeable INR print"`,'aahl-mat');
op('Adani Airport Holdings','srcs','appendArr',`["AAHL ₹1,000 cr 3-yr bonds at 8.96% (29-Sep-26) ⚑","https://www.whalesbook.com/news/English/bankingfinance/Adani-Airport-Raises-indian-rupee1000-Crore-via-3-Year-Bonds/6abb582d5aacb956d0840f43"]`,'aahl-srcs');

// ---- Embassy: first trust-level direct bank NCDs ----
op('Embassy Office Parks','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Structural first — ₹1,000 cr of Series XVIII 3-yr floating-rate NCDs (initial 6.97%) were subscribed by a scheduled European multinational bank on 25-Sep-26, the first direct bank financing to an Indian REIT at trust level under RBI's Jun-26 framework (CARE AAA/Stable; refinancing use) — a new INR lender class opens for trust-level paper, exactly the INR-route deepening the trust rule anticipates.`,'embassy-rat');
op('Embassy Office Parks','maturities','appendArr',`"₹1,000 cr Series XVIII 3-yr FRN (6.97% initial) added 25-Sep-26 — first RBI-framework bank subscription"`,'embassy-mat');
op('Embassy Office Parks','ratingevo','appendArr',`"25-Sep-26: CARE AAA/Stable on the Series XVIII NCDs — first trust-level direct bank financing under the RBI Jun-26 framework"`,'embassy-evo');
op('Embassy Office Parks','srcs','appendArr',`["Embassy ₹1,000 cr NCDs to a European bank (25-Sep-26)","https://www.business-standard.com/industry/news/embassy-reit-raises-rs-1000-crore-ncds-european-bank-debt-refinance-126092500589_1.html"]`,'embassy-srcs');

// ---- Cube: ₹1,150 cr AAA NCD (2-Oct) ----
op('Cube Highways (Trust','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> The NCD machine prints again — ₹1,150 cr of AAA NCDs (7.50% quarterly, 5-yr) closed 28-Sep on NSE EBP and announced 2-Oct-26, subscribed Axis Bank ₹550 cr + ICICI Bank ₹600 cr, refinancing senior debt/CP plus capex/maintenance, part of a ₹4,500 cr multi-tranche programme; ICRA reaffirmed the Trust at AAA/Stable 7-Sep-26 (DB row).`,'cube-rat');
op('Cube Highways (Trust','finhist','prependArr',`"2-Oct-26: ₹1,150 cr AAA NCDs (7.50%/5-yr; Axis ₹550 cr + ICICI ₹600 cr) — ₹4,500 cr programme underway"`,'cube-fin');
op('Cube Highways (Trust','srcs','appendArr',`["Cube raises ₹1,150 cr AAA NCDs (2-Oct-26)","https://www.business-standard.com/content/press-releases-ani/cube-highways-trust-raises-rs-1-150-crore-through-aaa-rated-ncds-126100200489_1.html"]`,'cube-srcs');

// ---- ACME: CRISIL outlook Positive ----
op('ACME Solar','rating','setStr',`Project SPVs: ICRA/CRISIL AA− · CRISIL outlook on ACME Solar Holdings → Positive (2-Oct-26 ⚑ level)`,'acme-rating');
op('ACME Solar','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> CRISIL revised the outlook on ACME Solar Holdings to Positive (published 2-Oct-26 ⚑ rated level unconfirmed from snippets) and 107 MW/437 MWh of BESS was commissioned the same week — the upgrade-momentum leg is now agency-confirmed.`,'acme-rat');
op('ACME Solar','ratingevo','appendArr',`"2-Oct-26: CRISIL outlook → Positive on ACME Solar Holdings ⚑ details"`,'acme-evo');
op('ACME Solar','srcs','appendArr',`["CRISIL outlook revision — ACME (2-Oct-26) ⚑","https://www.kalkine.co.in/article/announcements/acme-solar-holdings-nseacmesolar-why-did-crisil-revise-its-outlook-to-positive"],["107 MW/437 MWh BESS commissioned (2-Oct-26) ⚑","https://www.ess-news.com/2026/10/02/acme-solar-commissions-107-mw-437-mwh-of-bess-in-india/"]`,'acme-srcs');

// ---- Avaada: holdco NCD + ECB refi plan ----
op('Avaada Energy','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> The INR leg arrives at holdco level — Avaada Ventures plans ₹3,515 cr of NCDs (Series A ₹3,240 cr + Series B ₹275 cr) at 11.75% fixed, ~3-yr, alongside a US$398mn senior secured ECB, to refinance existing liabilities (reported 17-Sep-26); the 11.75% print marks where structurally subordinated group paper clears — squarely private-credit territory.`,'avaada-rat');
op('Avaada Energy','maturities','appendArr',`"₹3,515 cr NCD + US$398mn ECB refi plan (17-Sep-26) — holdco-level, 11.75% area"`,'avaada-mat');
op('Avaada Energy','srcs','appendArr',`["Avaada Ventures ₹3,515 cr NCD + $398mn ECB plan (17-Sep-26)","https://renewablewatch.in/2026/09/17/avaada-ventures-plans-rs-35-15-billion-ncd-and-398-million-ecb-raise/"]`,'avaada-srcs');

// ---- IndiGo: CRISIL watch continued ----
op('InterGlobe Aviation','rating','setStr',`ICRA AA / Watch Negative (Mar-26, unresolved) · CRISIL AA−/A1+ Watch Developing continued 25-Sep-26 · USER ADDITION (11-Jul-26)`,'indigo-rating');
op('InterGlobe Aviation','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Watch season drags on — CRISIL continued its AA−/A1+ 'Watch with Developing Implications' on the ₹9,000 cr facilities on 25-Sep-26 (West Asia fuel/FX uncertainty, per the NSE disclosure), while ICRA's Mar-26 Watch Negative remains unresolved; Q2 FY27 results due late-Oct.`,'indigo-rat');
op('InterGlobe Aviation','ratingevo','appendArr',`"25-Sep-26: CRISIL continues AA−/A1+ on Watch Developing (₹9,000 cr facilities) — West Asia fuel/FX uncertainty"`,'indigo-evo');
op('InterGlobe Aviation','srcs','appendArr',`["CRISIL watch continued — NSE disclosure (25-Sep-26)","https://nsearchives.nseindia.com/corporate/Indigo1_25092026193740_Disclosure_CreditRating.pdf"]`,'indigo-srcs');

// ---- Manipal: FY26 numbers + IPO fast-track ----
op('Manipal Health','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Sahyadri is reported closed ⚑ (no exchange print — unlisted) and FY26 shareholder-approved numbers are out: revenue ₹10,336 cr (+25%), EBITDA margin ~25.6%; management is reported to be fast-tracking the IPO post-Sahyadri at a higher valuation ⚑ — either way the ₹5,300 cr FPI-bond bridge take-out remains the live trade.`,'manipal-rat');
op('Manipal Health','finhist','prependArr',`"FY26 (AGM-approved): revenue ₹10,336 cr (+25%), EBITDA margin ~25.6%"`,'manipal-fin');
op('Manipal Health','srcs','appendArr',`["Manipal fast-tracks IPO post-Sahyadri ⚑","https://www.outlookbusiness.com/corporate/manipal-hospitals-fast-tracks-ipo-plan-after-buying-sahyadri-eyes-higher-valuation"],["FY26 shareholder-approved results — rev ₹10,336 cr","https://scanx.trade/stock-market-news/companies/manipal-health-shareholders-approve-fy26-results-25-4-revenue-growth/52231708"]`,'manipal-srcs');

// ---- OYO: PRISM DRHP ----
op('OYO (Oravel','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> IPO mechanics firmed — parent (renamed PRISM) filed an updated DRHP on 30-Jun-26 for a ₹6,650 cr all-fresh-issue IPO (no OFS; possible ₹1,330 cr pre-IPO placement), proceeds routed to the Singapore subsidiary to repay/prepay the TLB-linked borrowings; SEBI approval reported ⚑ — the take-out path for the $825mn loan now runs through the listing.`,'oyo-rat');
lit(`"IPO proceeds partly to debt reduction (₹6,650 cr issue) — H2-26 event"`,`"IPO proceeds to repay/prepay the Singapore sub's TLB debt — ₹6,650 cr all-fresh issue (updated DRHP 30-Jun-26; SEBI nod reported ⚑)"`,'oyo-mat');
op('OYO (Oravel','srcs','appendArr',`["PRISM files updated ₹6,650 cr DRHP (30-Jun-26)","https://groww.in/blog/oyo-parent-prism-files-updated-drhp-for-6650-crore-ipo-with-sebi"]`,'oyo-srcs');

// ---- Godrej Industries: ₹750 cr NCD allotted ----
op('Godrej Industries','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> The proposed-NCD flag resolves — ₹750 cr of unsecured NCDs allotted 18-Sep-26 at 8.50% (~5.5-yr, maturity Mar-32, NSE-listed) ⚑ aggregator-sourced, following the ₹1,000 cr Jul-26 private placement; the holdco is now a repeat NCD issuer.`,'godrejind-rat');
op('Godrej Industries','maturities','appendStr',`; +₹750 cr 8.50% NCDs due Mar-32 (18-Sep-26) ⚑`,'godrejind-mat');
op('Godrej Industries','srcs','appendArr',`["Godrej Industries ₹750 cr NCDs at 8.50% (18-Sep-26) ⚑","https://www.whalesbook.com/corporate-news/English/bankingfinance/Godrej-Industries-Raises-Rs-750-Crore-via-Non-Convertible-Debentures/6aace924f2017017ae6d0dd1"]`,'godrejind-srcs');

// ---- L&T: tokenized NCD ----
op('Larsen & Toubro','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Market-structure first — ₹500 cr of 3-yr 7.40% unsecured NCDs allotted 9-Sep-26 via NSDL's DLT platform, India's first tokenized corporate NCD issuance (NSE listing planned).`,'lt-rat');
op('Larsen & Toubro','maturities','appendArr',`"₹500 cr 3-yr 7.40% tokenized NCDs (9-Sep-26) — first DLT-platform corporate issuance"`,'lt-mat');
op('Larsen & Toubro','srcs','appendArr',`["L&T tokenized NCDs — India's first (9-Sep-26)","https://www.investywise.com/larsen-toubro-tokenized-non-convertible-debentures-allotted"]`,'lt-srcs');

// ---- Hindalco: AluChem scrapped ----
op('Hindalco','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Scrapped the US$125mn AluChem (US specialty alumina) acquisition on 3-Oct-26 ⚑, citing the prolonged CFIUS review and US government-shutdown delays — the 1mt specialty-alumina-by-FY30 goal stands, now organic-first.`,'hindalco-rat');
lit(`acquisitions:"Aleris (2020, $2.8bn) history; current cycle organic. Copper/recycling bolt-ons possible."`,`acquisitions:"Aleris (2020, $2.8bn) history; AluChem (US$125mn, announced Jun-25) scrapped 3-Oct-26 on CFIUS delays ⚑ — current cycle organic. Copper/recycling bolt-ons possible."`,'hindalco-acq');
op('Hindalco','srcs','appendArr',`["Hindalco cancels the US$125mn AluChem deal (3-Oct-26) ⚑","https://www.whalesbook.com/news/English/industrial-goodsservices/Hindalco-Cancels-dollar125-Million-US-Deal-After-Regulatory-Delays/6ac009965aacb956d089d875"]`,'hindalco-srcs');

// ---- Greenko: negative confirmation ----
op('Greenko (Greenko','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Still no public confirmation of the 29-Jul-26 US$535mn Greenko Solar retirement ⚑ (two full news passes; funding remains pre-placed via the NaBFID ₹6,200 cr line incl. the ₹4,800 cr Feb-26 tranche, and no default/distress signals anywhere) — next pass should go straight to SGX/India INX filings of the RG issuers.`,'greenko-rat');

// ---- Apraava: Raigad transmission SPV acquisition ----
op('Apraava Energy','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> Beyond the AA+ row — Apraava acquired the Raigad Power Transmission SPV (TBCB) on 29-Sep-26: ~400 ckm incl. ~340 ckm of 765 kV lines plus a 4,500 MVA pooling station evacuating ~3 GW of Raigad pumped storage, 24-month build on a 35-yr BOOT — another ₹1,000 cr-plus financing lane beside the meter roll-out.`,'apraava-rat');
lit(`acquisitions:"Concession wins (AMISP auctions) rather than M&A."`,`acquisitions:"Concession wins (AMISP auctions) plus TBCB M&A — Raigad Power Transmission SPV acquired 29-Sep-26 (~400 ckm / 4,500 MVA, 35-yr BOOT)."`,'apraava-acq');
op('Apraava Energy','srcs','appendArr',`["Apraava acquires Raigad transmission SPV (29-Sep-26)","https://solarquarter.com/2026/09/29/apraava-energy-acquires-raigad-power-transmission-spv-to-develop-400-ckm-transmission-project-in-maharashtra/"]`,'apraava-srcs');

// ---- TMCV: ICRA 29-Sep reaffirm ----
op('Tata Motors (CV)','rating','setStr',`CRISIL/ICRA/CARE AA+/Stable (CV entity; ICRA reaffirmed 29-Sep-26, rated amount ₹19,818 cr); Tata Sons letter of support on Iveco funding`,'tmcv-rating');
op('Tata Motors (CV)','rationale','appendStr',` <b>Pass 2:</b> ICRA reaffirmed [ICRA]AA+/Stable/A1+ on 29-Sep-26 with the Iveco bridge factored — rated amount enhanced ₹17,600 → ₹19,818 cr; the rationale cites net cash ~₹13,500 cr (Jun-26), 36.8% Q1 FY27 CV share and 12-18-month deleveraging of the bridge-funded structure.`,'tmcv-rat');
lit(`"⚑ Post-Iveco rating action pending; Tata Sons support noted"`,`"29-Sep-26: ICRA AA+/Stable/A1+ reaffirmed with the Iveco bridge factored — rated amount enhanced to ₹19,818 cr; Tata Sons support noted"`,'tmcv-evo');
op('Tata Motors (CV)','srcs','appendArr',`["Tata Motors launches €3.82bn Iveco tender (7-Sep-26)","https://www.autocarpro.in/news/tata-motors-launches-382-billion-cash-tender-offer-for-iveco-group-134546"],["ICRA rationale — TML CV reaffirmed (29-Sep-26)","https://www.icra.in/Rating/GetRationalReportFilePdf?id=146031"]`,'tmcv-srcs');

// ---- RIL: the second (10-yr) tranche ----
op('Reliance Industries (group','rationale','appendStr',` <b>Pass 2:</b> A further ₹13,000 cr 10-yr tranche at 7.90% (~G+65bp) was reported priced 1-Oct-26 ⚑ — taking the month's issuance to ₹25,000 cr, RIL's largest monthly domestic bond borrowing on record.`,'ril-rat');
op('Reliance Industries (group','maturities','appendArr',`"₹13,000 cr 10-yr 7.90% tranche reported 1-Oct-26 ⚑ — ₹25,000 cr total Sep-26 supply"`,'ril-mat');
op('Reliance Industries (group','srcs','appendArr',`["RIL ₹13,000 cr 10-yr at 7.90% (1-Oct-26) ⚑","https://en.channeliam.com/2026/10/01/reliance-industries-10-year-bond-issue/"]`,'ril-srcs');

// ---- Sun: corroborating sources ----
op('Sun Pharmaceutical','srcs','appendArr',`["Sun eyes ₹10,000 cr bond sale for the Organon bridge (30-Sep-26)","https://medicaldialogues.in/amp/news/industry/pharma/sun-pharma-eyes-rs-10000-crore-bond-sale-to-fund-organon-acquisition-loan-180353"],["Organon shareholders approve the merger (24-Jul-26)","https://www.outlookbusiness.com/corporate/organon-shareholders-approve-sun-pharma-merger-deal-nears-completion"]`,'sun-srcs');

// ---- STL (new card): the 1-Oct US$1.2bn hyperscaler agreement ----
op('Sterlite Technologies (STL)", city','rationale','appendStr',` <b>Update (5-Oct-26, pass 2):</b> The order book keeps compounding — the US arm signed a supply agreement worth up to US$1.2bn (~₹11,500 cr) with a hyperscaler for optical connectivity products, Jan-26→Dec-30 (annual POs, capped reciprocal liabilities), announced 1-Oct-26 — distinct from the May-26 US$1.1bn contract, further underwriting the ₹3,000 cr build.`,'stl-rat');
op('Sterlite Technologies (STL)", city','finhist','appendArr',`"1-Oct-26: US$1.2bn 5-yr hyperscaler supply agreement (US arm) — distinct from the May-26 order"`,'stl-fin');
op('Sterlite Technologies (STL)", city','srcs','appendArr',`["STL US$1.2bn hyperscaler supply agreement (1-Oct-26)","https://www.businesstoday.in/markets/stocks/story/sterlite-tech-shares-gain-5-as-arm-wins-1-2-billion-supply-agreement-558989-2026-10-01"]`,'stl-srcs');

// ---- WATCH re-tests ----
lit(`but FY27 capex guided ₹700–800 cr — still sub-gate; kept."`,`but FY27 capex guided ₹700–800 cr — still sub-gate; kept. Re-test 5-Oct-26: board passed an enabling resolution (1-Oct-26) to raise up to ₹1,000 cr via equity/NCD/QIP routes ⚑ — no committed issue yet; capex still sub-gate; kept."`,'watch-aarti');
lit(`kept — Birla Estates is the entity to re-screen once it levers."`,`kept — Birla Estates is the entity to re-screen once it levers. Re-test 5-Oct-26: CARE reaffirmed AA and revised the outlook to Positive (30-Sep-26 ⚑ aggregator) on ₹500 cr NCDs + planned ₹250 cr, citing ₹1,825 cr of bank-facility prepayment — still deleveraging; kept."`,'watch-abre');
lit(`Revisit if BOOT/HAM equity needs or a large acquisition re-lever the book (NCC precedent)."`,`Revisit if BOOT/HAM equity needs or a large acquisition re-lever the book (NCC precedent). Re-test 5-Oct-26: order momentum — UAE gas-pipeline EPC LoA >₹4,000 cr (28-Sep-26) plus ₹2,025 cr of T&D/O&G orders (24-Sep-26); still net-debt-light with no term-debt trigger; kept."`,'watch-kpil');
lit(`Promote when audited FY25/FY26 financials verify the gate and an INR NCD/term programme surfaces."`,`Promote when audited FY25/FY26 financials verify the gate and an INR NCD/term programme surfaces. Re-test 5-Oct-26: ~US$1.1bn of owner support (Tata Sons/SIA) reported closing 3-Sep-26 ⚑ — equity-side, not an INR debt event; gate figures still unverified; kept."`,'watch-airindia');
lit(`if the greenfield programme ≥₹2,000 cr firms up, the under-levered balance sheet makes it a clean capex-finance candidate."`,`if the greenfield programme ≥₹2,000 cr firms up, the under-levered balance sheet makes it a clean capex-finance candidate. Re-test 5-Oct-26: the >₹2,000 cr UP greenfield (60k tractors + 15k CE units p.a.) broke ground 19-Aug-26 but is funded from Kubota preferential-issue proceeds + accruals — zero new debt (FY27 cash capex ≤₹900 cr); trigger fails on funding mix; kept."`,'watch-escorts');
lit(`the DRHP, when filed, is the verification source."}`,`the DRHP, when filed, is the verification source. Also 3-Oct-26 ⚑: reports of KO planning a ~US$1bn IPO of Hindustan Coca-Cola Holdings at ~US$10bn valuation, consistent with the 2027 listing track."}`,'watch-hccb');

// ---- HERO: add pass-2 highlights ----
lit(`· Greaves IPO abandoned) · weekly news refresh`,`· Greaves IPO abandoned · pass 2: Aster DM merged-entity CRISIL AA+ (29-Jul) · Juniper ₹1,800 cr IPO done + ICRA AA− platform upgrade (17-Sep) · Embassy ₹1,000 cr first trust-level direct-bank NCDs · Cube ₹1,150 cr + AAHL ₹1,000 cr prints · Avaada ₹3,515 cr holdco NCD plan · ACME outlook Positive) · weekly news refresh`,'hero');

// ---- LOG: pass-2 block before the 1-Sep block ----
const anchor1sep=`<details style="margin-top:10px"><summary style="cursor:pointer;font-weight:700;color:var(--indigo-dark)">News refresh — 1 Sep 2026 (news-refresh mode · v6.5)</summary>`;
if(count(src,anchor1sep)!==1){errors.push('[log] 1-Sep anchor not unique');}
else{
const block=`<details style="margin-top:10px" open><summary style="cursor:pointer;font-weight:700;color:var(--indigo-dark)">News refresh — 5 Oct 2026 · pass 2 (cloud run, merged · v6.6)</summary><div style="font-size:13px;color:var(--muted);margin-top:8px"><p><b>Second pass the same day</b> — a parallel cloud run of the weekly scan, merged on top of the morning's v6.6 without disturbing its calls (STL promotion, Greaves closure, Tata Steel redemption, Serentica reconcile all stand). <b>Additional card updates (17):</b> Aster DM (⚑ resolved — merged entity assigned CRISIL AA+/Stable/A1+ 29-Jul; ICRA unit upgrade to AA mid-Sep ⚑) · Juniper (failed-equity flag resolved — ₹1,800 cr IPO completed Aug-26; ICRA upgrade PR 17-Sep: A+ (Positive) → AA−/Stable on IPO-led deleveraging, ₹600 cr mezz prepaid; SPV upgrades 29-Sep) · Embassy REIT (₹1,000 cr 3-yr FRN 6.97% to a European bank 25-Sep — FIRST trust-level direct bank financing under RBI's Jun-26 framework) · Cube (₹1,150 cr AAA NCD 2-Oct — Axis ₹550 cr + ICICI ₹600 cr, 7.50%/5-yr, ₹4,500 cr programme; ICRA AAA reaffirm 7-Sep) · AAHL (₹1,000 cr 3-yr 8.96% domestic bonds 29-Sep ⚑ — first sizeable INR print) · Avaada (₹3,515 cr holdco NCD @11.75% + $398mn ECB refi plan 17-Sep) · ACME (CRISIL outlook → Positive 2-Oct ⚑; 107 MW/437 MWh BESS commissioned) · Apraava (Raigad transmission SPV acquired 29-Sep — ~400 ckm, 4,500 MVA, 35-yr BOOT) · TMCV (ICRA AA+/Stable/A1+ reaffirmed 29-Sep with the bridge factored — ₹19,818 cr) · RIL (second tranche: ₹13,000 cr 10-yr 7.90% reported 1-Oct ⚑ — ₹25,000 cr Sep total) · IndiGo (CRISIL AA−/A1+ Watch Developing continued 25-Sep; ICRA Watch Neg unresolved) · Manipal (Sahyadri reported closed ⚑; FY26 rev ₹10,336 cr +25%; IPO fast-track ⚑) · OYO (PRISM ₹6,650 cr all-fresh DRHP 30-Jun; proceeds to the TLB; SEBI nod ⚑) · Godrej Industries (₹750 cr 8.50% NCD allotted 18-Sep ⚑ — proposed-NCD flag resolved) · L&T (₹500 cr 7.40% tokenized NCD 9-Sep — India's first DLT corporate issuance) · Hindalco (AluChem US$125mn scrapped 3-Oct on CFIUS ⚑) · STL card (US$1.2bn hyperscaler supply agreement 1-Oct — distinct from the May-26 US$1.1bn order) · Greenko (negative confirmation logged — $535mn Jul-26 retirement STILL unverified ⚑ after two passes; check SGX/INX filings next).</p><p><b>No material change (pass-2 sweep):</b> JSW Steel (JSW Energy + JSW Steel plan ₹2,850 cr of bonds Oct–Dec, 21-Sep; JSW Energy ₹500 cr 7.90% 7-yr NCD 28-Sep at parent level) · UltraTech (4.6 mtpa commissioned 24-Sep) · Adani Green (BESS 6.63 GWh — 70% of FY27 target) · ABReL (CRISIL Watch Developing STILL unresolved; CCI cleared the GIP infusion 26-Mar) · Torrent Pharma (NCLT approved the JB Chem amalgamation 6-Jul) · SAMIL (Rotary Connectors 50.1% ~₹500 cr, 21-Sep) · Aurobindo (Ind-Ra AA+ affirmed — DB grade confirmed ⚑ dated print) · Tata Electronics (Dholera vendor park 4-Oct; ₹61,280 cr guarantees in the FY26 AR) · CG Power (G1 Sanand commercial production since 4-Jul) · Dorf-Ketal (Italmatch unresolved; own IPO deferred ⚑) · Zydus (Sunshine Lanka JV deadline → Dec-26 ⚑) · Biocon Biologics (Fitch outlook Positive 7-Feb) · Continuum (IPO withdrawn + $67.5mn Just Climate, Apr-26 ⚑) · Torrent Green (322 MWp Nashik commissioned 8-Sep) · Shapoorji ($1.6bn private-credit close 20-Jul — no new print) · Prestige/Vertis/UPL/Vedanta/Sun/Tata Steel/Serentica/JSW Paints/Greaves — covered by the morning pass. DB-row date refreshes: Godrej Properties ICRA AA+ (2-Sep) · KIMS ICRA AA (15-Sep) · Interise AAA (17-Sep) · Cube Trust AAA (7-Sep) · JSW Neo standalone AA−/A1+ (11-Sep).</p><p><b>Watchlist re-tests (pass 2, no further promotions):</b> HCCB (KO ~US$1bn IPO at ~$10bn reported 3-Oct ⚑) · Aarti (₹1,000 cr enabling resolution 1-Oct ⚑ — no committed issue) · Aditya Birla Real Estate (CARE AA outlook → Positive 30-Sep ⚑) · KPIL (UAE EPC LoA >₹4,000 cr 28-Sep + ₹2,025 cr orders 24-Sep — still deleveraged) · Air India (~$1.1bn owner support reported 3-Sep ⚑ — equity-side) · Escorts Kubota (UP greenfield equity-funded — zero new debt) · Superform / Cohance / Sona BLW / Redington / ABFRL / Bharat Forge / JSW Hydro / Grasim / Shree / Anzen / Energy Infrastructure Trust (nothing material). All kept. New-candidate sweeps: no qualifiers 28-Sep–5-Oct (SG Mart CRISIL AA− upgrade 17-Sep — trading-led, thin EBITDA, no trigger). CARE/CRISIL rows in ratings_current.csv carry unparseable date formats ⚑ scraper formatting — flagged for the pipeline.</p></div></details>`;
src=src.replace(anchor1sep, block+anchor1sep);
}

if(errors.length){console.error('ERRORS:\n'+errors.join('\n'));process.exit(1);}
fs.writeFileSync(FILE,src);
console.log('pass-2 merge applied cleanly');
