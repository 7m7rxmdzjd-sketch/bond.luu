import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from vnstock.technical import stock_historical_data

# === CẤU HÌNH ===
symbols = [
    "MVB","NST","CLM","TMB","PHN","VIF","KSV","CMC","TTL","SDC","VC1","VC6","VC7","VC2","VTV","LIG","LCD","ICG","VNC","ARM","TET","MED","X20","PVS","PVC","PVI","ONE","AME","TTH","CKV","POT","IPA","ATS","SD9","SD5","PJC","VC9","TIG","CEO","VC3","VMC","VNT","SDA","S99","PIA","MCO","HHC","V12","PLC","SRA","CSC","EBS","INN","HEV","SVN","SHE","CX8","KSQ","API","HDA","HAT","ECI","IDJ","SDU","EID","VCM","SHN","PV2","CMS","PVG","HLD","PCG","ADC","MBG","PPE","VLA","PPS","PMB","MST","CET","DTK","PCH","FID","DVM","THD","PRE","GMA","BNA","TVC","CAR","KSF","VHE","SCG","DNC","NTP","TSB","CAN","TJC","KKC","PTS","DXP","MAC","DP3","HCT","DHP","VMS","BXH","PPT","GIC","VSA","RCL","PMC","D11","SPC","SFN","SGH","WCS","STC","UNI","ALT","PPP","TMC","GLT","TV3","PMS","VTC","SJ1","PSC","IDC","VGP","PTD","HTC","VNF","HMH","PEN","KST","SGD","TFC","PGT","TPP","SAF","NBW","BTW","GDW","INC","SED","PCT","VTJ","PGS","BSC","PSD","PSE","TOT","SMN","DDG","VTZ","NRC","TA9","NDN","KMT","CDN","MAS","VSM","PRC","VE1","DAE","BED","SSM","CJC","PIC","NDX","PDB","V21","SJE","DHT","STP","QHD","VCS","SCI","DST","HUT","BBS","PVB","BTS","DTD","KSD","KDM","GKM","L18","C69","HAD","CTB","OCH","AAV","ITQ",
    "TBX","LBE","MCF","DTG","MKV","BCF","SGC","CAG","NVB","KHS","SDG","L40","CPC","PSW","PBP","VIT","NAG","IDV","MEL","VGS","PGN","L14","HVT","LAS","NSH","NFC","NBP","BCC","BPC","THS","THB","SPI","VNR","NAP","BAB","HOM","VBC","VE4","VE3","AMC","HMR","CTP","DAD","TXM","GMX","DC2","PMP","TKU","SDN","NET","DNP","TTC","BAX","VTH","SMT","SZB","NHC","MCC","AMV","TTT","MIC","HCC","VCC","QTC","DIH","PCE","PTI","CCR","TV4","SEB","CIA","NTH","PPY","TNG","TDT","BKC","HGM","HJS","CAP","HKT","SLS","CST","VHL","MDC","THT","QST","HLC","NBC","TVD","DS3","DTC","CTT","LDP","LHC","VDL","S55","DL1","VE8","KTS","VGT","TVN","XPH","HMG","KTL","DFC","HSM","GTD","TV1","HTM","DLT","M10","TBD","MIE","MGG","BT1","HFX","HNR","VNY","DCH","MIM","VHF","SPH","HEJ","MCG","VEC","EMG","HSP","VEA","VLC","TVG","MVN","C12","CT6","VWS","CDG","HCI","HMS","S12","DDM","VGV","GH3","XDH","CH5","HNB","ICC","NOS","TMX","HHN","HC1","SJG","VFR","VIW","NS2","CNN","CCV","MES","SD1","LLM","HAN","CKD","L12","LIC","TCK","HAF","USC","TSJ","TH1","TVA","VXT","TED","VNX","IHK","PEQ","HFC","PCC","NAS","VFC","DP1","X26","PBC","VTK","DP2","AMP","DVN","CTX","PHH","VNB","ILS","VEF","HD6","TA6","ACM","C22","PMT","TEL","VIE","PCM","PTP","HNP","VIH","BTH","VVN","HES","BVG","DCS","HD2","CIP","DAG","FHN","CMT","LTC","RAT","HNM","DDB","TST","DHN","CTN","PVM","FBA","DAC","SJC","S96","APP","VCT","NDC","PLE","ICI","NCS","TBH","VPC","VAV","PEC","FOX","PLA","EMS","TB8","ABC","CTA","PWA","STL","HNF","HRB","VW3","PVV","HU6","SDB","HAV","SBM","CVN","H11","LCS","APL","PFL","VHD","VTE","BSH","PTT","HD8","VLG","AMD","EIC","PVL","CMI","VTI","VGI","EFI","PIV","MFS","SIG","VCR","VSE","FLC","TTG","TIN","CK8","CDO","BVL","PVO","PAI","PID","DFF","TAL","DTP","KLF","HKB","XLV","VVS","NBE","HVA","DPS","VBG","MZG","CCC","G36","TGG","SJF","IBC","E29","LMC","TOP","NHP","HSV","HSA","SSH","ECO","MTL","NWT","BLN","TAB","BMV","CEN","ODE","MDA","CFM","BIG","VMK","HIO","TRV","TVH","VNA","DPH","FSO","DDH","BHP","L62","BAL","ITS","HPW","PHP","ILC","HMD","CID","V15","TR1","TKG","DVC","SIV","HND","HC3","CDH","HPB","HPP","CCP","QBS","PSP","HBH","AMS","VPA","DDV","TUG","CPH","VGR","VSN","VNP","SSF","GND","VTA","MNB","VGG","FIC","NAC","CT3","SVG","CHS","SGS","PNT","SAL","ISG","SWC","VST","GTS","IN4","STS","PMJ","VTR","UPH","BSG","MRF","SGP","CDP","DM7","A32","APT","TL4","FCS","HEC","ACS","SCD","VOC","VET","SGB","VSF","VKP","IME","VIN","PEG","RBC","FRM","RCD","BBT","FTI","NSG","C21","AGX","DSP","TSG","TPS","SAS","TNM","BTV","TNA","TIE","BVN","HAI","DCF","SSN","VPR","CLX","BVB","ABB","PNG","CC1","LG9","L45","PPH","CGV","NCG","HPT","BLI","CNT","DTI","PVE","GER","TKC","ICF","VSG","BMG","EME","HFB","MCH","SSG","KVC","CI5","HBC","SCO","VNH","HLA","TS4","YTC","NDP","CMD","MKP","SHC","SPV","HNI","TTD","ITA","LUT","KDF","VAB","DIC","SAP","BTD","SBD","VBH","TBR","VES","VDT","SII","VNZ","STT","PPI","CLG","HSI","SBB","FHS","TRS","PCF","CMF","AG1","CMN","PTO","PJS","VNI","SID","PDV","NTB","FOC","HPI","EIN","TLI","MHL","HTE","LSG","TNB","TDS","PTV","OIL","PSG","ONW","PNP","TCW","MSR","SAC","THW","TAW","SEA","MML","VTD","ACV","TOS","LMH","MGR","ILA","DTE","BGE","AIG","BCR","SGI","VDG","THP","PRO","SPD","LM7","DNN","DDN","DAS","VTX","TW3","DAN","DNM","DNE","DCR","DPC","VDN","VMT","SJM","HTP","SVH","DNL","TS3","DSD","XMP","HU3","MA1","SD2","TVM","VXP","FCC","SD8","BHK","MTH","PTH","HTT","XMC","SCJ","SDP","KIP","SDD","IDP","ASA","DVG","PVR","MPT","UMC","NDW","NDT","MND","BBM","NDF","KTT","NJC","TV6","PND","X77","KSH","TSA","THM",
    "HDW","L63","DHD","TRT","BBH","LPT","V11","KHD","BCA","TGP","VRG","SCL","DKG","HUG","CNC","PAS","TBW","MTB","HHG","BTB","TTZ","FTM","POB","BOT","LAW","MTG","LAI","HVG","BTG","NBT","VXB","BTU","TBT","GPC","DWS","BDT","PGB","DOP","TCJ","DMN","VLP","VLF","VLW","CKA","LTG","AFX","ANT","AGM","DNA","ATA","AGF","ACE","AGP","AVF","KGM","KLB","NGC","KTC","CTW","TAR","UCT","HAM","CCM","WSB","CCA","CCT","TOW","BLF","SBL","CMW","CAD","JOS","SNC","CAT","MPC","PXC","CMM","STW","USD","UXC","VBB","DSG","VTS","L61","DCG","BNW","MBN","DHB","BGW","AAH","HPH","NVP","VPW","XHC","TLT","XMD","G20","SHG","LM3","GVT","BSD","PTE","HHB","BSP","F88","L35","THU","THN","DTH","LO5","VC5","HU4","VCP","PVH","PSN","GAB","NAW","TDF","HLT","C4G","PX1","PVA","CNA","NAU","C92","PTX","NTF","SRB","VE2","QPH","TKA","SB1","PDC","BSL","TSD","PXA","HDP","MTA","GSM","S27","POV","MLS","VTQ","LNC","NQB","BQB","E12","MQB","SEP","MDF","HDM","RCC","HEP","HGT","MTP","HWS","SPB","VHH","CMP","ALV","PTG","BII","BMD","BRS","ICN","UDC","BWS","VTG","PVX","BRR","MTV","UPC","DC1","PMW","VMG","BMK","PSB","PVY","PXL","POS","TNS","PXI","PXT","VIR","PXS","DMS","VGL","IFS","DPP","TMW","BEL","L44","IRC","DNW","DND","DGT","HJC","NSS","PSL","SZE","VLB","DNT","TID","DOC","SNZ","BHC","DCT","BMF","DID","SDK","SDV","LMI","NTW","DVW","LKW","CDR","PAP","GCF","SZG","TLP","PRT","MVC","GDA","POM","DZM","BT6","HBD","APC","VKC","NTC","BCP","BDG","IST","UDJ","PLO","TTN","BMJ","HLO","MBT","IBD","SBR","MH3","ISH","ABI","RTB","CHC","FRC","HOT","QNU","QCC","VHG","AVC","QNT","BDW","PIS","BLT","TNP","GCB","BTN","TDB","ATG","SQC","QSP","NTT","KHW","NUE","BIO","VE9","SKV","SKH","SKN","QNS","LQN","MQN","APF","QNW","BSQ","PQN","PXM","PBT","PWS","MPY","GTT","L43","SD6","SBH","NNT","TMG","TNW","TIS","MEF","STH","FBC","FT1","TTB","CQT","TTS","NHV","KSS","ATB","CYC","CBS","KCB","BCV","CBI","DXL","NLS","DKC","LCC","HPM","AIC","TQW","GGG","VCX","YBC","HLS","VIM","MLC","LCM","ND2","SP2","GLC","BHA","PAT","SD7","SCC","S72","MEC","VCW","NSL","NED","S74","BHI","NQN","CQN","QNC","VQC","MTS","RIC","VMA","KHL","QHW","HLB","QTP","HLY","VTM","TQN","UEM","CMK","WTC","CPI","BCB","MGC","VDB","TD6","DLR","LDW","DUS","DNH","BWA","BHG","SD4","SDT","SD3","GLW","FGL","GHC","SDY","HPD","TAN","DRG","DWC","DLD","UDL","CFV","EPC","CPA","DBM","BSA","DRI","NXT","AVG","TT6","VCE","GEX","SRC","PC1","NHH","VAF","TMT","VCG","PLX","HVN","VGC","TRA","BVH","CTG","VCB","BID","TCB","VIB","VPB","CMG","HDG","MBB","ICT","PTC","SHI","MHC","HU1","HAS","HID","CRE","JVC","LGL","DPG","VIC","FPT","VPD","PHC","BHN","SJS","EVE","ELC","PGC","NSC","DGC","FCN","TNI","RAL","NTL","NCT","TNT","NVT","FIT","POW","OGC","VJC","VHM","KOS","VPI","TPB","EVF","ADG","TTA","PLP","HAH","KPF","VTP","EVG","HVH","CTR","ASG","CRC","TEG","NO1","VRE","TN1","BKG","AST","GEE","BAF","VOS","VIP","TCH","MSB","SSB","HAP","VSC","DVP","TCO","HHS","VPG","HHP","DQC","VID","SC5","RDP","LM8","VTB","VSI","HMC","PAC","VPS","CSM","TV2","SFG","COM","PET","VDP","VMD","LGC","TCD","PNJ","SMA","VTO","SAB","VNM","FDC","HDB","SFC","STG","SVT","REE","GMC","SBV","OCB","NAB","STB","GMD","BRC","SVC","BTT","EIB","GVR","HTL","NLG","LIX","NVL","TVT","TCM","CSV","HT1","GDT","ACB","BMP","TLG","TIX","SFI","PAN","ITD","MCP","VNL","PIT","VPH","PJT","SRF","PNC","TMS","SGR","HTV","HAX","STK","VNS","ADP","HQC","HTI","GIL","ITC","NAV","SPM","LCG","SAV","VFG","TDH","CII","PVD","OPC","ST8","KDH","SGT","SSC","CCI","KDC","PVT","TPC","DSN","DGW","BFC","DXG","DTA","IDI","CDC","DPM","CLC","ASP","NHT","DTT","SCR","SHP","CTD","PDR","SMC","MSN","NBB","DRH","YEG","CLW","TDW","TCL","PGD","HAR","CLL","SIP","PTL","HTN","GSP","PVP","SCS","MWG","SHA","CTF","KHG","BCG","AGG","FRT","ABR","SGN","SBG","HVX","VNE","DRC","HTG","DXV","HHV","SBA","DRL","CHP","LEC","FIR","CIG","TLD","HPX","MSH","SVD","NHA","FCM","PPC","AAA","HCD","APH","HPG","TDP","ADS","LAF","LHG","BIC","THG","CTES","DHC","ABT","DBT","VHC","IMP","DMC","DCL","ANV","ASM","ACL","DAT","CKG","SKG","DHG","SHB","AAM","TSC","CMX","CMV","DCM","FMC","CCL","DBC","KBC","TDG","CVT","BMI","AAT","LSS","NAF","HNA","GMH","HUB","ABS","TNC","DIG","VRC","GAS","HDC","HRC","DC4","BTP","VNG","CNG","RYG","PGV","TYA","SAM","TCR","MDG","D2D","VCF","PDN","BBC","CTI","DHA","UIC","TLH","SVI","SZL","NT2","SZC","VCA","TIP","ILB","LDG","DXS","BCM","BWE","C32","PHR","KSB","DTL","HSG","KMR","GTA","BCE","TDC","NKG","TTF","ACG","NNC","IJC","ACC","TDM","DPR","TMP","SJD","TRC","SBT","TCT","PMG","C47","QNP","PTB","DBD","BMC","VSH","SMB","VPL","KHP","MIG","BSR","DHM","DAH","TNH","TBC","HII","YBM","L10","MCM","HSL","LBM","GEG","HAG","QCG","S4A","DLG","HNG","TTE","LPB","PSH","PGI"
]

ATR_PERIOD = 1
ATR_MULT = 0.75
START_DATE = "2023-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# === FILE CSV TRÊN DESKTOP ===
CSV_PATH = "/Users/hoangluu/Desktop/stock_history.csv"

# === Heikin Ashi ===
def heikin_ashi(df):
    df_ha = df.copy()
    df_ha["HA_Close"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4
    ha_open = [(df["Open"].iloc[0] + df["Close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[-1] + df_ha["HA_Close"].iloc[i-1]) / 2)
    df_ha["HA_Open"] = ha_open
    df_ha["HA_High"] = df_ha[["High", "HA_Open", "HA_Close"]].max(axis=1)
    df_ha["HA_Low"] = df_ha[["Low", "HA_Open", "HA_Close"]].min(axis=1)
    return df_ha

# === ATR ===
def calc_atr(df, period):
    high_low = df["High"] - df["Low"]
    high_close = abs(df["High"] - df["Close"].shift())
    low_close = abs(df["Low"] - df["Close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# === Chandelier Exit ===
def chandelier_exit(df, atr_period, atr_mult):
    df = heikin_ashi(df)
    df["ATR"] = calc_atr(df, atr_period)
    df["haClose_prev"] = df["HA_Close"].shift(1)
    df["atr_prev"] = df["ATR"].shift(1)

    df["longStop1"] = df["HA_Close"].rolling(atr_period).max() - df["ATR"] * atr_mult
    df["longStop2"] = df["HA_Close"].shift(1).rolling(atr_period).max() - df["ATR"].shift(1) * atr_mult
    df["shortStop1"] = df["HA_Close"].rolling(atr_period).min() + df["ATR"] * atr_mult
    df["shortStop2"] = df["HA_Close"].shift(1).rolling(atr_period).min() + df["ATR"].shift(1) * atr_mult

    df["longStop"] = np.where(df["haClose_prev"] > df["longStop2"],
                              np.maximum(df["longStop1"], df["longStop2"]),
                              df["longStop1"])
    df["shortStop"] = np.where(df["haClose_prev"] < df["shortStop2"],
                               np.minimum(df["shortStop1"], df["shortStop2"]),
                               df["shortStop1"])

    dir_list = []
    prev_dir = 1
    for i in range(len(df)):
        if np.isnan(df["shortStop2"].iloc[i]) or np.isnan(df["longStop2"].iloc[i]):
            dir_list.append(prev_dir)
            continue
        if df["HA_Close"].iloc[i] > df["shortStop2"].iloc[i]:
            dir_list.append(1)
        elif df["HA_Close"].iloc[i] < df["longStop2"].iloc[i]:
            dir_list.append(-1)
        else:
            dir_list.append(prev_dir)
        prev_dir = dir_list[-1]
    df["dir"] = dir_list

    df["buySignal"] = (df["dir"] == 1) & (pd.Series(df["dir"]).shift(1) == -1)
    df["sellSignal"] = (df["dir"] == -1) & (pd.Series(df["dir"]).shift(1) == 1)

    return df

# === Load dữ liệu ===
def fetch_history(sym):
    try:
        return stock_historical_data(sym, START_DATE, END_DATE,
                                     resolution='1D', type='stock',
                                     beautify=False, decor=False)
    except:
        return stock_historical_data(sym, START_DATE, END_DATE,
                                     resolution='1D', type='stock',
                                     beautify=False, decor=False, source='DNSE')

# === Ghi CSV ===
def save_to_csv(date_str, results):
    if os.path.exists(CSV_PATH):
        df_csv = pd.read_csv(CSV_PATH)
    else:
        df_csv = pd.DataFrame({"Symbol": symbols})

    df_csv = df_csv.set_index("Symbol")

    # Thêm cột ngày mới
    df_csv[date_str] = results

    df_csv.to_csv(CSV_PATH)
    print(f"\n📁 ĐÃ LƯU VÀO FILE: {CSV_PATH}\n")


# === MAIN SCAN ===
def scan_signals():
    date_str = datetime.now().strftime("%Y-%m-%d")
    results = {}

    print("=== STOCK SCANNER (Chandelier Exit) ===")
    print("Symbol | Close | Signal")
    print("---------------------------")

    for sym in symbols:
        try:
            df = fetch_history(sym)
            if df is None or len(df) == 0:
                results[sym] = "NO_DATA"
                print(f"{sym:<6} | --- | No data")
                continue

            # Chuẩn hoá cột
            col_map = {c: c.capitalize() for c in df.columns}
            df.rename(columns=col_map, inplace=True)

            if not all(c in df.columns for c in ["Open", "High", "Low", "Close"]):
                results[sym] = "ERR"
                print(f"{sym:<6} | --- | Missing OHLC")
                continue

            df = chandelier_exit(df, ATR_PERIOD, ATR_MULT)

            last = df.iloc[-1]
            close_p = round(last["Close"], 2)

            if last["buySignal"]:
                signal = "BUY"
            elif last["sellSignal"]:
                signal = "SELL"
            else:
                signal = "HOLD"

            results[sym] = signal
            print(f"{sym:<6} | {close_p:<6} | {signal}")

        except Exception:
            results[sym] = "ERR"
            print(f"{sym:<6} | ERROR")
            print(traceback.format_exc())

    print("---------------------------")
    save_to_csv(date_str, results)


if __name__ == "__main__":
    scan_signals()
