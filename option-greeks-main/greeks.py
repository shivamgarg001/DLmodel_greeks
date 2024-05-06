from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import pymysql.cursors

months = {'Jan': 1,'Feb': 2,'Mar': 3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

headers = {
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US, en, q-0.9",
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36",
}

session = requests.Session()
errors = []

# ==================================================== Quering database  ====================================================
connection = pymysql.connect(
    host="localhost",
    user="root",
    password="OmGarg8700@",
    database="option_chain"
)
cur = connection.cursor()

def insert_data_options(option_name, currDate, expiryDate, currTime, price, vega, gamma, delta, IV):
    # here will be query
    query = f"insert into options(Name, option_name, currDate, expiryDate, currTime, price, vega, gamma, delta, IV) values('NIFTY 50', '{option_name}', '{currDate}', '{expiryDate}', '{currTime}', {price}, {vega}, {gamma}, {delta}, {IV})"
    try:
        cur.execute(query)
        connection.commit()
    except:
        print("Error: ", query)
        errors.append(query)
        connection.rollback()

def insert_data_options_cal(nifty_price, option_name, currDate, currTime, ce_vega, pe_vega, ce_gamma, pe_gamma, ce_delta, pe_delta):
    query = f"Insert into options_cal(Name, nifty_price, option_name, currDate, currTime, ce_vega, pe_vega, ce_gamma, pe_gamma, ce_delta, pe_delta) values('NIFTY 50', {nifty_price}, '{option_name}', '{currDate}', '{currTime}', {ce_vega}, {pe_vega}, {ce_gamma}, {pe_gamma}, {ce_delta}, {pe_delta})"
    
    try:
        cur.execute(query)
        connection.commit()
    except:
        print("Error: ", query)
        errors.append(query)
        connection.rollback()

def get_data():
    query = "Select * from options"
    output = cur.execute(query)
    print(output)
    for i in output:
        print(i)


# ========================================== calculating security current price =====================================================
def nse_secfno(symbol,attribute="lastPrice"):
    request = session.get("http://www.nseindia.com/", headers=headers, timeout=10)
    cookies = dict(request.cookies)
    positions = session.get('http://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O', headers={
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US, en, q-0.9",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36",
    }, cookies=cookies, timeout=10).json()

    if(symbol == "NIFTY"):
        return positions['marketStatus']['last']
    
    endp = len(positions['data'])
    for x in range(0, endp):
        if(positions['data'][x]['symbol']==symbol.upper()):
            return positions['data'][x][attribute]
        

# most important ============================ Calculation of greeks and updation of database ==================================        
def calculate_greeks(nifty_price, data, ce_vega, ce_gamma, ce_delta, pe_vega, pe_gamma, pe_delta, flag, test_current=""):
    S = nifty_price
    K = data['strikePrice']
    r = 0.1

    expiry = data['expiryDate'].split("-")
    year = int(expiry[2])
    month = months[expiry[1]]
    date = int(expiry[0])

    t = 0
    if(test_current != ""):
        test_current = test_current.split("-")
        t = ((datetime(year, month, date, 15, 30, 0) - datetime(int(test_current[2],), int(test_current[1]), int(test_current[0]), int(test_current[3]), int(test_current[4]), int(test_current[5])))/timedelta(days=1))/365
    else:
        t = ((datetime(year, month, date, 15, 30, 0) - datetime.now())/timedelta(days=1))/365

    if(flag == 'c'):
        price = data['CE']['lastPrice']

        IV = implied_volatility(price, S, K, t, r, flag)
        Delta =  delta(flag, S, K, t, r, IV)
        Gamma = gamma(flag, S, K, t, r, IV)
        Vega = vega(flag, S, K, t, r, IV)

        ce_delta += Delta
        ce_gamma += Gamma
        ce_vega += Vega

        insert_data_options(str(K) + ' CE', datetime.date.today(), "-".join(expiry), datetime.datetime.now().strftime('%H:%M:%S'), price, Vega, Gamma, Delta, IV)

    else:
        price = data['PE']['lastPrice']

        IV = implied_volatility(price, S, K, t, r, flag)
        Delta =  delta(flag, S, K, t, r, IV)
        Gamma = gamma(flag, S, K, t, r, IV)
        Vega = vega(flag, S, K, t, r, IV)

        pe_delta += Delta
        pe_gamma += Gamma
        pe_vega += Vega

        insert_data_options(str(K) + ' PE', datetime.date.today(), "-".join(expiry), datetime.datetime.now().strftime('%H:%M:%S'), price, Vega, Gamma, Delta, IV)

    return [ce_delta, ce_gamma, ce_vega, pe_delta, pe_gamma, pe_vega]

    # theta is not coming of actual value so we leave it for now





# -------------------------------------------------------------- main code ---------------------------------------------------------
def main_function(morning_ce_vega, morning_pe_vega, morning_ce_gamma, morning_pe_gamma, morning_ce_delta, morning_pe_delta):

    request = session.get("http://www.nseindia.com/", headers=headers, timeout=10)
    cookies = dict(request.cookies)
    data = session.get("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers, cookies=cookies, timeout=10).json()

    # calculating expiry dates needed
    expiry_dates = data["records"]["expiryDates"]
    current_expiry = expiry_dates[0]
    current_expiry_month = current_expiry.split("-")[1]
    next_expiry = expiry_dates[1]

    # current month expiry
    current_month_ex = ""
    for i in range(1, 4):
        mon = expiry_dates[i].split("-")[1]
        if(mon != current_expiry_month):
            current_month_ex = expiry_dates[i-1]
            break

    if((current_month_ex == current_expiry) or (current_month_ex == next_expiry)):
        current_month_ex = ""

    otm_calls = []
    otm_puts = []

    nifty_price = nse_secfno("NIFTY")
    print()
    print("Current Market price: ", nifty_price)
    atm_option = round((nifty_price/100))*100
    print("ATM price: ", atm_option, end="\n\n")

    for i in range(0, 31):
        otm_calls.append(atm_option + 50*i)

    for i in range(1, 31):
        otm_puts.append(atm_option - 50*i)

    print("OTM PUTS:", otm_puts)
    print("OTM CALLS:", otm_puts)

    # nifty ce 
    nifty_ce_vega = 0
    nifty_ce_gamma = 0
    nifty_ce_delta = 0

    # nifty pe
    nifty_pe_vega = 0
    nifty_pe_gamma = 0
    nifty_pe_delta = 0


    # ================================================ for current expiry ===================================================
    current_expiry_data = data['filtered']['data']
    index = -1;

    # for puts 
    for i in range(0, len(current_expiry_data)):
        if(current_expiry_data[i]['strikePrice'] == otm_puts[-1]):
            index = i;
            break

    for i in range(index, index+30):
        nifty_ce_delta, nifty_ce_gamma, nifty_ce_vega, nifty_pe_delta, nifty_pe_gamma, nifty_pe_vega = calculate_greeks(nifty_price, current_expiry_data[i], nifty_ce_vega, nifty_ce_gamma, nifty_ce_delta, nifty_pe_vega, nifty_pe_gamma, nifty_pe_delta, 'p')

    # for calls
    index += 30

    for i in range(index, index+32):
        nifty_ce_delta, nifty_ce_gamma, nifty_ce_vega, nifty_pe_delta, nifty_pe_gamma, nifty_pe_vega = calculate_greeks(nifty_price, current_expiry_data[i], nifty_ce_vega, nifty_ce_gamma, nifty_ce_delta, nifty_pe_vega, nifty_pe_gamma, nifty_pe_delta, 'c')



    # -----------------------------------------------for next_expiry and current expiry -------------------------------------------
    next_expiry_data = data['records']['data']
    index = -1
    for i in range(0, len(next_expiry_data)):
        strike = next_expiry_data[i]['strikePrice']
        expiry = next_expiry_data[i]['expiryDate']
        if(strike < otm_puts[-1]):
            continue
        elif(strike > otm_calls[-1]):
            break;

        elif(strike >= otm_puts[-1] and strike<otm_calls[0]):
            if(expiry == next_expiry):
                nifty_ce_delta, nifty_ce_gamma, nifty_ce_vega, nifty_pe_delta, nifty_pe_gamma, nifty_pe_vega = calculate_greeks(nifty_price, next_expiry_data[i], nifty_ce_vega, nifty_ce_gamma, nifty_ce_delta, nifty_pe_vega, nifty_pe_gamma, nifty_pe_delta, 'p')

            if(current_month_ex != "" and expiry == current_month_ex):
                nifty_ce_delta, nifty_ce_gamma, nifty_ce_vega, nifty_pe_delta, nifty_pe_gamma, nifty_pe_vega = calculate_greeks(nifty_price, next_expiry_data[i], nifty_ce_vega, nifty_ce_gamma, nifty_ce_delta, nifty_pe_vega, nifty_pe_gamma, nifty_pe_delta, 'p')

        elif (strike >= otm_calls[0] and strike <= otm_calls[-1]):
            if(expiry == next_expiry):
                nifty_ce_delta, nifty_ce_gamma, nifty_ce_vega, nifty_pe_delta, nifty_pe_gamma, nifty_pe_vega = calculate_greeks(nifty_price, next_expiry_data[i], nifty_ce_vega, nifty_ce_gamma, nifty_ce_delta, nifty_pe_vega, nifty_pe_gamma, nifty_pe_delta, 'c')

            if(current_month_ex != "" and expiry == current_month_ex):
                nifty_ce_delta, nifty_ce_gamma, nifty_ce_vega, nifty_pe_delta, nifty_pe_gamma, nifty_pe_vega = calculate_greeks(nifty_price, next_expiry_data[i], nifty_ce_vega, nifty_ce_gamma, nifty_ce_delta, nifty_pe_vega, nifty_pe_gamma, nifty_pe_delta, 'c')

    print("Calculated this time: ")
    print(nifty_ce_vega)
    print(nifty_pe_vega)
    print(nifty_ce_gamma)
    print(nifty_pe_gamma)
    print(nifty_ce_delta)
    print(nifty_ce_delta)

    print("Difference: ")
    print(nifty_ce_vega - morning_ce_vega)
    print(nifty_pe_vega - morning_pe_vega)
    print(nifty_ce_gamma - morning_ce_gamma)
    print(nifty_pe_gamma - morning_pe_gamma)
    print(nifty_ce_delta - morning_ce_delta)
    print(nifty_ce_delta - morning_pe_delta)

    # database call
    insert_data_options_cal(nifty_price, 'CE/PE', datetime.date.today(), datetime.datetime.now().strftime('%H:%M:%S'), nifty_ce_vega - morning_ce_vega, nifty_pe_vega - morning_pe_vega, nifty_ce_gamma - morning_ce_gamma, nifty_pe_gamma - morning_pe_gamma, nifty_ce_delta - morning_ce_delta, nifty_ce_delta - morning_pe_delta)

    return [nifty_ce_vega, nifty_pe_vega, nifty_ce_delta, nifty_pe_delta, nifty_ce_gamma, nifty_pe_gamma]


# =========================================at last we have to take difference from 9:15 vega gamma and delta =======================

# calculate_greeks(nifty_price, 9.70, 19650, "27-Jul-2023")

# IV = implied_volatility(111, nifty_price, 19650, ((datetime(2023, 8, 3, 15, 30, 0) - datetime.now())/timedelta(days=1))/365, 0.1, 'c')
# print(vega('c', nifty_price, 19650, ((datetime(2023, 8, 3, 15, 30, 0) - datetime.now())/timedelta(days=1))/365, 0.1, IV))


# =====================================  main calling function that should be called at 9:15 ===================================

morning_ce_vega, morning_pe_vega, morning_ce_gamma, morning_pe_gamma, morning_ce_delta, morning_pe_delta = 0
def calling_function():
    current_time = datetime.datetime.now()
    prev_minute = 15
    while((current_time.hour >= 9) and current_time.hour <= 15):
        if((current_time.hour == 9 and current_time.minute >= 15) or (current_time.hour == 15 and current_time.minute <= 30) or (current_time.hour > 9 and current_time.hour < 15)):
            
            if(prev_minute == current_time.minute):
                # ===========================  main function call with database call  =========================================
                # 9:15 call
                if(current_time.hour == 9 and current_time.minute == 15):
                    morning_ce_vega, morning_pe_vega, morning_ce_gamma, morning_pe_gamma, morning_ce_delta, morning_pe_delta = main_function(morning_ce_vega, morning_pe_vega, morning_ce_gamma, morning_pe_gamma, morning_ce_delta, morning_pe_delta)

                # whole day calls
                else:
                    main_function(morning_ce_vega, morning_pe_vega, morning_ce_gamma, morning_pe_gamma, morning_ce_delta, morning_pe_delta)

                if(prev_minute == 55): 
                    prev_minute = 0         # this should be start of program or start of markets
                else:
                    prev_minute += 5

            else:
                continue    
        else:
            return

        current_time = datetime.datetime.now()


    print("Day has Ended Succesfully")
    for i in errors:
        print(i)
