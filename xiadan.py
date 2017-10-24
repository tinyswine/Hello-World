# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 09:07:12 2017

@author: luowen
"""

from tools import *
import datetime as dt
import multiprocessing
from order import *
import warnings
warnings.filterwarnings("ignore")
from stop_tradingday import stop_tradingday


if __name__ == '__main__':
    
    print "=============================================================="
    print "start "
    print dt.datetime.now()
    print "=============================================================="
    
    
    stop_tradingday()
    delete_swap_alert()
    con_monitor = create_engine('mysql://{0}:{1}@{2}/monitor'.format(user,password,ip))
    delete_swap_alert_sql(con_monitor)
    check_data()


    #####copy_policy_from_aliyun()
        
    

   
    now_time = int(time.strftime('%H%M',time.localtime(time.time())))
    if now_time>0 and now_time<=1130 :
        time_start = "09:00:17"
        time_end = "09:07:00"
        if now_time > 900:
            time_start =  (dt.datetime.now() + dt.timedelta(seconds = 30)).strftime("%H:%M:%S")
            time_end = (dt.datetime.now() + dt.timedelta(seconds = 150)).strftime("%H:%M:%S")
        print("900")
    elif now_time>1130 and now_time<=1500 :
        time_start = "13:30:17"
        time_end = "13:37:00"
        if now_time > 1330:
            time_start =  (dt.datetime.now() + dt.timedelta(seconds = 30)).strftime("%H:%M:%S")
            time_end = (dt.datetime.now() + dt.timedelta(seconds = 150)).strftime("%H:%M:%S")
        print("1300")
    elif now_time>1500 and now_time<=2400 :
        time_start = "21:00:17"
        time_end = "21:07:00"
        if now_time > 2100:
            time_start =  (dt.datetime.now() + dt.timedelta(seconds = 30)).strftime("%H:%M:%S")
            time_end = (dt.datetime.now() + dt.timedelta(seconds = 150)).strftime("%H:%M:%S")
        print("2100")

#    for account in tradeList:
#        prepare_order(time_start, time_end, account)
        
    def mulityOrder(accounts):
        
        try:
            prepare_order(time_start, time_end, accounts, policy_info, money_total, money_pos_percent)
        except Exception as e:
            print(e)
            print accounts,'Error'   
            send_error(accounts, str(e))
            
    def mulityOrder_nonight(accounts):
        
        try:
            prepare_order(time_start, time_end, accounts, policy_info_nonight, money_total_nonight, money_pos_percent_nonight)
        except Exception as e:
            print(e)
            print accounts,'Error'   
            send_error(accounts, str(e))
            
    def mulityOrder_arbitrage(accounts):
        
        try:
            prepare_order(time_start, time_end, accounts, policy_info_arbitrage, money_total_arbitrage, money_pos_percent_arbitrage)
        except Exception as e:
            print(e)
            print accounts,'Error'   
            send_error(accounts, str(e))
            
    def mulityOrder_trend(accounts):
        
        try:
            prepare_order(time_start, time_end, accounts, policy_info_trend, money_total_trend, money_pos_percent_trend)
        except Exception as e:
            print(e)
            print accounts,'Error'   
            send_error(accounts, str(e))
            
    def mulityOrder_mix(accounts):
        
        try:
            prepare_order(time_start, time_end, accounts, policy_info_mix, money_total_mix, money_pos_percent_mix)
        except Exception as e:
            print(e)
            print accounts,'Error'   
            send_error(accounts, str(e))
            
    tradeList = night_accountlist    
    process_cnt = len(tradeList)
#    print(shishi_accountlist)
#    mulityOrder(tradeList[7])
#    assert(False)

    pool = multiprocessing.Pool(process_cnt)
    pool.map(mulityOrder,tradeList)
    pool.close()
    pool.join()
    
    tradeList = nonight_accountlist   
    process_cnt = len(tradeList)
#    mulityOrder_nonight(tradeList[0])
#    assert(False)
    if process_cnt > 0:
        pool = multiprocessing.Pool(process_cnt)
        pool.map(mulityOrder_nonight,tradeList)
        pool.close()
        pool.join()
        
    tradeList = arbitrage_accountlist   
    process_cnt = len(tradeList)
#    mulityOrder_nonight(tradeList[0])
#    assert(False)
    if process_cnt > 0:
        pool = multiprocessing.Pool(process_cnt)
        pool.map(mulityOrder_arbitrage,tradeList)
        pool.close()
        pool.join()
        
    tradeList = trend_accountlist   
    process_cnt = len(tradeList)
#    mulityOrder_nonight(tradeList[0])
#    assert(False)
    if process_cnt > 0:
        pool = multiprocessing.Pool(process_cnt)
        pool.map(mulityOrder_trend,tradeList)
        pool.close()
        pool.join()
        
    tradeList = mix_accountlist   
    process_cnt = len(tradeList)
#    mulityOrder_nonight(tradeList[0])
#    assert(False)
    if process_cnt > 0:
        pool = multiprocessing.Pool(process_cnt)
        pool.map(mulityOrder_mix,tradeList)
        pool.close()
        pool.join()

    print('=============done============')
