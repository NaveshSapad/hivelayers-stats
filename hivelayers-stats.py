#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from hiveengine.market import Market
from hiveengine.api import Api
import time
from datetime import datetime as dt
import streamlit as st
from beem import Hive
from beem import Steem
from beem.transactionbuilder import TransactionBuilder
from beembase import operations
from beem.instance import set_shared_steem_instance
from hiveengine.wallet import Wallet
from hiveengine.api import Api
import requests
import pymssql
import json
from datetime import timedelta
import os
import altair as alt
import matplotlib.pyplot as plt
import datetime
from PIL import Image


def get_history(user,token): 
    end=0
    x=0
    s=[]
    
    
    
    while(end!=1):
        time.sleep(0.02)
        res=api.get_history(user,token,offset=x)
        s.append(res)

        x=x+len(res)
        if(len(res)<500):
            end=1

    listfinal=[]
    for i in range(0,len(s)):
        for j in range(0,len(s[i])):
            listfinal.append(s[i][j])
            
    
    
    return(listfinal)

def get_buy_sell_history(listfinal,token):
    buy_list=[]
    sell_list=[]
    
    for i in range(0,len(listfinal)):
        if(listfinal[i]['operation']=='market_buy' or listfinal[i]['operation']=='market_sell'):
            if(listfinal[i]['operation']=='market_buy'):
                buy_list.append([listfinal[i]['quantityTokens'],str((float(listfinal[i]['quantitySteem'])/float(listfinal[i]['quantityTokens']))),time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(listfinal[i]['timestamp']))])
            elif(listfinal[i]['operation']=='market_sell'):
                sell_list.append([listfinal[i]['quantityTokens'],str((float(listfinal[i]['quantitySteem'])/float(listfinal[i]['quantityTokens']))),time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(listfinal[i]['timestamp']))])
    
    
    return(buy_list,sell_list)

def get_transfer_history(user,token,listfinal):
    add_q=0
    sub_q=0
    add_list=[]
    sub_list=[]
    
    for i in range(0,len(listfinal)):
        if(listfinal[i]['operation']=='tokens_stake'):
            if(listfinal[i]['from']!=user):
                add_q += float(listfinal[i]['quantity'])
                add_list.append([listfinal[i]['from'],listfinal[i]['quantity'],time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(listfinal[i]['timestamp']))])
            else:
                if(listfinal[i]['to']!=user):
                    sub_q += float(listfinal[i]['quantity'])
                    sub_list.append([listfinal[i]['to'],listfinal[i]['quantity'],time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(listfinal[i]['timestamp']))])
            
        if(listfinal[i]['operation']=='tokens_transfer' or listfinal[i]['operation']=='tokens_issue'):
            if(listfinal[i]['from']!=user):
                add_q += float(listfinal[i]['quantity'])
                add_list.append([listfinal[i]['from'],listfinal[i]['quantity'],time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(listfinal[i]['timestamp']))])
            else:
                sub_q += float(listfinal[i]['quantity'])
                sub_list.append([listfinal[i]['to'],listfinal[i]['quantity'],time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(listfinal[i]['timestamp']))])
                
    
                
    return(add_q,sub_q,add_list,sub_list)

def calculate_current_avg(listfinal,buy_list,sell_list,user,token,buy_avg,sell_avg,what_you_got,what_you_sent,first,buy_history,second,sell_history,third,receive,fourth,send):
    
    if not buy_list:
        df_buy = pd.DataFrame(columns = ['Quantity','Price','Date'])
        
    else:
        df_buy=pd.DataFrame(buy_list)
        
    if not sell_list:
        df_sell = pd.DataFrame(columns = ['Quantity','Price','Date'])
        
    else:
        df_sell=pd.DataFrame(sell_list)

    df_buy.columns=['Quantity','Price','Date']
    df_sell.columns=['Quantity','Price','Date']

    df_buy['Quantity']=pd.to_numeric(df_buy['Quantity'])
    df_buy['Price']=pd.to_numeric(df_buy['Price'])
    

    df_sell['Quantity']=pd.to_numeric(df_sell['Quantity'])
    df_sell['Price']=pd.to_numeric(df_sell['Price'])

    df_buy['Hive_paid_total']=df_buy['Quantity']*df_buy['Price']
    df_sell['Hive_got_total']=df_sell['Quantity']*df_sell['Price']

    buy_total=df_buy['Hive_paid_total'].sum()
    buy_q=df_buy['Quantity'].sum()
    avg_buy=buy_total/buy_q
    
    if first.checkbox("Click to see your buy_history"):
        buy_history.table(df_buy)
    
    if second.checkbox("Click to see your sell_history"):
        sell_history.table(df_sell)

    print('\nAverage price (BUY):'+str(avg_buy)+' Amount:'+str(buy_q))


    sell_total=df_sell['Hive_got_total'].sum()
    sell_q=df_sell['Quantity'].sum()
    avg_sell=sell_total/sell_q


    buy_avg.write("<p class='output'>You have bought <b>{}</b> {} totally - Average buy price : {}</p>".format(buy_q,token,avg_buy),unsafe_allow_html=True)
    sell_avg.write("<p class='output'>You have sold <b>{}</b> {} totally - Average sell price : {}</p>".format(sell_q,token,avg_sell),unsafe_allow_html=True)
    
    print('\nAverage price (SELL):'+str(avg_sell)+' Amount:'+str(sell_q))
    
    
    add_q,sub_q,add_list,sub_list=get_transfer_history(user,token,listfinal)
    

    what_you_got.write("<p class='output'>You have received <b>{}</b> {} from others </p>".format(add_q,token),unsafe_allow_html=True)
    what_you_sent.write("<p class='output'>You have sent <b>{}</b> {} to others </p>".format(sub_q,token),unsafe_allow_html=True)
    
    print("\nSent:"+str(sub_q),"Received:"+str(add_q))
    
    profit=buy_total-sell_total
    remaining_amount=buy_q+add_q-sell_q-sub_q
    current_avg= (profit)/(remaining_amount)
    
    

    if not add_list:
        df_receive= pd.DataFrame(columns=['From','Quantity','Date'])
        
    else:
        df_receive= pd.DataFrame(add_list)
        df_receive.columns=['From','Quantity','Date']

    if not sub_list:
        df_send= pd.DataFrame(columns=['To','Quantity','Date'])
        

    else:   
        df_send=pd.DataFrame(sub_list)
        df_send.columns=['To','Quantity','Date']
    
    if third.checkbox("Click to see your received_history"):
        receive.table(df_receive)
    
    if fourth.checkbox("Click to see your send_history"):
        send.table(df_send)
    
    
    return(remaining_amount,current_avg,profit)

def get_sym_list():
    market= Market()
    market_details=market.get_metrics()
    symbols_list=[]
    for i in range(0,len(market_details)):
        symbols_list.append(market_details[i]['symbol'])

    symbols_list.sort()
        
    return symbols_list

def hivebreakeven():
    api=Api()
    
    st.markdown("<h1 id='title'><center> Enter your username and the token you wish to see the details for and click enter </center></h1>",unsafe_allow_html=True)

    st.markdown('''
    <style>
            #title {color:#ffffff}
            body {background-color: #000;
            }
            .output {padding:10px;
                text-align:center;}

            .summary {padding:10px}
            .css-8gj76c{border-radius:10px;
                                background-color: #ffffff;
                        }
            .css-1a7pngp{border-radius:10px;
                                background-color: #ffffff;
                                padding:20px;
                        }
            
    </style>
    ''',unsafe_allow_html=True)
    symbols_list= get_sym_list()
    
    entry,output = st.beta_columns([1,3])

    
    user=entry.text_input("Enter the username:")
    token=entry.selectbox("Enter the token:",symbols_list)
    user=user.lower()
    token=token.upper()
    
    if user:
        if token:
            progress_details= entry.empty()
            progress_bar= entry.progress(0)
            
            output.markdown("<h1><center> {} stats </center></h1>".format(token),unsafe_allow_html=True)

            buy_avg=output.empty()
            sell_avg=output.empty()
            what_you_got=output.empty()
            what_you_sent=output.empty()

            output.write('<hr>',unsafe_allow_html=True)

            first=entry.empty()
            buy_history = entry.empty()
            second=entry.empty()
            sell_history = entry.empty()
            third=entry.empty()
            receive = entry.empty()
            fourth=entry.empty()
            send = entry.empty()



            listfinal=get_history(user,token)

            buy_list,sell_list=get_buy_sell_history(listfinal,token)

            current_holdings,current_avg,profit=calculate_current_avg(listfinal,buy_list,sell_list,user,token,buy_avg,sell_avg,what_you_got,what_you_sent,first,buy_history,second,sell_history,third,receive,fourth,send)

            

            output.markdown("<h1> <center>Summary </center></h1><h3 class = 'summary'>Current Holdings is : {} {}</center></h3>".format(current_holdings,token),unsafe_allow_html=True)


            if(current_avg>0):
                current_avg= abs(current_avg)
                output.markdown("<h3 class='summary'>Current_avg( HIVE ): {} HIVE per {} </h3><h6 class='summary'>Formula used: (Buy_total - sell_total) / (Buy_quantity+ received_amount - sell_quantity - sent_amount)</h6><h3 class='summary'>That means you can sell the {} at any price greater than {} HIVE to make profits .If you sell for less , you will make loss.</center></h3>".format('%.8f' % current_avg,token,token,'%.8f' % current_avg),unsafe_allow_html=True) 

            elif(current_avg!=0 and profit!=0):
                output.markdown("<h3 class='summary'>Cool , you are on profits already .Total profits so far: {} HIVE.Don't forget you still hold {} amount of {} token</center></h3>".format(str(abs(profit)),"%.6f" % current_holdings,token),unsafe_allow_html=True)
            else:
                output.markdown("<h3 class='summary'>No profit , no loss</center></h3>",unsafe_allow_html=True)
                
            
        else:
            entry.write("Please enter token")
            
    else:
        entry.write("Please enter both Username and token")
        

def establish_conn(uid,pwd):
    conn = pymssql.connect(server='vip.hivesql.io', user=uid, password=pwd, database='DBHive')
    
    return conn

def hivecommunity():
    
    uid = os.environ['hiveuid']
    pwd = os.environ['hivepwd']

    conn=establish_conn(uid,pwd)

    st.markdown('''<style>
    .css-z8kais{align-items:center;}
    .css-1bjf9av .css-106gl43 {border:3px black solid;}

    .css-1f5jof3{margin-left:150px;}
    
    </style>    ''',unsafe_allow_html=True)

    user=st.sidebar.text_input("Enter your Hive username")
    user=user.lower()
    title=st.empty()

    
    
    
    title.markdown("<h1><center> Enter your Hive username in the sidebar to get your data ( left) </center></h1>",unsafe_allow_html=True)
    if user:
       
        title.empty()
        
        
        
        user_pc = pd.read_sql_query('''select * from Comments where author= '{}' and created > GETDATE()-31  ORDER BY ID DESC '''.format(user),conn)

        
        
        post_c=0
        comment_c=0
        save_list=[]
        for i in range(0,len(user_pc)):
            try:
                json_metadata=json.loads(user_pc['json_metadata'][i])
                if 'app' in json_metadata:
                    save_list.append([user_pc['author'][i],user_pc['parent_author'][i],json_metadata['app'],user_pc['created'][i].date()])
                    
                    if(user_pc['parent_author'][i]==''):
                        post_c += 1
                    else:
                        comment_c += 1
                        
            except:
                print("Yes")
                pass

        
        
            
        st.markdown("<h3><center> Your total post count for past 30 days: {}, Your total comment count for past 30 days: {}</center></h3><hr>".format(post_c,comment_c),unsafe_allow_html=True)
            
        df_pc = pd.DataFrame(save_list,columns=['Author','Parent_author','Frontend','Date'])

        

        df_pc['Type']=str
        for i in range(0,len(df_pc)):
            if df_pc['Parent_author'][i]=='':
                df_pc['Type'][i]='Post'
            else:
                df_pc['Type'][i]='Comment'

        df_today_pc = df_pc[df_pc['Date']==dt.utcnow().date()]
        df_today_pie= df_today_pc.groupby('Frontend').count() 

        df_p=df_pc[df_pc['Parent_author']=='']
        df_c=df_pc[df_pc['Parent_author']!='']
        
        df_today_p= df_p[df_p['Date']==dt.utcnow().date()]
        df_today_frontends_p=df_today_p.groupby('Frontend').count()

        df_today_c= df_c[df_c['Date']==dt.utcnow().date()]
        df_today_frontends_c=df_today_c.groupby('Frontend').count()

        df_today_frontends_p.rename(columns={'Author':'Post_count'},inplace=True)
        df_today_frontends_c.rename(columns={'Author':'Comment_count'},inplace=True)
        
        

        base=alt.Chart(df_pc)
        
        
        
        st.markdown("<h1><center> Post + Comment count Date wise </center></h1>",unsafe_allow_html=True)
        
        b = base.mark_bar(size=20,point=True).encode(x='Date', y='count(Author)',color='Type',tooltip=['count(Type)','Type','Date']).configure_axis(
        labelFontSize=20,
        titleFontSize=20
    ).properties(width=1000,height=700)

        st.write(b)

        base1=alt.Chart(df_pc)

        st.markdown("<hr><h1> <center> Frontend Data </center> </h1>",unsafe_allow_html=True)

        c= base1.mark_bar(size=20,point=True).encode(x='Date',y='count(Author)',color='Frontend',tooltip=['count(Frontend)','Frontend']).configure_axis(
        labelFontSize=20,
        titleFontSize=20
    ).properties(width=1000,height=700)

        st.write(c)

        st.markdown("<h1><center> Today's data </center></h1>",unsafe_allow_html=True)

        left_today,right_today= st.beta_columns([1,1])

        left_today.write("<h3><center> Posts </center></h3>",unsafe_allow_html=True)

        right_today.write("<h3> <center> Comments </center> </h3>",unsafe_allow_html=True)

        

        left_today.table(df_today_frontends_p['Post_count'])
        right_today.table(df_today_frontends_c['Comment_count'])

        

    
    st.markdown("<hr><h1> <center> Engagement Program </center> </h1>",unsafe_allow_html=True)

    d = st.date_input(
         "Choose the date to display the data",
         datetime.date(2021, 4, 11),
         min_value=datetime.date(2021, 2, 15),
         max_value=datetime.date(2021, 4, 11))

    st.write('Selected Date:', str(d))

    engage_l=st.empty()
    engage_leo=st.empty()
    engage_c=st.empty()
    engage_ctp=st.empty()
    engage_st=st.empty()
    engage_stem=st.empty()
    engage_sp=st.empty()
    engage_sports=st.empty()
    engage_wd=st.empty()
    engage_weed=st.empty()
    engage_pb=st.empty()
    engage_pob=st.empty()


    if d:
        file_name='Images/Images_{}'.format(str(d))

        engage_l.markdown("<h3> LeoFinance Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_leo.image(file_name+'/leo.png')
        
        engage_c.markdown("<h3>Ctptalk Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_ctp.image(file_name+'/ctp.png')
        
        engage_st.markdown("<hr><h3> STEMGeeks Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_stem.image(file_name+'/stem.png')
        
        engage_sp.markdown("<hr><h3> Sportstalksocial Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_sports.image(file_name+'/sports.png')



        if(str(d)>'2021-03-21'):
            engage_wd.markdown("<hr><h3> WEED Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
            engage_weed.image(file_name+'/weed.png')
        
            engage_pb.markdown("<hr><h3> ProofOfBrain Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
            engage_pob.image(file_name+'/pob.png')

    


def load_csv(token):
    if(token=='BRO'): # Use else when you add more tokens.
        df=pd.read_csv('csv/bro_payouts.csv')
    elif(token=='INDEX'):
        df=pd.read_csv('csv/index_payouts.csv')
    elif(token=='DHEDGE'):
        df=pd.read_csv('csv/dhedge_payouts.csv')
    elif(token=='EDS'):
        df=pd.read_csv('csv/EDS_payouts.csv')
    elif(token=='SPI'):
        df=pd.read_csv('csv/spi_payouts.csv')
    elif(token=='TAN'):
        df=pd.read_csv('csv/tan_payouts.csv')
    elif(token=='UTOPIS'):
        df=pd.read_csv('csv/UTOPIS_payouts.csv')

    
    sym_list=list(set(df['symbol'])) # Take unique symbols
    all_list=[]
    if token!='EDS' and token!='SPI' and  token!='UTOPIS':
        all_list=['All']
    for i in sym_list:
        all_list.append(i) # For Token selection in sidebar

    
    return df,all_list,sym_list


def load_image(token):
    if(token=='BRO'):
        image = Image.open('logos/bro.png') # Get Bro image
    elif(token=='INDEX'):
        image = Image.open('logos/index.png')# Get INDEX image
    elif(token=='DHEDGE'):
        image = Image.open('logos/dhedge.png') # Get DHEDGE image
    elif(token=='EDS'):
        image = Image.open('logos/EDS.png') # Get EDS image
    elif(token=='SPI'):
        image = Image.open('logos/EDS.png') # Same Image for both EDS and SPI
    elif(token=='TAN'):
        image = Image.open('logos/tan.png') # Change this
    elif(token=='UTOPIS'):
        image = Image.open('logos/utopis.png')
    
    
    return image

def load_user_details(df,hive_user,token):
    

    sum_hive_yesterday=0 # To calculate yesterday's hive payout

    df_user_details=df[df['to']==hive_user] # This loads only specific user details 
    df_user_details['quantity']=pd.to_numeric(df_user_details['quantity']) # Converting to float.

    date_count=len(set(df_user_details['date']))

    
    if not df_user_details.empty:
        
        df_last_date=df_user_details[df_user_details['date']==max(df_user_details['date'])]
        
        df_last_date.reset_index(inplace=True)
        

        if token!='EDS' and token!='UTOPIS':
            for i in range(0,len(df_last_date)):
                sum_hive_yesterday += get_token_price(df_last_date['symbol'][i])*df_last_date['quantity'][i]
        else:
            sum_hive_yesterday = df_last_date['quantity'][0]

        if st.checkbox("Click here to see most recent date payout "):
            st.table(df_last_date)
    
    
            



    if df_user_details.empty:
        return df_user_details,0,date_count,sum_hive_yesterday
    else:
        return df_user_details,1,date_count,sum_hive_yesterday

def get_balance(hive_user,token):
    wallet=Wallet(hive_user)

    list_balances=wallet.get_balances()
    for i in range(0,len(list_balances)):
        if(list_balances[i]['symbol']==token):
            return(float(list_balances[i]['balance'])+float(list_balances[i]['stake']))
    return(0)

def get_chart(df_user_details,token,sym_list,sym):
    total_hive=0
    #my_bar.progress(20)
    total=0
    

    if(token):
        if sym!='All':  # Then a particular symbol is selected in the selectbox .
            st.markdown('<hr>',unsafe_allow_html=True)

            
            df_sym=df_user_details[df_user_details['symbol']==sym] # Retreive that particular symbol details . 
            sum_sym=df_sym['quantity'].sum() # Add it .


            if st.checkbox('Show table: Last 10 days '+sym+' payout'):
                st.table(df_sym.head(10))

            market=Market() # Market instance
            list_metrics=market.get_metrics() # Returns all the tokens in HE with details

            if sym!='SWAP.HIVE':
                for i in list_metrics:
                    if(i['symbol']==sym): # Selecting only the symbol we want
                        total=(float(i['lastPrice'])* sum_sym) # Taking last price and multiplying with total token earned which is = sum_sym

            if sym=='HIVE' or sym=='SWAP.HIVE':
                total=sum_sym

            total_hive=total
                                
            st.write('<div class="card"><div class="card-header"><center>Total '+sym+' from Jan 1  : '+ '%.6f' % sum_sym+' '+sym+' , In HIVE = '+'%.6f' % total +'.</center>',unsafe_allow_html=True)
            
            if sum_sym>0:
                c = alt.Chart(df_sym).mark_line(point=True).encode(x='date', y='quantity',color='symbol',tooltip=['quantity']).properties(width=1400,height=500) 
                st.write(c)

            #my_bar.progress(50)

            return total_hive

        
        
        else: # Selected ALL
            n=0
            for sym in sym_list:
                n=n+1
                #my_bar.progress((n/len(sym_list)))
                
                st.markdown('<hr>',unsafe_allow_html=True)
                st.header(sym)
                df_sym=df_user_details[df_user_details['symbol']==sym]
                sum_sym=df_sym['quantity'].sum()

                if st.checkbox('Show table: Last 10 days '+sym+' payout'):
                    st.table(df_sym.head(10))


                market=Market() # Market instance
                list_metrics=market.get_metrics() # Returns all the tokens in HE with details

                if sym!='SWAP.HIVE':
                    for i in list_metrics:
                        if(i['symbol']==sym): # Selecting only the symbol we want
                            total=(float(i['lastPrice'])* sum_sym) # Taking last price and multiplying with total token earned which is = sum_sym

                if sym=='HIVE' or sym=='SWAP.HIVE':
                    total=sum_sym
                    
                total_hive=total_hive+total
                
                
            
                st.write('<div class="card"><div class="card-header"><center>Total '+sym+' from Jan 1 : '+'%.6f' % sum_sym+' '+sym+' , In HIVE = '+'%.6f' %total+'.</center>',unsafe_allow_html=True)
            
                if sum_sym>0:
                    c = alt.Chart(df_sym).mark_line(point=True).encode(x='date', y='quantity',color='symbol',tooltip=['quantity']).properties(width=1400,height=500)                
                    st.write(c)

            
            return total_hive


def get_token_price(token):
    market=Market() # Market instance
    list_metrics=market.get_metrics() # Returns all the tokens in HE with details
    for i in list_metrics:
        if(i['symbol']==token): # Selecting only the symbol we want
            return(float(i['lastPrice']))

    return(0)
    
    

def hivetoken():
    
    st.markdown('''
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.16.0/umd/popper.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
    ''',unsafe_allow_html=True)

    

    

    st_hive_username=st.sidebar.empty() # Username - Empty
    st_select_token=st.sidebar.empty() # Token selection - Empty
    st_select_symbol=st.sidebar.empty() # Symbol - Empty
    st_image=st.sidebar.empty() # Image - Empty
    st_proc=st.sidebar.empty() 
    #st_progress=st.sidebar.empty()
 

    hive_user=st_hive_username.text_input('Enter your Hive username','amr008')
    hive_user=hive_user.lower()
    token=st_select_token.selectbox('Select the token you wish to see dividends for',['BRO','INDEX','DHEDGE','EDS','SPI','UTOPIS','TAN'])
    
    if token:
        start=dt.now()
        #st_progress.progress(10)
        df,all_list,sym_list = load_csv(token)

        
        image=load_image(token)
        st_image.image(image,use_column_width=True)

    if hive_user:
        df_user_details,n,date_count,sum_hive=load_user_details(df,hive_user,token)
        st_proc.write("Data Loaded")
        #st_progress.progress(100)


    sym = st_select_symbol.selectbox('Select SYMBOL',all_list)

    balance=get_balance(hive_user,token)

    if sym:
        if n==1:       
            st.title('@'+hive_user+' payouts: '+token)
            st.subheader('Your current balance: '+str(balance)+' '+token)
            st.markdown('''
            <h5>Buy more {} token here - <a href='https://hive-engine.com/?p=market&t={}'>H-E Market</a></h5>
            '''.format(token,token),unsafe_allow_html=True)

            st_total_hive=st.empty()

            st_display_progress=st.empty()

            my_bar = st_total_hive.progress(0)

            my_bar.progress(10)
            st_display_progress.write("Please wait while the all the payouts details loads completely to calculate final HIVE")
            

            total_hive=get_chart(df_user_details,token,sym_list,sym)

            my_bar.progress(100)

            st_display_progress.empty()
            
            per_day_average= total_hive/date_count

            
            current_token_price= get_token_price(token)


            if token!='EDS' and token!='SPI' and token!='UTOPIS':
                APR = (((sum_hive) * 365) / (float(balance) * current_token_price )*100)
            else:
                APR = (((sum_hive) * 52) / (float(balance) * current_token_price )*100)
                APR1=  (((sum_hive) * 52) / (float(balance) * 1 )*100)
           
            if token=='TAN':
                st_total_hive.markdown('<hr><hr><h3>Total Hive from {} token from Jan 1  is: {} HIVE<br> <hr> Per week average(Hive) for the above period from {} token= {} HIVE.<br><hr>Most recent payout ( in Hive ) ={} Hive <br><hr> APR (based on most recent payout + Recent price of {}):{} % (Consider this only if today is not thursday)</h3>'.format(sym,'%.5f' % total_hive,sym,'%.4f' %per_day_average,"%.5f"%sum_hive,token,"%.2f"%APR),unsafe_allow_html=True)
            elif token=='SPI':
                st_total_hive.markdown('<hr><hr><h3>Total Hive from {} token from Jan 1  is: {} HIVE<br> <hr> Per week average(Hive) for the above period from {} token= {} HIVE.<br><hr>Most recent payout ( in Hive ) ={} Hive <br><hr> APR (based on most recent payout + Recent price of {}):{} % </h3>'.format(sym,'%.5f' % total_hive,sym,'%.4f' %per_day_average,"%.5f"%sum_hive,token,"%.2f"%APR),unsafe_allow_html=True)
            elif token!='EDS' and token!='UTOPIS':
                st_total_hive.markdown('<hr><hr><h3>Total Hive from {} token from Jan 1  is: {} HIVE<br> <hr> Per day average(Hive) for the above period from {} token= {} HIVE.<br><hr>Yesterdays payout ( in Hive ) ={} Hive <br><hr> APR (based on most recent payout + Recent price of {}):{} % </h3>'.format(sym,'%.5f' % total_hive,sym,'%.4f' %per_day_average,"%.5f"%sum_hive,token,"%.2f"%APR),unsafe_allow_html=True)
            elif token=='EDS':
                st_total_hive.markdown('<hr><hr><h3>Total Hive from EDS to your account is: {} HIVE<br><br><hr>Most recent payout ( in Hive ) ={} Hive <br><hr> APR (based on most recent payout + Recent price of {}):{}% <br><hr> Since most of the users bought EDS at 1 HIVE - APR ( based on EDS price as 1 HIVE ) = {}% </h3>'.format('%.5f' % total_hive,sum_hive,token,"%.2f"%APR,"%.2f"%APR1),unsafe_allow_html=True)
            elif token=='UTOPIS':
                st_total_hive.markdown('<hr><hr><h3>Total Hive from {} to your account is: {} HIVE<br><br><hr>Most recent payout ( in Hive ) ={} Hive <br><hr> APR (based on most recent payout + Recent price of {}):{}% <br> </h3>'.format(token,'%.5f' % total_hive,'%.3f' % sum_hive,token,"%.2f"%APR),unsafe_allow_html=True)
                #st_total_hive.markdown('<hr><hr><h3>Total Hive from {} to your account is :{} HIVE <br><hr>'.format(token,'%.5f' % total_hive),unsafe_allow_html=True)
            
            
        else:
            st.title('@'+hive_user+' has no payouts from '+token+' to display')
            st.markdown('''
            <h5>Buy your first {} token here - <a href='https://hive-engine.com/?p=market&t={}'>H-E Market</a></h5>
            '''.format(token,token),unsafe_allow_html=True)
      

       
def hiveauthorrewards():
    import pandas as pd
    from datetime import datetime as dt
    import requests
    import json
    from hiveengine.market import Market
    from hiveengine.api import Api
    import time
    from datetime import timedelta
    import re
    
    #st.write("1 = Only a particular URL details ")
    #st.write("2 = All pending posts ( not comments )")
    #print("3 = All pending posts and comments")
    
    #print("Note: Option 3 might take lot of time depending on your engagement - 500 posts+comments takes approximately 25 minutes")
    
    choice= st.selectbox("Choose 1 or 2",['1 = Only a particular URL details','2 = All pending posts ( not comments )'])
    if choice=='1 = Only a particular URL details':
        select_choice=1
    else:
        select_choice=2
    #select_choice=int(select_choice)
    start=dt.now()

    
    if select_choice==1:
        url=st.text_input("Enter URL of your post: ")
        if st.button("Get my data"):
            if url!='':
                flag_current=0

                new_url=url.split('@')[1]
                permlink='@'+new_url
                st.write('https://peakd.com/{}'.format(permlink))
                my_post_progress = st.progress(0)

                res = requests.get('https://scot-api.hive-engine.com/{}?hive=1'.format(permlink))
                json_r=res.json()

                if not json_r:
                    flag_current=2


                def get_token_price(token):
                    market=Market() # Market instance
                    list_metrics=market.get_metrics() # Returns all the tokens in HE with details
                    for i in list_metrics:
                        if(i['symbol']==token): # Selecting only the symbol we want
                            return(float(i['lastPrice']))

                    return(0)

                tokens=json_r.keys()
                all_balance_list=[]
                n=str(1)

                hive_total=0
                len_token=len(tokens)


                for keys in tokens:
                    my_post_progress.progress(1/len_token)
                    len_token -= 1
                    quantity=json_r[keys]['pending_token']
                    str_precision=json_r[keys]['precision']
                    precision=int((n.ljust(str_precision+1,'0')))
                    quantity=float(quantity)/precision

                    if(json_r[keys]['cashout_time']>str(dt.utcnow())):
                        flag_current=1
                        
                    

                    hive_price=quantity * get_token_price(keys)
                    hive_price=round(hive_price,3)

                    all_balance_list.append([keys,round(quantity,4),hive_price])

                    hive_total += hive_price



                my_post_progress.progress(100)
                my_post_progress.empty()

                if(flag_current==2):
                    print("Enter valid URL")

                elif(flag_current==1):

                    number_votes=len(json_r[keys]['active_votes'])

                    st.write("Number of votes on your post = {}".format(number_votes))


                    df_tokens= pd.DataFrame(all_balance_list,columns=['Token','Amount','Amount in Hive'])

                    st.table(df_tokens)

                    st.write("Total Hive when all the tokens are converted to Hive = {}".format(round(hive_total,3)))
                    st.write("Note : This includes both author and curation rewards ")
                else:
                    st.write("This post has already been paid out <br><br> Please enter pending posts/comments to get the data ")


           
    elif select_choice==2:
             
            uid = os.environ['hiveuid']
            pwd = os.environ['hivepwd']
        
            conn=establish_conn(uid,pwd)
            
            user=st.text_input("Enter your username: ")

            if st.button("Get my data"):
                if user!='':
                    permlink_column = pd.read_sql_query('''select permlink from Comments where author='{}' and  parent_author='' and created > GETDATE()-7 ORDER BY ID DESC '''.format(user),conn)
                    
                    permlink_list=[]
                    for permlink in permlink_column['permlink'].to_list():
                        permlink_list.append('@'+user+'/'+permlink)
                        #print('@'+user+'/'+permlink)
                    
                    sum_tokens={}
                    only_hive={}
                    my_post_progress = st.progress(0)
                    perm_left=len(permlink_list)
                    for permlink in permlink_list:
                        st.write('https://peakd.com/{}'.format(permlink))
                        my_post_progress.progress(1/perm_left)
                        perm_left -= 1

                        res = requests.get('https://scot-api.hive-engine.com/{}?hive=1'.format(permlink))
                        json_r=res.json()

                        if not json_r:
                            flag_current=2


                        def get_token_price(token):
                            market=Market() # Market instance
                            list_metrics=market.get_metrics() # Returns all the tokens in HE with details
                            for i in list_metrics:
                                if(i['symbol']==token): # Selecting only the symbol we want
                                    return(float(i['lastPrice']))

                            return(0)

                        tokens=json_r.keys()
                        all_balance_list=[]
                        n=str(1)

                        hive_total=0
                        len_token=len(tokens)


                        for keys in tokens:
                            #my_post_progress.progress(1/len_token)
                            len_token -= 1
                            quantity=json_r[keys]['pending_token']
                            str_precision=json_r[keys]['precision']
                            precision=int((n.ljust(str_precision+1,'0')))
                            quantity=float(quantity)/precision

                            if(json_r[keys]['cashout_time']>str(dt.utcnow())):
                                flag_current=1

                            hive_price=quantity * get_token_price(keys)
                            hive_price=round(hive_price,3)

                            all_balance_list.append([keys,round(quantity,4),hive_price])
                            
                            if keys not in sum_tokens:
                                sum_tokens[keys] = 0
                                sum_tokens[keys] += quantity
                            else:
                                sum_tokens[keys] += quantity

                            hive_total += hive_price
                        
                        if 'hive' not in only_hive:
                            only_hive['hive'] = 0 
                            only_hive['hive'] += hive_total
                        else:
                            only_hive['hive'] += hive_total

                        

                        if(flag_current==2):
                            st.write("Enter valid URL")

                        elif(flag_current==1):

                            number_votes=len(json_r[keys]['active_votes'])

                            st.write("Number of votes on your post = {}".format(number_votes))


                            df_tokens= pd.DataFrame(all_balance_list,columns=['Token','Amount','Amount in Hive'])

                            st.table(df_tokens)

                            st.write("Total Hive when all the tokens are converted to Hive = {}".format(round(hive_total,3)))
                            st.write("Note : This includes both author and curation rewards ")

                            st.write("<hr>",unsafe_allow_html=True)
                        else:
                            st.write("This post has already been paid out <br><br> Please enter pending posts/comments to get the data ")

                        #print("--"*100)

                    
                    my_post_progress.progress(100)
                    my_post_progress.empty()

                    st.write("<h1><center>Summary</center></h1>",unsafe_allow_html=True)
                    
                    df_sumtokens=pd.DataFrame.from_dict(sum_tokens.items())
                    df_sumtokens.columns=['Token','Quantity']
                    df_sumtokens=df_sumtokens.set_index('Token')
                    
                    st.table(df_sumtokens)

                    hive_final= round(only_hive['hive'],3)
                
                    st.write("If you convert all the tokens to Hive = {} HIVE ".format(hive_final))

                    st.write("<i> Note , this includes both author and curation rewards </i>",unsafe_allow_html=True)
                    
def brofi():
    user=st.text_input('Enter your username')
    user=user.lower()

    if st.button('Retrieve my data'):
        api=Api()
        delegation_list=api.find_all('tokens','delegations',query={'to':'brofi','from':user})
        sum_delegation=[]
        
        for i in range(0,len(delegation_list)):
            sum_delegation.append([delegation_list[i]['symbol'],delegation_list[i]['quantity']])
        df_test=pd.DataFrame(sum_delegation,columns=['symbol','quantity'])
        df_test['quantity']=pd.to_numeric(df_test['quantity'])
        df_test=df_test.groupby('symbol').sum().round(3)

        st.write("Your current delegation to @brofi is-")
        st.table(df_test)
        
        
        start=dt.now()
        end=0
        x=0
        s=[]
        st.write("Please wait while I retrieve your dividends details - ")
        prog=st.progress(0)

        while(end!=1):
            res = requests.get('https://accounts.hive-engine.com/accountHistory?account={}&limit=500&offset={}&token=BRO&timestampStart=1617062280&timestampEnd=1901255650'.format(user,x))
            s.append(res.json())

            x=x+len(res.json())
            if(len(res.json())<500):
                end=1

        listfinal=[]
        for i in range(0,len(s)):
            for j in range(0,len(s[i])):
                listfinal.append(s[i][j])

        prog.progress(25)


            

        store_list=[]
        for i in range(0,len(listfinal)):
            if listfinal[i]['operation']=='tokens_transfer':
                if listfinal[i]['from']=='brofi':
                    if listfinal[i]['symbol']=='BRO':
                        store_list.append([listfinal[i]['from'],listfinal[i]['to'],listfinal[i]['symbol'],listfinal[i]['quantity'],listfinal[i]['memo'],time.strftime('%Y-%m-%d', time.gmtime(listfinal[i]['timestamp']))])
                    
        df_brofi = pd.DataFrame(store_list,columns=['from','to','symbol','quantity','memo','date'])
        df_brofi['quantity']=pd.to_numeric(df_brofi['quantity'])
        df_brofi=df_brofi[df_brofi['date']>'2021-03-29'].reset_index(drop=True)

        df_brofi['memo']=df_brofi['memo'].str.replace("'",'"')
        df_brofi['pay_bro']=df_brofi['memo'].str.split('payout: ')
        df_brofi['payment']=''

        for i in range(0,len(df_brofi)):
            df_brofi['payment'][i]=json.loads(df_brofi['pay_bro'][i][1])

        list_values=[]
        for i in range(0,len(df_brofi)):
            for j in df_brofi['payment'][i].items():
                list_values.append([df_brofi['from'][i],df_brofi['to'][i],j[0],j[1],df_brofi['date'][i]])
            
        df_user_details=pd.DataFrame(list_values,columns=['from','to','symbol','quantity','date'])

        sym_list=list(set(df_user_details['symbol']))

        

        
        n=0
        total_bro=0
        for sym in sym_list:
            
            n=n+1
            prog.progress(n/len(sym_list))
            
                
            st.markdown('<hr>',unsafe_allow_html=True)
            st.header(sym)
            df_sym=df_user_details[df_user_details['symbol']==sym]
            sum_sym=df_sym['quantity'].sum()
            st.write('<div class="card"><div class="card-header"><center>Total BRO you have received for delegating '+sym+' from March 30 till today is : '+'%.6f' % sum_sym+' BRO.</center>',unsafe_allow_html=True)
            
            if sum_sym>0:
                c = alt.Chart(df_sym).mark_line(point=True).encode(x='date', y='quantity',color='symbol',tooltip=['quantity']).properties(width=1400,height=500)                
                st.write(c)

        total_bro = df_user_details.sum()['quantity']
        prog.empty()    

        st.write("Total Bro you have received by delegating all the above tokens from March 30 till today is :{} BRO".format(total_bro.round(7)))

        


        print(dt.now()-start)
          

    
    
if __name__ == '__main__':
    
    st.set_page_config(page_title='Hive Earnings stats',layout='wide')
    choose_app = st.sidebar.selectbox("Choose the app",['Token','Community','BreakEven','Post Rewards','BroFi'])
    api=Api()
    
    
    if choose_app == 'Token':
        
        hivetoken()
        
    elif choose_app == 'Community':
        
        hivecommunity()
    elif choose_app== 'BreakEven':
        
        hivebreakeven()

    elif choose_app== 'BroFi':
        
        brofi()
        
    else:
        hiveauthorrewards()
    
    
    
    

