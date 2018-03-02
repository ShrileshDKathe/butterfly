
# coding: utf-8

# In[151]:


import pandas as pd


# In[152]:


data = pd.read_csv('beta_1_icici.csv')
data = data.rename(columns=lambda x: x.strip())  #Takes care of white-spaces
data['DATE']=pd.to_datetime(data['DATE'])


# In[153]:


data=data.sort_values(['DATE','STRIKE PRICE'],ascending=[True,True]) # Sorts data by Date and strike price


# In[154]:

# Function to calculate window

def window_calc(l):
    length=len(l)
    final=[None] * (length)

    for i in range(length-1):
        if(i==0):
            continue
        fin=-2*l[i]+l[i-1]+l[i+1]
        final[i]=fin
    return final
    
    


# In[155]:


t=data.groupby('DATE')
cols=['STRIKE PRICE','DATE','OPEN','HIGH','LOW','CLOSE']
short_bf=pd.DataFrame(columns=cols)
for date,group in t:
    #print ('Calculating for date:'+ str(date))
    opn=window_calc(list(group['OPEN']))
    close=window_calc(list(group['CLOSE']))
    high=window_calc(list(group['HIGH']))
    low=window_calc(list(group['LOW']))
    SP=list(group['STRIKE PRICE'])
    final={'STRIKE PRICE':SP,'DATE':[date]*(len(opn)),'OPEN':opn,'HIGH':high,'LOW':low,'CLOSE':close}
    #print(len(SP),len([date]*(len(opn))),len(opn),len(high),len(low),len(close),)
    df=pd.DataFrame.from_dict(final)
    short_bf=pd.concat([short_bf,df])
    


# In[156]:


short_bf=short_bf.round({'OPEN': 2, 'HIGH': 2,'LOW':2,'CLOSE':2})


# In[157]:


final_df=pd.DataFrame(columns=['STRIKE PRICE','DATE','OPEN_SB','HIGH_SB','LOW_SB','CLOSE_SB','OPEN_LB','HIGH_LB','LOW_LB','CLOSE_LB'])


# In[158]:


final_df[['STRIKE PRICE','DATE','OPEN_SB','HIGH_SB','LOW_SB','CLOSE_SB']]=short_bf[['STRIKE PRICE','DATE','OPEN','HIGH','LOW','CLOSE']]


# In[159]:


#short_bf.to_csv('short_butterfly.csv',columns=['STRIKE PRICE','DATE','OPEN','HIGH','LOW','CLOSE'])


# In[160]:


short_bf_avg=short_bf.groupby('DATE').mean()


# In[161]:


short_bf_avg['SB_AVG']=(short_bf_avg['OPEN'] +short_bf_avg['HIGH'] + short_bf_avg['LOW'] + short_bf_avg['CLOSE'] )/4


# In[162]:


short_bf_avg=short_bf_avg.round({'SB_AVG':2})
short_bf_avg=short_bf_avg.drop(['CLOSE','HIGH','LOW','OPEN'],axis=1)


# In[163]:


final_df = final_df.join(short_bf_avg,on=['DATE'],how='inner')


# In[164]:


long_bf=short_bf


# In[165]:


long_bf['OPEN']=-short_bf['OPEN']
long_bf['HIGH']=-short_bf['HIGH']
long_bf['LOW']=-short_bf['LOW']
long_bf['CLOSE']=-short_bf['CLOSE']



# In[166]:


long_bf=long_bf.round({'OPEN': 2, 'HIGH': 2,'LOW':2,'CLOSE':2})


# In[167]:


final_df[['OPEN_LB','HIGH_LB','LOW_LB','CLOSE_LB']]=long_bf[['OPEN','HIGH','LOW','CLOSE']]


# In[168]:


#long_bf.to_csv('long_butterfly.csv',columns=['STRIKE PRICE','DATE','OPEN','HIGH','LOW','CLOSE'])


# In[169]:


long_bf_avg=long_bf.groupby('DATE').mean()


# In[170]:


long_bf_avg['LB_AVG']=(long_bf_avg['OPEN'] +long_bf_avg['HIGH'] + long_bf_avg['LOW'] + long_bf_avg['CLOSE'] )/4


# In[171]:


long_bf_avg=long_bf_avg.round({'LB_AVG':2})
long_bf_avg=long_bf_avg.drop(['CLOSE','HIGH','LOW','OPEN'],axis=1)


# In[172]:


#long_bf_avg.to_csv('long_butterfly_avg.csv',columns=['AVG'])


# In[173]:


final_df = final_df.join(long_bf_avg,on=['DATE'],how='inner')


# In[175]:

try:
    final_df.to_csv('output/butterfly.csv' , index=False)

except:
    print('File already exists.')

