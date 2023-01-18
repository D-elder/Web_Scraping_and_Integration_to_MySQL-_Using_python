#!/usr/bin/env python
# coding: utf-8

# Importing libraries for scrapping web data

# In[ ]:


#importing libraries
import bs4 
from bs4 import BeautifulSoup
import requests


# reading the url

# In[5]:


# reading url
url = 'https://en.wikipedia.org/wiki/List_of_largest_manufacturing_companies_by_revenue' #reading url
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')


# In[6]:


## #we grab the body of the table and store in the varaible table

table = soup.find('table', {'class': 'wikitable sortable plainrowheads'}).tbody


# In[7]:


# reading the columns of the scraped data set
rows = table.find_all('tr')

columns = [v.text.replace('\n', '') for v in rows[0].find_all('th')] # cleaning foreign elements

print(columns)


# In[8]:


# Getting the body of the tables as values

for i in range(1, len(rows)):
  tds = rows[i].find_all('td')
  if len(tds) == 4:
    values = [tds[0].text, tds[1].text,'',tds[2].text, tds[3].text.replace('\n', ''.replace('\xa0', ''))]
  else:
    values = [td.text.replace('\n', '').replace('\xa0', '') for td in tds]
    print(values)


# In[10]:


# converting columns to a dataframe
import pandas as pd
df = pd.DataFrame(columns=columns)


# In[11]:


# converting table body(rows) to dataFrame
df2 = pd.DataFrame(data=values)


# In[12]:


# joining scraped df and df2

data = df.append(df2, ignore_index=True)


# In[22]:


#installing pythnon-mysql link package
pip install mysql-connector-python


# In[23]:


# importing Mysql connector and creating database
import mysql.connector as msql
from mysql.connector import Error
try:
    conn = msql.connect(host='localhost', user='root',  
                        password='????????')#give ur username, password
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE revenue")
        print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)


# In[30]:


data.head()


# In[41]:


#creating table on thye created database

import mysql.connector as msql
from mysql.connector import Error
try:
    conn = msql.connect(host='localhost', database='revenue', user='root', password='?????????')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS employee_data;')
        print('Creating table....')
 # in the below line please pass the create table statement which you want #to create
        cursor.execute("CREATE TABLE revenue_data3(No varchar(255),Company varchar(255),Industry varchar(255),Revenue int,Headquarters varchar(255))")
        print("Table is created....")
        #loop through the data frame
        for i,row in data.iterrows():
            #here %S means string values 
            sql = "INSERT INTO revenue.revenue_data VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)


# In[ ]:




