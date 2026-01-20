# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pandas as pd    
data = [["James","","Smith",30,"M",60000], 
        ["Michael","Rose","",50,"M",70000], 
        ["Robert","","Williams",42,"",400000], 
        ["Maria","Anne","Jones",38,"F",500000], 
        ["Jen","Mary","Brown",45,None,0]] 
columns = ['First Name', 'Middle Name','Last Name','Age','Gender','Salary']

# Create the pandas DataFrame 
pandasDF = pd.DataFrame(data=data, columns=columns) 
  
# print dataframe. 
print(pandasDF)

#Outputs below data on console

pdCount=pandasDF.count()
print(pdCount)

print(pandasDF.max())
print(pandasDF.mean())