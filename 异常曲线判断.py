import pandas as pd
import numpy as np
import sys
from datetime import datetime,time,timedelta
import pymysql

def _dtw_distance(ts_a, ts_b, d = lambda x,y: abs(x-y)):
        max_warping_window=10000
        # Create cost matrix via broadcasting with large int
        ts_a, ts_b = np.array(ts_a), np.array(ts_b)
        M, N = len(ts_a), len(ts_b)
        cost = sys.maxsize * np.ones((M, N))

        # Initialize the first row and column
        cost[0, 0] = d(ts_a[0], ts_b[0])
        for i in range(1, M):
            cost[i, 0] = cost[i-1, 0] + d(ts_a[i], ts_b[0])

        for j in range(1, N):
            cost[0, j] = cost[0, j-1] + d(ts_a[0], ts_b[j])

        # Populate rest of cost matrix within window
        for i in range(1, M):
            for j in range(max(1, i - max_warping_window),
                            min(N, i + max_warping_window)):
                choices = cost[i - 1, j - 1], cost[i, j-1], cost[i-1, j]
                cost[i, j] = min(choices) + d(ts_a[i], ts_b[j])

        # Return DTW distance given window 
        return cost[-1, -1]
def get_data(check_date):
    config={
          *
        }
    db=pymysql.connect(**config)
    cursor=db.cursor()
    sql="select date,time,disk_ser,raid_id,pg_resp from dasd_perf_pg where date='"+check_date+"' "
    cursor.execute(sql)
    sqldata=cursor.fetchall()
    df=pd.DataFrame(list(sqldata),columns=['date','time','disk_ser','raid_id','pg_resp'])
    cursor.close()
    db.close()
    return df
#取当前值
DiskSerList=[50562]
CheckDate=datetime.now().strftime('%Y-%m-%d')
data=get_data(CheckDate)
data['datetime']=str(data['date'])+' '+str(data['time'])

delete_id=[]
for i in data['raid_id'].unique():
    if data[data['raid_id']==i]['pg_resp'].sum() < 0:
        if i not in delete_id:
            delete_id.append(i)
data=data[~data['raid_id'].isin(delete_id)]


data.index=data['datetime']
#确定基线曲线
for ser in DiskSerList:
    data_ser=data[data['disk_ser']==ser]
    pg_resp_median_dict={}
    for i in data_ser.index:
        pg_resp_median_dict[i]=data_ser['pg_resp'].ix[i].median() 
    pg_resp_median_list=sorted(pg_resp_median_dict.items(),key=lambda x:x[0])
    list1=[]
    list2=[]
    for i in pg_resp_median_list:
        list1.append(i[1])
        list2.append(i[0])
    base_list=pd.Series(list1,index=list2)
    #将所有曲线与基线曲线对比，计算DTW
    idlist=data_ser['raid_id'].unique()
distance_dict={}
for key in idlist:
    comp_list=data_ser['pg_resp'].loc[data_ser['raid_id']==key]
    distance = _dtw_distance(base_list, comp_list)
    distance_dict[key]=distance
#异常告警处理模块
series_distance=pd.Series(list(distance_dict.values()),index=list(distance_dict.keys()))
td=series_distance.describe()
high = td['75%'] + 1.5 * (td['75%'] - td['25%'])
low = td['25%'] - 1.5 * (td['75%'] - td['25%'])
forbid_index =series_distance[(series_distance > high) | (series_distance < low)].index
for id in forbid_index.values:
    print(id+'的盘卷响应时间异常，请重点关注')

