import multiprocessing as mp
import pandas as pd
import os
import numpy as np
import time
from joblib import Parallel,delayed
def createDir(id2name, outputPath):
    # create the directories 
    # outputPath: "stockNews/"
    if not os.path.exists(outputPath):
        os.mkdir(outputPath)
    
        for stockid in id2name.keys():
            os.makedirs(outputPath + "/{}".format(stockid))
    pass

def assign(newsFiles, id2name, stockNewsCount):
    # write corresponding news to corresponding txt file
    # stockNewsDateCount = {}
    for csvFile in newsFiles: # 600 days of news file
        # csvFile: sina-2020-10-10.csv
        if csvFile[-3:] != 'csv':
            continue
        date = csvFile[-14:-4] # 2020-10-10

        # open file
        if os.path.exists(csvFile):
            newsFile = pd.read_csv(csvFile, sep = '\t')
            for tup in newsFile.itertuples():
                if csvFile[-22:-15] == 'general':
                    title = tup[1]
                    content = tup[2]
                else:
                    title = tup[2]
                    content = tup[3]
                if pd.isna(content):
                    content = ' '
                if pd.isna(title):
                    title = ' '
                content = content.strip().replace(u'\u3000', u' ').replace(u'\xa0', u' ').replace('\n', ' ')
                for id_ in id2name.keys():
                    if id_ in title or id_[2:] in title or id2name[id_] in title or \
                        id_ in content or id_[2:] in content or id2name[id_] in content:
                        f = open(outputPath + f'/{id_}/{date}.txt', 'a+',encoding='utf-8')
                        f.write(title + ' ' + content + '\n')
                        f.close()

                        stockNewsCount[id_][date] = stockNewsCount[id_].get(date, 0) + 1 # maintain the count of news for each stock

    # stockNewsCount[id_] = stockNewsDateCount
    pass

def process(idList, stockNewsCount):
    for id_ in idList:
        assign(id_, newsFiles, id2name, stockNewsCount)
    pass

###########################
# Defining variables
# change this path to fit yours directory
dataPath = 'stockDataFromTushare'
outputPath = dataPath + '/stockNews'
mappingPath = dataPath + '/stockid2name.csv'


import tushare as ts
import numpy as np
from tqdm import tqdm
import datetime
ts.set_token('3f545dd41ef95e7da766a941dc16c58761da22450f6b70ebdff858a8')
pro = ts.pro_api()
# data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
# data.iloc[:,0] = data.iloc[:,0].apply(lambda x:x.lower()[-2:]+x[:-3])
# data.to_csv(mappingPath)

id2namedf= pd.read_csv(mappingPath, sep = ',',index_col=0)
id2name = {tup[1]:tup[2] for tup in id2namedf.itertuples()}

newsFiles = [dataPath + '/general/' + csvFile for csvFile in os.listdir(dataPath + '/general')] + \
            [dataPath + '/sina/' + csvFile for csvFile in os.listdir(dataPath + '/sina')] + \
            [dataPath + '/ths/' + csvFile for csvFile in os.listdir(dataPath + '/ths')] + \
            [dataPath + '/eastmoney/' + csvFile for csvFile in os.listdir(dataPath + '/eastmoney')] + \
            [dataPath + '/ycj/' + csvFile for csvFile in os.listdir(dataPath + '/ycj')] + \
            [dataPath + '/ws/' + csvFile for csvFile in os.listdir(dataPath + '/ws')]
def get_nday_list(n):
    before_n_days = []
    if isinstance(n,str):
        n = (datetime.date.today()-datetime.datetime.strptime(n,"%Y%m%d").date()).days
    for i in range(1, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days

dateList = get_nday_list("20190101")
newsFilesList = [['//'.join([dataPath,source,f'{source}-{date}.csv']) for source in ['general','sina','ths','eastmoney','ycj','ws']] for date in dateList]
if __name__ == '__main__':
    # start
    # creating directories
    createDir(id2name, outputPath)

    # initializing variables for parallel processing
    id_list = list(id2name.keys())
    nb_process = int(mp.cpu_count()) - 1
    #nb_process = 7
    l = list(np.array_split(id_list, nb_process))
    l = [x.tolist() for x in l]

    # stockNewsCount = mp.Manager().dict() # count how many news on a certain date for a certain stock
    stockNewsCount = {tup[1]: {} for tup in id2namedf.itertuples()}
    # process_list = [mp.Process(target=process, args = (idList,stockNewsCount)) for idList in l]

    time1=time.time()
    # for p in process_list:
    #     p.start()
    #
    # for p in process_list:
    #     p.join()
    Parallel(n_jobs=nb_process-1)(delayed(assign)(newsFile,id2name,stockNewsCount) for newsFile in tqdm(newsFilesList))
    # for newsFile in tqdm(newsFilesList):
    #     assign(newsFile,id2name,stockNewsCount)
    time2=time.time()
    print('Cost time: ' + str(time2 - time1) + 's')
    # Cost time: 1346.320482969284s
    # for 300 stocks and 4000 news files
    ######################
    # some analysis of the assign result

    stockNewsCount = stockNewsCount
    cnt = 0
    for key in stockNewsCount.keys():
        cnt += np.sum(list(stockNewsCount[key].values()))
    print("Altogether {} news for 300 stocks".format(cnt))
    # Altogether 163251 news for 300 stocks

    print("Averagely {} news for each stock".format(cnt/300))
    # Averagely 544.17 news for each stock

    #count	300.000000
    #mean	544.170000
    #std	1145.446034
    #min	48.000000
    #25%	144.750000
    #50%	260.500000
    #75%	542.750000
    #max	15046.000000
