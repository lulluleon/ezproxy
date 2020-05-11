""" transform title list into reference data & upload into MySQL """


import pandas as pd
import numpy as np
import glob
import datetime
import time
from sqlalchemy import create_engine

titlefiles = glob.glob('upload/springer_book.csv')
date_string = str(datetime.date.today())  # 'yyyy-mm-dd'
time_string = time.strftime("%H%M", time.localtime())
outputfile = 'upload\output/'+date_string+"-"+time_string+'.csv'

# read title list (csv format)
for titlefile in titlefiles:
    print("reading " + titlefile)  # check if files are in read

    df = pd.read_csv(titlefile, encoding='latin', engine='python')
    df = df.drop_duplicates(subset=['SSID', 'titleId', 'providerCode', 'intotaCode', 'sTitleName', 'providerTitleId',
                                    'ISSN', 'ISBN', 'yearStart', 'yearEnd'])

    # duplicate rows by difference of startYear and endYear, and create column "year" by allocating each year
    df = df.loc[df.index.repeat(df['difference'])]
    df['agg1'] = df[['providerCode', 'titleId', 'yearStart', 'yearEnd', 'intotaCode', 'sTitleName', 'ISSN',
                     'ISBN', 'providerTitleId']].apply(lambda x: ','.join(x.astype(str)), axis=1)
    df['year'] = np.where(df['difference'] <= 1, df['yearEnd'],
                          df['yearStart']+df.groupby(['SSID', 'agg1'])['yearStart'].cumcount())
    df = df.drop_duplicates(subset=['SSID', 'titleId', 'providerCode', 'intotaCode', 'sTitleName',
                                    'providerTitleId', 'ISSN', 'ISBN', 'year'])

    df['agg2'] = df[['providerCode', 'SSID', 'titleId', 'sTitleName', 'ISSN', 'ISBN',
                     'providerTitleId']].apply(lambda x: ','.join(x.astype(str)), axis=1)
    df['dbGroupCode'] = df.groupby(['agg2', 'year'])['intotaCode'].transform(':'.join)
    df['dbName'] = df.groupby(['agg2', 'year'])['dbName'].transform(':'.join)
    df['dbIncluded'] = df['dbGroupCode'].str.count(':') + 1
    df = df.reset_index(drop=True)
    df = df.drop(['agg1', 'agg2'], axis=1)

    print(df.head(10))
    print("data created")

    # load to MySQL
    df.to_csv(outputfile, index=False)
    print("csvfile created")
    df = df.drop(['yearStart', 'yearEnd', 'dbIncluded', 'difference'], axis=1)


    engine = create_engine('mysql+mysqlconnector://[user_name]:[password]@[host_name]/[databse_name]', echo=False)

    df.to_sql(name='temporary_table', con=engine, if_exists='replace', index=False)
    print("temporary table created")

    with engine.begin() as cur:
        # query to insert rows in libDatabase table
        qry1 = 'INSERT IGNORE INTO libDatabase (SELECT dbName, intotaCode, providerName, providerCode, dbGroupCode ' \
               'FROM temporary_table)'
        cur.execute(qry1)
        print("libDatabase")

        # query to insert rows in libTitleList table
        qry2 = 'INSERT IGNORE INTO libTitleList (SELECT fTitleName, SSID, providerCode, titleId, status, type, subject, ' \
               'dbGroupCode FROM temporary_table)'
        cur.execute(qry2)
        print("LibTitlelist")

        # query to insert instances dbByUniqueIdentifier (delete if column "uniqueIdentifier" contains empty string)
        qry3 = 'INSERT IGNORE INTO dbByUniqueIdentifier (SELECT SSID, providerCode, ISSN, dbGroupCode, year ' \
               'FROM temporary_table)'
        qry4 = 'INSERT IGNORE INTO dbByUniqueIdentifier (SELECT SSID, providerCode, ISBN, dbGroupCode, year ' \
               'FROM temporary_table)'
        qry30 = 'DELETE FROM dbByUniqueIdentifier WHERE uniqueIdentifier = ""'
        cur.execute(qry3)
        cur.execute(qry4)
        cur.execute(qry30)
        print("dbByUniqueIdentifier")

        # query to insert instances dbByProviderTitleId
        qry5 = 'INSERT IGNORE INTO dbByProviderTitleId (SELECT SSID, providerCode, providerTitleId, dbGroupCode, year ' \
               'FROM temporary_table)'
        qry50 = 'DELETE FROM dbByProviderTitleId WHERE providerTitleId = 0'
        cur.execute(qry5)
        cur.execute(qry50)
        print("dbByProviderTitleId")

        # query to insert instances dbByTitleName
        qry6 = 'INSERT IGNORE INTO dbByTitleName (SELECT SSID, providerCode, sTitleName, dbGroupCode, year ' \
               'FROM temporary_table)'
        qry60 = 'DELETE FROM dbByTitleName WHERE sTitleName = ""'
        cur.execute(qry6)
        cur.execute(qry60)
        print("dbByTitleName")

        qry7 = 'INSERT IGNORE INTO dbByCode (SELECT dbCode, dbGroupCode FROM temporary_table ' \
               'WHERE dbGroupCode NOT LIKE "%:%")'
        qry70 = 'DELETE FROM dbByCode WHERE dbCode = ""'
        cur.execute(qry7)
        cur.execute(qry70)
        print("dbByCode")

        qry8 = 'INSERT IGNORE INTO dbByHost (SELECT dbHost, dbGroupCode FROM temporary_table ' \
               'WHERE dbGroupCode NOT LIKE "%:%")'
        qry80 = 'DELETE FROM dbByHost WHERE dbHost = ""'
        cur.execute(qry8)
        cur.execute(qry80)
        print("dbByHost")
        
        # query to insert instances dbByProviderArticleId
        qry9 = 'INSERT IGNORE INTO dbByProviderTitleId (SELECT providerCode, uniqueIdentifier, providerArticleId ' \
               'FROM temporary_table)'
        qry90 = 'DELETE FROM dbByProviderTitleId WHERE providerTitleId = 0'
        cur.execute(qry9)
        cur.execute(qry90)
        print("dbByProviderArticleId")

    print("success!")


