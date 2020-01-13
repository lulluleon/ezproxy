""" transform title list into reference data & upload into MySQL """


import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# read title list (csv format)
df = pd.read_csv("elsevier1.csv", encoding='latin', engine='python')

# duplicate rows by difference of startYear and endYear, and create column "year" by allocating each year
df = df.loc[df.index.repeat(df['difference'])]
df['year'] = np.where(df['difference'] <= 1, df['yearEnd'], df['yearStart'] + df.groupby(['SSID', 'intotaCode', 'providerCode'])['yearStart'].cumcount())
df = df.drop_duplicates(subset=['SSID', 'year', 'titleId', 'intotaCode'])
df = df.reset_index(drop=True)

# create dbGroupCode by combining intotaCode, if aggregated, name is "Mixed"
df['dbGroupCode'] = df.groupby(['SSID', 'year', 'titleId'])['intotaCode'].transform(':'.join)
df['dbIncluded'] = df['dbGroupCode'].str.count(':') + 1
df['dbName'] = np.where(df['dbIncluded'] > 1, "Mixed", df['dbName'])
print("data created")

# load to MySQL
df.to_csv("output.csv", index=False)
print("csvfile created")

engine = create_engine('mysql+mysqlconnector://root:Princeps4464!@localhost/UC_Lib_EZproxy_sandbox', echo=False)
# fTitleName,SSID,titleId,dbName,intotaCode,providerName,providerCode,status,type,dbHost,dbCode,ISSN,ISBN,providerTitleId,sTitleName,yearStart,yearEnd,difference,year,subject, dbGroupCode

df.to_sql(name='temporary_table', con=engine, if_exists='replace', index=False)
print("temporary table created")
with engine.begin() as cur:
    # query to insert rows in libDatabase table
    qry1 = 'INSERT IGNORE INTO libDatabase (SELECT dbName, intotaCode, providerName, providerCode, dbGroupCode FROM temporary_table)'
    cur.execute(qry1)
    print("libDatabase")

    # query to insert rows in libTitleList table
    qry2 = 'INSERT IGNORE INTO libTitleList (SELECT fTitleName, SSID, titleId, status, type, subject FROM temporary_table)'
    cur.execute(qry2)
    print("LibTitlelist")

    # query to insert instances dbByUniqueIdentifier (delete if column "uniqueIdentifier" contains empty string)
    qry3 = 'INSERT IGNORE INTO dbByUniqueIdentifier (SELECT SSID, providerCode, ISSN, year, dbGroupCode FROM temporary_table)'
    qry4 = 'INSERT IGNORE INTO dbByUniqueIdentifier (SELECT SSID, providerCode, ISBN, year, dbGroupCode FROM temporary_table)'
    qry30 = 'DELETE FROM dbByUniqueIdentifier WHERE uniqueIdentifier = ""'
    cur.execute(qry3)
    cur.execute(qry4)
    cur.execute(qry30)
    print("dbByUniqueIdentifier")

    # query to insert instances dbByProviderTitleId
    qry5 = 'INSERT IGNORE INTO dbByProviderTitleId (SELECT SSID, providerCode, providerTitleId, year, dbGroupCode FROM temporary_table)'
    qry50 = 'DELETE FROM dbByProviderTitleId WHERE providerTitleId = 0'
    cur.execute(qry5)
    cur.execute(qry50)
    print("dbByProviderTitleId")

    # query to insert instances dbByTitleName
    qry6 = 'INSERT IGNORE INTO dbByTitleName (SELECT SSID, providerCode, sTitleName, year, dbGroupCode FROM temporary_table)'
    cur.execute(qry6)
    print("dbByTitleName")

print("success!")
