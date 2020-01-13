"""Parse EZproxy logfile and export results into csv"""

from urllib.parse import urlparse, unquote_plus
from crossref.restful import Works
import glob
import csv
import re
import sys
import mysql.connector
import datetime
import time

# Raw Log Files, check directory name
inputfiles = glob.glob('data/ezp201924.log')

# using date/time format to create output file names
date_string = str(datetime.date.today())  # '2017-12-26'
time_string = time.strftime("%H%M", time.localtime())
outputfile = 'output/'+date_string+"-"+time_string+'.csv'

# incomplete refurl tags causing error
errortag = ['132.181.', 'app:', 'sogou', 'a:3:{s:2:', 'file:/']
#  original script: 'app:', '(null)', "blank", "132.181", "file:", "6DAC0D5BDFC3D6CFEE151", "27D9F7D4CEBE234B32122", "6DAC0D5BDFC3D6CFEE202"

# exclude staffip if necessary
staffips = []

on_campusips = ['132.181', '202.36.178', '202.36.179', ]  # IP range 132.181.*.*: oncampus wired, IP range 202.36.178.* , 202.36.179.*

# if in list, try search patterns
target_provider = ['springer', 'elsevier', 'sciencedirect', 'oxf', 'oed', 'oup', 'ebsco', 'jstor', 'hein', 'wiley', 'sage', 'proquest']

dbidentification_host = ['sk.sage', 'methods.sage', 'thecochranelibrary', 'ebookcentral.proquest', 'parlipapers.proquest', 'ulrichsweb.serialssolutions',
                         'bbcjournalism', 'socialwork.oxfordre', 'opil.ouplaw', 'oxfordartonline', 'oxfordbibliographies',
                         'classics.oxfordre', 'oxfordhandbooks', 'academic.oup', 'oaj.oxfordjournals', 'oxfordmusiconline', 'oed.com',
                         'agupubs.onlinelibrary.wiley', '.onlinelibrary.wiley', 'birpublications.org']

dbidentification_path = ['/artbibliographies', 'professional/australianeducationindex', 'professional/scb', '/professional/', '/eebo', '/earthquake', '/eric',
                         '/georef', '/georefinprocess', '/healthcomplete', '/lion', '/materialsienceengineering', '/pqdt', '/hnpnewyorktimes', '/ptsdpubs',
                         '/sociologicalabstracts','/socabs', '/classics/', '/conference',  # proquest path indicator
                         ]

other_usage = ['/ehost/pdfviewer', '/eds/pdfviewer','/ehost/ebookviewer', '/EbscoViewerService/ebook', #ebscohost
               'docview/'] # proquest

dbindentification_query = ['db=', 'collection=', 'oso:', 'journalCode=']

name_indicator = ['publication', 'loi', 'toc', 'journal', 'book', 'series', 'productKey', 'stable', 'topics', 'Book', 'reference', 'Reference', 'home', 'title', '10.1068',]

issn_indicator = ['publication', 'loi', 'toc', 'issn', 'journal', '/', ]

httpcode = [200, ]


# avoid strings
geturl_astring = ['rl/exams', '/connect', 'gr2tq4rz9x', 'rss', 'maf/app', '/beacon', '/config', '/menu', '/logout', '/login', '/Login', '/api', '/_default', '/action/',
                  '/font', '/cookies', '/ImportWidget', '/botdetectcaptcha', '/metadata/suggest', '/userimages', '/retrieve', '/checkout/cart',
                  '/localization', '/truesightedgemarker', '/sdfe/arp', '/Authentication', '/ExternalImage', '/stat', '/css', 'volumesAndIssues/', 'wicket:', # springer auto-generated
                  '/companylogo', '/signup-login', '/corporate-login', '/widget', '/change-location',
                  '/redirect', '_ajax', '/metaproducts/','.progressivedisplay', #  proquest auto-generated
                  '/topics/annotations', '/SSOCore', '/science/link', #  elsevier(sciencedirect) auto-generated
                  '/record/pubMetDataAjax.uri', '/record/recordPageLinks.uri', '/record/references.uri', '/home.uri', '/redirect/linking.uri', '/cto2/getdetails.uri',#  scopus auto-generated
                  '/HOL/Welcome', '/HOL/VMTPG', '/HOL/message', '/HOL/ajaxcalls', '/HOL/dummy', '/HOL/Citation', '/HOL/PL', '/HOL/P', '/HOL/QRCode', '/HOL/SearchVolumeSOLR',
                  '/HOL/ViewImageLocal', '/HOL/insertBeta', '/HOL/PageDummy',  '/HOL/highlights', '/HOL/PDFsearchable', '/HOL/Print', '/HOL/ErrorCatch', '/HOL/NotSubscribed', '/HOL/NotAvailable', #  hein auto-generated logs
                  '/data/Get', 'Data/Get', '/Data/Search', '/Data/Populate', '/Books/GetRelatedChapters', '/Books/ChapterMetadata', '/Books/Metadata', '/Books/BookContentTabs', '/Books/SearchChapters',
                  '/Search/Get', '/MethodsMap/GetMethodsMapRef', 'bundles', '/embeddedvideo', '/search/advanced', #  sage auto-generated
                  '/community/society-resources', '/sdfe/ears', '/readcube/log', '/ehost/result', '/track/controlled/', '/Messages.properties', '/audio',
                  '.png', '.css', 'woff', '.svg', '.jpg', 'jpeg', '.gif', '.f4v', '.ico', 'json', '.swf', 'tiff', '.tff', '.js', 'woff2', '.txt', '.sml', ':ping',
                  'bsigroup'] # should be included for complete DB

# column names
fieldnames = ['ipAddress', 'userName', 'onCampus', 'dateTime', 'getUrl', 'getHost', 'getPath', 'getQuery', 'category',
              'providerCode', 'dbHost', 'dbCode', 'year', 'issn', 'isbn', 'uniqueIdentifier', 'sTitleName', 'providerTitleId', 'doi', 'pii', 'unidentifiable',
              'httpStatus', 'refUrl', 'refHost',]

# open outputfile to write to
with open(outputfile, 'w', newline='', encoding="utf-8") as outputs:
    wr = csv.DictWriter(outputs, fieldnames=fieldnames)
    wr.writeheader()
    print('EZproxy analysis beginning... This may take a few minutes.\n')  # check if csv file created

# read each raw log file
    for inputfile in inputfiles:
        linenumber = 1

        with open(inputfile) as logfile:
            resultdict = {fieldnames[i]: '' for i in range(0, len(fieldnames))}  # convert list into dictionary
            print("reading " + inputfile)  # check if files are in read
            time.sleep(1)

            line = logfile.readline()

            # start splitting each line of the logfile up
            while line:
                elements = line.split()  # split the line on spaces into elements list
                line = str(logfile.readline()).rstrip('\n')

                ipaddress = elements[0]
                resultdict["ipAddress"] = ipaddress

                if any(x in ipaddress for x in on_campusips):
                    resultdict["onCampus"] = "Y"
                else:
                    resultdict["onCampus"] = "N"

                user = elements[2]
                resultdict["userName"] = user

                # date is in two elements, glue them together and convert datetime format
                date = (elements[3] + elements[4]).strip('[]')
                date = date[0:20]
                date = datetime.datetime.strptime(date, '%d/%b/%Y:%H:%M:%S')
                resultdict["dateTime"] = date

                geturl = elements[6]  # full requesting url
                u_geturl = unquote_plus(geturl)
                resultdict["getUrl"] = u_geturl

                p_geturl = urlparse(u_geturl)  # parse requesting url
                ghost = p_geturl.hostname
                resultdict["getHost"] = ghost  # hostname for
                gpath = p_geturl.path
                resultdict["getPath"] = gpath
                gquery = p_geturl.query
                resultdict["getQuery"] = gquery

                ## find provider
                if 'springer' in ghost:
                    resultdict["providerCode"] = 'PRVAVX'
                elif 'elsevier' in ghost:
                    resultdict["providerCode"] = 'PRVESC'
                elif 'sciencedirect' in ghost:
                    resultdict["providerCode"] = 'PRVESC'
                elif 'scopus' in ghost:
                    resultdict["providerCode"] = 'PRVESC'
                elif 'oxf' in ghost:
                    resultdict["providerCode"] = 'PRVASL'
                elif 'oed' in ghost:
                    resultdict["providerCode"] = 'PRVASL'
                elif 'oup' in ghost:
                    resultdict["providerCode"] = 'PRVASL'
                elif 'ebsco' in ghost:
                    resultdict["providerCode"] = 'PRVEBS'
                elif 'proquest' in ghost:
                    resultdict["providerCode"] = 'PRVPQU'
                elif 'jstor' in ghost:
                    resultdict["providerCode"] = 'PRVJST'
                elif 'hein' in ghost:
                    resultdict["providerCode"] = 'PRVBAA'
                elif 'wiley' in ghost:
                    resultdict["providerCode"] = 'PRVWIB'
                elif 'sage' in ghost:
                    resultdict["providerCode"] = 'PRVSPB'
                else:
                    resultdict["providerCode"] = None

                        ## find db indicator
                # with hostname (sage, oxford, some wiley & proQuest)
                if any(x in ghost for x in dbidentification_host):
                    resultdict["dbHost"] = ghost
                else:
                    resultdict["dbHost"] = None

                # with dbcode (ebscohost, hein, proQuest)
                if any(x in gpath for x in dbidentification_path):
                    dbcode1 = re.search(r'(/)(\w*)', gpath).group(2)
                    resultdict["dbCode"] = dbcode1
                elif any(x in gquery for x in dbindentification_query):
                    try:
                        dbcode2 = re.search(r'(db=|collection=|journalCode=|oso:)(\w*)(/|)', gquery).group(2)
                        resultdict["dbCode"] = dbcode2
                    except AttributeError:
                        resultdict["dbCode"] = None
                elif "eid=2-s" in gquery:
                    resultdict["dbCode"] = "SCOPUS"
                else:
                    resultdict["dbCode"] = None

                # with journalname(shortname included) or providerId(springer, proQuest)
                if any(x in gpath for x in name_indicator):
                    try:
                        journal1 = re.search(r'(/loi/|/toc/|\.journals/|/journal/|/book/|/books/|/Book/|/reference/|/Reference//|/home/|/topics/)(\D{2,}\d*)(/|\?|)', gpath).group(2)
                        journal1 = journal1.replace('-', '')
                        try:
                            journal1=journal1.split('/')[0]
                            resultdict["sTitleName"] = journal1
                        except AttributeError:
                            resultdict["sTitleName"] = journal1
                    except AttributeError:
                        resultdict["sTitleName"] = None
                    try:
                        journal3 = re.search(r'(10.1068/|10.3141/|10.1037/|10.2190/|10.1023/)(\w*)', gpath).group()  ## exceptional doi format: directly allocate db by doi
                        resultdict["sTitleName"] = journal3
                    except:
                        pass

                elif 'title' in gquery:
                    try:
                        journal2 = re.search (r'(title=)(\D{2,}|\D{2,}\d*)(&date)', gquery).group(2)
                        journal2 = journal2.replace(' ', '')
                        journal2 = journal2.lower()
                        resultdict["sTitleName"] = journal2
                    except AttributeError:
                        resultdict["sTitleName"] = None
                else:
                    resultdict["sTitleName"] = None

                if any(x in gpath for x in name_indicator):
                    try:
                        providerid1 = re.search(r'(/bookseries/|/series/|productKey=|/journal/|/stable/pdf/|/stable/|publication/|publications|theBookId=)(\d{1,7}\b)', gpath).group(2)
                        if 'hein' in ghost:
                            resultdict["providerTitleId"] = None
                        else:
                            resultdict["providerTitleId"] = providerid1
                    except AttributeError:
                        resultdict["providerTitleId"] = None
                elif any(x in gquery for x in name_indicator):
                    try:
                        providerid2 = re.search(r'(productKey=|theBookId=)(\d{1,7})', gquery).group(2)
                        resultdict["providerTitleId"] = providerid2
                    except AttributeError:
                        resultdict["providerTitleId"] = None
                else:
                    resultdict["providerTitleId"] = None

                # with isbn (most ebooks)
                if '978' in u_geturl:
                    try:
                        isbn1 = re.search(r'(978)-(\d*)-(\d*)-(\d*)-(\d*)|(978)-(\d*)-(\d*)-(\d*)|(978)(\d{10})', u_geturl).group()
                        try:
                            isbn1 = isbn1.replace('-', '')
                            resultdict["isbn"] = isbn1
                        except:
                            resultdict["isbn"] = isbn1
                    except AttributeError:
                        resultdict["isbn"] = None

                # with issn - need to look up from other dataset (e.g. titlelist)
                elif any(x in u_geturl for x in issn_indicator):
                    try:
                        issn1 = re.search(r'(/publication/|publication=|/loi/|/loi/S|/loi/s|/toc/|/toc/S|/toc/s|issn\.|issn/|issn=|journal:|/journal/|/\w*/\w*/\d*/\d*/\d*/|/)(\d{4}-\w*|\d{8}\b|\d{6}\w{2}\b)', u_geturl).group(2)
                        issn1 = issn1.replace('-', '')
                        if re.search(r'(978[0-9]{5})', issn1) != None:
                            resultdict["issn"] = None
                        elif "/docview" in gpath:
                            resultdict["issn"] = None
                        else:
                            resultdict["issn"] = issn1
                    except AttributeError:
                        resultdict["issn"] = None
                else:
                    resultdict["issn"] = None

                #  with doi - need to look up from other dataset
                if '10.' in u_geturl:
                    try:
                        doi1 = re.search(r'(10\.)(\d*/)(\w*\.)(\w*\.)(\d*\.)(\d*\.)(\w*)|(10\.)(\d*/)(\w*\.)(\w*\.)(\d*\.)(\w*)|'
                                         r'(10\.)(\d*/)(\w*\.)(\w*\.)(\w*)|(10\.)(\d*/)(\(\w*\))(\d*)-(\d*)|(10\.)(\d*/)(\w*:)(\w*)|(10\.)(\d*/)(\w*)-(\d*)-(\d*)-(\d*)-(\w*)|'
                                         r'(10\.)(\d*/)(\w*)-(\d*)-(\d*)-(\w*)|(10\.)(\d*/)(\w*\.)(\w*)-(\w*)-(\d*)-(\w*)|'
                                         r'(10\.)(\d*/)(\w*\.)(\w*)-(\w*)|(10\.)(\d*/)(\w*\.)(\w*)|(10\.)(\d*/)(\w*)-(\w*)|(10\.)(\d*/)-(\w*)|'
                                         r'(10\.)(\d*/)(\w*)|(10\.)(\d*%2F)(\w*)-(\d*)-(\d*)-(\w*)|(10\.)(\d*%2F)(\w*)'
                                         , u_geturl).group()
                        resultdict["doi"] = doi1
                        doi2 = doi1.replace('-', '')
                        """ # doi by api (too slow)
                        works = Works()
                        instance = works.doi(doi2)
                        if re.search(r'(/978)', doi2) != None:
                            try:
                                resultdict ["year"] = instance.get('created', {}).get('date-parts')[0][0]
                            except AttributeError:
                                resultdict["year"] = 9999
                        else:
                            try:
                                resultdict["year"] = instance.get('created', {}).get('date-parts')[0][0]
                                resultdict["uniqueIdentifier"] = instance.get('ISSN')[0].replace('-','')
                            except AttributeError:
                                resultdict["year"] = 9999
                                resultdict["uniqueIdentifier"] = None
                        """
                        try:
                            issn2 = re.search(r'(j\.|/s|/S|BF|PL|/|[)])(\d{8}|\d{7}\w{1})', doi2).group(2)
                            if re.search(r'(978[0-9]{5})', issn2) != None:
                                resultdict["issn"] = None
                            else:
                                resultdict["issn"] = issn2
                        except AttributeError:
                            resultdict["issn"] = None

                        try:
                            issn2 = re.search(r'(j\.|/)(\D{2,}|\D{2,}\d)(\.|:|)', doi2).group(2)
                            issn2 = issn2.lower()
                            resultdict["sTitleName"] = issn2.replace('.','')
                            try:
                                year1 = re.search(r'(/\D*\.)([1-2][0-9]{3})', doi2).group(2)
                                resultdict["year"] = year1
                            except AttributeError:
                                resultdict["year"] = 9999
                        except:
                            resultdict["sTitleName"] = None
                    except AttributeError:
                        resultdict["doi"] = None
                else:
                    resultdict["doi"] = None

                # with pii(sicence direct, elsevier) - need to look up
                if 'pii' in u_geturl:
                    try:
                        pii1 = re.search(r'(pii/|pii=)(\w*-\w\(\w\)\w-\w|\w*)', u_geturl).group(2)
                        resultdict["pii"] = pii1
                        issn3 = pii1.replace('-', '')
                        try:
                            issn3 = re.search(r'(S|s|)(\d{8}|\w{8})', issn3).group(2)
                            if re.search(r'(\AB978)', issn3) != None:
                                resultdict["issn"] = None
                            else:
                                resultdict["issn"] = issn3
                        except AttributeError:
                            resultdict["issn"] = None
                    except AttributeError:
                        resultdict["pii"] = None
                else:
                    resultdict["pii"] = None

                resultdict["uniqueIdentifier"] = (str(resultdict["issn"])+str(resultdict["isbn"])).replace("None",'')


                if any(x in gpath for x in other_usage):
                    resultdict["unidentifiable"] = 1
                else:
                    resultdict["unidentifiable"] = None

                # find if published year exists
                try:
                    year = re.search(r'(year=|year/|DT "|date=)([1-2][0-9]{3})', u_geturl).group(2)
                    resultdict["year"] = year
                except:
                    resultdict["year"] = 9999  # year unidentifiable

                httpreturncode = elements[8]
                resultdict["httpStatus"] = httpreturncode

                ## parse referring url
                if any(x in elements[10] for x in errortag):
                    print("problematic text")  # cause error if in url (e.g. non-string/blank)
                else:
                    u_refurl = unquote_plus(elements[10].strip('"'))  # convert into unicode strings if non-problematictext
                    p_refurl = urlparse(u_refurl)
                    u_refurl = u_refurl.replace('-', '')
                    resultdict["refUrl"] = u_refurl.rstrip()

                    # parse referring url's host: to identify accessed from where
                    try:
                        unescaped_ref_host = p_refurl.hostname.replace('.ezproxy.canterbury.ac.nz', '') # remove data doesn't contribute
                    except:
                        unescaped_ref_host = p_refurl.hostname
                    resultdict["refHost"] = unescaped_ref_host

                # convert http status code into int
                try:
                    httpreturncode_int = int(httpreturncode)
                except:
                    httpreturncode_int = 399

                ## check if a record is a unique action & db identifiable
                if resultdict["dbHost"] != None:
                    resultdict["category"] = 1
                elif resultdict ["dbCode"] != None:
                    resultdict["category"] = 2
                elif resultdict["issn"] != None:
                    resultdict["category"] = 3
                elif resultdict["isbn"] != None:
                    resultdict["category"] = 3
                elif resultdict["providerTitleId"] != None:
                    resultdict["category"] = 4
                elif resultdict["sTitleName"] != None:
                    resultdict["category"] = 5
                elif resultdict["doi"] != None:
                    resultdict["category"] = 6
                elif resultdict["pii"] != None:
                    resultdict["category"] = 6
                elif resultdict["unidentifiable"] != None:
                    resultdict["category"] = 7
                else:
                    resultdict["category"] = 0

                ## data removing
                writeout_line = writeout_code = writeout_ip = writeout_path = writeout_geturl = writeout_check = True

                if any(x in geturl for x in target_provider):  # for research purpose, should be removed when the project is further expanded
                    pass
                else:
                    writeout_line = False

                # exclude from scv file if http return code is 200. 2xx or 3xx ends up with 200. Error related code are also removed(4xx - 9xx).
                if httpreturncode_int in httpcode:
                    pass
                else:
                    writeout_code = False

                # exclude staffip from csv file
                if any(x in ipaddress for x in staffips):
                    writeout_ip = False

                # exclude geturl/refurl with any undesirable string
                if any(x in u_geturl for x in geturl_astring):
                    writeout_geturl = False
                    print(linenumber, "as_geturl")

                # show result and write in dictionary to be exported as csv
                if writeout_line and writeout_code and writeout_ip and writeout_path and writeout_geturl and writeout_check:
                    print(resultdict)
                    wr.writerow(resultdict)


                    if resultdict["providerCode"] == 'PRVESC':
                        #  insert into DB
                        """
                        db = mysql.connector.connect(
                            host="132.181.143.31",
                            database="UC_Lib_EZproxy_sandbox",
                            user="ywy16",
                            password="Princeps4464!",
                            auth_plugin="mysql_native_password")
                        """
                        db = mysql.connector.connect(
                            host="localhost",
                            database="UC_Lib_EZproxy_sandbox",
                            user="root",
                            password="Princeps4464!")

                        print(db)

                        db_cursor = db.cursor()
                        qry = "INSERT INTO ezproxyLogs (ipAddress, dateTime, userName, providerCode, comUrl, year, dbHost, dbCode, uniqueIdentifier, providerTitleId, sTitleName, refUrl) " \
                              "VALUES (%(ipAddress)s, %(dateTime)s, %(userName)s, %(providerCode)s, %(getUrl)s, %(year)s, %(dbHost)s, %(dbCode)s, %(uniqueIdentifier)s, %(providerTitleId)s, %(sTitleName)s, %(refUrl)s)"

                        try:
                            db_cursor.execute(qry, resultdict)
                            db.commit()
                            print("new entry")

                        except mysql.connector.errors.IntegrityError:
                            pass
                        except mysql.connector.errors.DatabaseError:
                            pass


                linenumber += 1
