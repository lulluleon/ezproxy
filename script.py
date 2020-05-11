"""Parse EZproxy logfile and export results into csv and MySQL"""

from urllib.parse import urlparse, unquote_plus
from crossref.restful import Works
import glob
import csv
import re
import mysql.connector
import datetime
import time

# Raw Log Files ## check directory name
inputfiles = glob.glob('data/1/*.txt')

# using date/time format to create output file names
date_string = str(datetime.date.today())  # 'yyyy-mm-dd'
time_string = time.strftime("%H%M", time.localtime())
outputfile = 'output/'+date_string+"-"+time_string+'.csv'

# incomplete refurl tags causing error
errortag = ['132.181.', 'app:', 'sogou', 'a:3:{s:2:', 'file:/']

# exclude undesirabel IP Address ## include IP Address as a string, if required
staffips = []

# IP range 132.181.*.*: oncampus wired, IP range 202.36.178.* , 202.36.179.*
on_campusips = ['132.181', '202.36.178', '202.36.179', ]

# if in list, try search patterns
target_provider = ['springer', 'elsevier', 'sciencedirect', 'oxf', 'oed', 'oup', 'ebsco', 'jstor', 'hein', 'wiley',
                   'sage', 'proquest'] # able to remove when the project is expanded

dbidentification_host = ['sk.sage', 'methods.sage', 'thecochranelibrary', 'ulrichsweb',
                         'socialwork.oxfordre', 'oxfordartonline', 'oxfordbibliographies', 'oed.com',
                         '.onlinelibrary.wiley', 'birpublications.org', 'oxfordmusiconline',
                         'parlipapers', 'classics', 'bbcjournalism']

dbidentification_path = ['/artbibliographies', 'professional/australianeducationindex', 'professional/scb',
                         '/professional/', '/eebo', '/earthquake', '/eric', '/georef', '/georefinprocess',
                         '/healthcomplete', '/lion', '/materialsienceengineering', '/pqdt', '/hnpnewyorktimes',
                         '/ptsdpubs', '/sociologicalabstracts','/socabs', '/classics/', '/socialwork', ]

dbidentification_query = ['db=', 'collection=', 'oso:', ]

other_usage = ['/ehost/pdfviewer', '/eds/pdfviewer','/ehost/ebookviewer/ebook', '/ehost/ebookViewer/ebook', # ebscohost non-db identifiable logs
               '443/docview/', '433/pagepdf','/stable'] # proquest/jstor non-db identifiable logs with provideArticleId


name_indicator = ['publication', 'loi', 'toc', 'journal', 'book', 'series', 'productKey', 'stable', 'Book', 'reference',
                  'Reference', 'title', 'pdf']

issn_indicator = ['publication', 'loi', 'toc', 'issn', 'journal', '/', ]

httpcode = [200, 302]


# avoid strings
geturl_astring = ['.png', '.css', 'woff', '.svg', '.jpg', 'jpeg', '.gif', '.f4v', '.ico', 'json', '.swf', 'tiff',
                  '.tff', '.js', 'woff2', '.txt', '.sml', ':ping', '.ping',
                  'rl/exams', '/connect', 'gr2tq4rz9x', 'rss', 'maf/app', '/beacon', '/config', '/menu', '/logout',
                  '/login', '/Login', '/api', '/_default', '/action/', '/start', '/font', '/cookies', 'signup',
                  'navigator', 'countryLookup', '/stat', '/css', '/ImportWidget', '/botdetectcaptcha',
                  '/metadata/suggest', '/userimages', '/retrieve', '/checkout/cart', 'search/results', 'Search/Results',
                  '/localization', '/truesightedgemarker', '/sdfe/arp', '/Authentication', '/ExternalImage',
                  'volumesAndIssues/', 'wicket:', '/track/free', 'track/open', 'search?facet', 'citation-needed',
                  'citations.','app-pp', # springer
                  '/companylogo', '/signup-login', '/corporate-login', '/widget', '/change-location', '/usercss',
                  'opensearchxml', 'view-large', 'crawlprevention', 'siteToSearch', 'savecitation','/cite/',
                  'downloaddoclightbox','443/search','search-result','.layout','noresults','classics/search',
                  'groveart/search', 'oedlogin','crossreferencepopup', 'browsedictionary','hiddenform',
                  '/search/basic/hcppbasicsearch', '/grovemusic/browse', 'grovemusic/search',
                  '/result/pqpdocumentview:imgLinkUri','/result/pqpdocumentview:genericAjax',
                  'updatepreference','dictionarywordwheel', 'current/cover','academic/covers','system/images',
                  'result/pqpresultpage.gispdfhitspanel', 'result/pqpresultpage',#oxford
                  '/redirect', '_ajax', '/metaproducts/','.progressivedisplay','/citation', 'citeThisZone', '/results/',
                  'logrecommenderfeedback', 'index?accountid','advanced?accountid','sessionexpired', '/advanced/',
                  'citedreference','citedby','pdfdoctoolssection', 'expandedbasicsearchbox','markedlistcheckbox',# proquest
                  '/topics/','/SSOCore', '/science/link', 'export-citations', 'service.elsevier','_rest_magic/',
                  'smetrics.elsevier', '/record/pubMetDataAjax', '/onclick/', 'record/pubmetrics.uri', '/alert/',
                  'record/pubmetrixCitationsAjax.uri','/authid/','/sourceid/', '/source/',  # elsevier(sciencedirect/scopus)
                  '/HOL/Welcome', '/HOL/VMTPG', '/HOL/message', '/HOL/ajaxcalls', '/HOL/dummy', '/HOL/Citation',
                  '/HOL/PL', '/HOL/QRCode', '/HOL/SearchVolumeSOLR', '/LuceneSearch', '/HOL/ViewImageLocal',
                  '/HOL/insertBeta', '/HOL/PageDummy',  '/HOL/highlights', 'HOL/Index', '/SearchHistory',
                  '/SearchBuilder', '/HOL/ErrorCatch', '/HOL/NotSubscribed', '/HOL/NotAvailable', '/HOL/MarcXMLData',
                  'OneBoxCitation', 'SearchVolumeLucene', #  hein auto-generated logs
                  '/data/Get', 'Data/Get', '/Data/Search', '/Data/Populate', '/Books/GetRelatedChapters',
                  '/Books/ChapterMetadata', '/Books/Metadata', '/Books/BookContentTabs', '/Books/SearchChapters',
                  '/Search/Get', '/MethodsMap/GetMethodsMapRef', 'bundles', '/embeddedvideo', '/search/advanced',
                  ':80/search','reader/search', 'ExportCitation', 'CitationExport', 'reading-lists','BrowseMethods',
                  'data/LogVideoView', # sage
                  '/community/society-resources', '/sdfe/ears', '/readcube/log', '/ehost/result', '/track/controlled/',
                  '/Messages.properties', '/audio', '/GetIllustrations', '/GetResultListToc','CheckoutDialog',
                  '/LoadRecord', 'community.aspx','ehost/search', 'eds/results','addremovefolderitem','exportpanel',
                  'detail/imageQuickView', 'detail/detail', 'ehost/delivery','ehost/ebookdelivery', #ebsco
                  'bsigroup', 'oxcon.ouplaw', 'go.galegroup', 'safaribooks'] # keywords in this line should be included for complete DB analysis

non_dbCode = ['support', 'page', 'missing', 'products', 'book', 'app', 'moniker', 'na101', 'image', 'all']

non_title = ['a:', 'j.', 'j', 'bl', 'bf', '(issn)', '(sici)', 'B:', 'b:', 'acref', 'acprof', 'acprof:oso', 'oso',
             'pb', 'pl', 'issue', 'ThirdParty', 'Services', 'Citation', 'OUPMyAccount', 'EPIL', 'ORIL', 'MPECCOL',
             'supporthub', 'download', 'law:', '+', ':', '/', 'None'] # exclude from sTitleName

doiProvider = ['PRVAVX', 'PRVWIB', 'PRVSPB', 'PRVASL']

titleIdProvider = ['PRVAVX', 'PRVPQU', 'PRVJST']

jstor = ['jstor.org', 'jstororg' ]

# column names
fieldnames = ['ipAddress', 'userName', 'onCampus', 'dateTime', 'getUrl', 'getHost', 'getPath', 'getQuery', 'category',
              'providerCode', 'dbHost', 'dbCode', 'year', 'issn', 'isbn', 'uniqueIdentifier', 'sTitleName',
              'providerTitleId', 'providerArticleId', 'doi', 'pii', 'httpStatus', 'refUrl', 'refHost',]


# open outputfile to write to
with open(outputfile, 'w', newline='', encoding="utf-8") as outputs:
    wr = csv.DictWriter(outputs, fieldnames=fieldnames)
    wr.writeheader()
    print('EZproxy analysis beginning... This may take a few minutes.\n')  # check if output csv file created

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

                ## allocate providerName by string
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
                # parse hostname (sage, oxford, wiley & proQuest)
                if any(x in ghost for x in dbidentification_host):
                    resultdict["dbHost"] = ghost
                    if resultdict["dbHost"] == "www.onlinelibrary.wiley.com":
                        resultdict["dbHost"] = None
                    elif resultdict["dbHost"] == "public.oed.com":
                        resultdict["dbHost"] = None
                    else:
                        pass
                else:
                    resultdict["dbHost"] = None

                # parse dbcode (ebscohost, hein, proQuest)
                if any(x in gpath for x in dbidentification_path):
                    dbcode1 = re.search(r'(profiles/|/)(\w*)', gpath).group(2)
                    resultdict["dbCode"] = dbcode1
                elif any(x in gquery for x in dbidentification_query):
                    try:
                        dbcode2 = re.search(r'(db=|collection=|oso:)(\w*)(/|)', gquery).group(2)
                        resultdict["dbCode"] = dbcode2
                    except AttributeError:
                        resultdict["dbCode"] = None
                elif "eid=2-s" in gquery:
                    resultdict["dbCode"] = "scopus"
                elif "proquest" in ghost:
                    try:
                        dbcode3 = re.search(r'(\w*)(/docview/|/pagepdf/)', gpath).group(1)
                        dbcode3 = dbcode3.replace('/', '')
                        resultdict["dbCode"] = dbcode3
                    except AttributeError:
                        resultdict["dbCode"] = None
                else:
                    resultdict["dbCode"] = None

                # remove if a string is undesirable for the field
                for i in range(0, len(non_dbCode)):
                    if resultdict["dbCode"] == non_dbCode[i]:
                        resultdict["dbCode"] = None

                # mark if logs are disable to identify db
                if any(x in u_geturl for x in other_usage):
                    if resultdict["providerCode"] == 'PRVEBS':
                        resultdict["dbCode"] = "XXXEBS"
                    elif resultdict["providerCode"] == 'PRVPQU':
                        resultdict["dbCode"] = "NXXPQU"
                    elif resultdict["providerCode"] == 'PRVJST':
                        resultdict["dbCode"] = "NXXJST"
                    else:
                        resultdict["dbCode"] = None
                else:
                    pass

                # parse title name(short title name) or providerId(springer, proQuest)
                if any(x in gpath for x in name_indicator):
                    try:
                        journal1 = re.search(r'(/loi/|/toc/|\.journals/|/journal/|/book/|/books/|/Book/'
                                             r'|/reference/|/Reference//)(\D{2,}\d*)(/|\?|)', gpath).group(2)
                        journal1 = journal1.replace('-', '')
                        try:
                            journal1=journal1.split('/')[0]
                            resultdict["sTitleName"] = journal1
                        except AttributeError:
                            resultdict["sTitleName"] = journal1
                    except AttributeError:
                        resultdict["sTitleName"] = None
                elif 'journalCode=' in gquery:
                    try:
                        journal4 = re.search(r'(journalCode=)(\w{3})', gquery).group(2)
                        resultdict["sTitleName"] = journal4
                    except AttributeError:
                        resultdict["sTitleName"] = None
                elif 'title' in gquery:
                    try:
                        journal2 = re.search (r'(title=)(\D{2,}|\D{2,}\d*)(&date)', gquery).group(2)
                        journal2 = journal2.replace(' ', '')
                        journal2 = journal2.lower()
                        resultdict["sTitleName"] = journal2
                    except AttributeError:
                        resultdict["sTitleName"] = None

                elif 'academic.oup.com' in ghost:
                    try:
                        journal3 = re.search(r'(/\w*)(/article/|/article-abstract/|/issue/|/)', gpath).group(1)
                        journal3 = journal3.replace('/', '')
                        resultdict["sTitleName"] = journal3
                    except AttributeError:
                        resultdict["sTitleName"] = None
                else:
                    resultdict["sTitleName"] = None

                if any(x in str(resultdict["providerCode"]) for x in titleIdProvider):
                    if any(x in gpath for x in name_indicator):
                        try:
                            providerid1 = re.search(r'(productKey=|/journal/|/stable/pdf/|/pagepdf/|/stable/'
                                                    r'|t:ac=|publication/|publications|theBookId=)(\d{1,}\b)', gpath).group(2)
                            resultdict["providerTitleId"] = providerid1
                        except AttributeError:
                            resultdict["providerTitleId"] = None
                    elif any(x in gquery for x in name_indicator):
                        try:
                            providerid2 = re.search(r'(productKey=|theBookId=|t:ac=)(\d{1,})', gquery).group(2)
                            resultdict["providerTitleId"] = providerid2
                        except AttributeError:
                            resultdict["providerTitleId"] = None
                    else:
                        resultdict["providerTitleId"] = None

                if 'PRVPQU' in str(resultdict["providerCode"]):
                    try:
                        providerid3 = re.search(r'(docview/)(\d{1,}\b)',gpath).group(2)
                        resultdict["providerArticleId"] = providerid3
                    except AttributeError:
                        resultdict["providerArticleId"] = None


                # parse isbn
                if '978' in u_geturl:
                    try:
                        isbn1 = re.search(r'(978)-(\d*)-(\d*)-(\d*)-(\d*)'
                                          r'|(978)-(\d*)-(\d*)-(\d*)|(978)(\d{10})', u_geturl).group()
                        try:
                            isbn1 = isbn1.replace('-', '')
                            resultdict["isbn"] = isbn1
                        except:
                            resultdict["isbn"] = isbn1
                    except AttributeError:
                        resultdict["isbn"] = None
                else:
                    resultdict["isbn"] = None

                # parse issn
                if any(x in u_geturl for x in issn_indicator):
                    try:
                        issn1 = re.search(r'(/publication/|publication=|/loi/|/loi/S|/loi/s|/toc/|/toc/S|/toc/s'
                                          r'|issn\.|issn/|issn=|journal:|/journal/|/\w*/\w*/\d*/\d*/\d*/|/)(\d{4}-\w*'
                                          r'|\d{8}\b|\d{6}\w{2}\b|\d{8}\w{1})', u_geturl).group(2)
                        issn1 = issn1.replace('-', '')
                        issn1 = issn1.replace('a', '')
                        issn1 = issn1.replace('b', '')
                        issn1 = issn1.replace('c', '')
                        if re.search(r'(978[0-9]{5})', issn1) != None:
                            resultdict["issn"] = None
                        elif "proquest" in geturl:
                            resultdict["issn"] = None
                        else:
                            resultdict["issn"] = issn1
                    except AttributeError:
                        resultdict["issn"] = None
                else:
                    resultdict["issn"] = None

                # parse doi
                if '10.' in u_geturl:
                    try:
                        doi1 = re.search(r'(10\.)(\d*/)(\w*\.)(\w*\.)(\w*\.)(\w*\.)(\w*)'
                                         r'|(10\.)(\d*/)(\w*\.)(\w*\.)(\w*\.)(\w*)|'
                                         r'(10\.)(\d*/)(\w*\.)(\w*\.)(\w*)|(10\.)(\d*/)(\(\w*\))(\d*)-(\d{4})'
                                         r'|(10\.)(\d*/)(\(\w*\))(\d*)-(\w*)|'
                                         r'(10\.)(\d*/)(\w*:\W*/)(\w*)|(10\.)(\d*/)(\w*:)(\w*)'
                                         r'|(10\.)(\d*/)(\w*)-(\w*)-(\w*)-(\w*)-(\w*)|'
                                         r'(10\.)(\d*/)(\w*)-(\w*)-(\w*)-(\w*)|(10\.)(\d*/)(\w*\.)(\w*)-(\w*)-(\w*)-(\w*)|'
                                         r'(10\.)(\d*/)(\w*\.)(\w*)-(\w*)|(10\.)(\d*/)(\w*\.)(\w*)'
                                         r'|(10\.)(\d*/)(\w*)-(\w*)|(10\.)(\d*/)(\w*/)(\w*)|(10\.)(\d*/)(\w*)(\(\w*\))(\w*)|'
                                         r'(10\.)(\d*/)(\w*)|(10\.)(\d*%2F)(\w*)-(\w*)-(\w*)-(\w*)|(10\.)(\d*%2F)(\w*)'
                                         , u_geturl).group()
                        resultdict["doi"] = doi1
                        doi1 = doi1.replace('.pdf', '')
                        doi2 = doi1.replace('-', '')


                        try:
                            issn2 = re.search(r'(J\.|j\.|/|[)])(\b\d{8}\b|\d{7}\w{1}\b)', doi2).group(2)
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

                # parse pii
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


                # find if published year exists, otherwise allocate '9999'
                try:
                    year = re.search(r'(year=|year/|DT "|date=)([1-2][0-9]{3})', u_geturl).group(2)
                    resultdict["year"] = year
                except:
                    resultdict["year"] = 9999  # year unidentifiable

                # httpreturn code and convert http status code into int
                try:
                    httpreturncode = elements[8]
                    resultdict["httpStatus"] = httpreturncode
                    try:
                        httpreturncode_int = int(httpreturncode)
                    except:
                        httpreturncode_int = 399
                except IndexError:
                    pass



                ## parse referring url
                try:

                    if any(x in elements[10] for x in errortag):
                        print("problematic text")  # cause error if in url (e.g. non-string/blank)
                    else:
                        refurl = elements[10].strip('"')  # full requesting url
                        u_refurl = unquote_plus(refurl) # convert into unicode strings if non-problematictext
                        u_refurl = u_refurl.replace("-", "")
                        p_refurl = urlparse(u_refurl)
                        resultdict["refUrl"] = u_refurl

                        try:
                            unescaped_ref_host = p_refurl.hostname.replace('.ezproxy.canterbury.ac.nz', '') # remove data doesn't contribute
                        except:
                            unescaped_ref_host = p_refurl.hostname
                            resultdict["refHost"] = unescaped_ref_host
                except IndexError:
                    pass

                # JSTOR usage only appear in refURL
                if any (x in refurl for x in jstor):
                    resultdict["providerCode"] = 'PRVJST'
                    try:
                        providerArticleId1 = re.search(r'(stable/|stable%2f)(\d{1,})', refurl).group(2)
                        resultdict["providerArticleId"] = providerArticleId1
                    except AttributeError:
                        pass
                    try:
                        providerTitleId1 = re.search(r'(stable/resrep/|stable%2fresrep)(\d{1,8})', refurl).group(2)
                        resultdict["providerTitleId"] = providerTitleId1
                    except AttributeError:
                        pass
                    try:
                        doi3 = re.search(r'(10\.)(\d*/)(\w*)|(10\.)(\d*%2F)(\w*)', refurl).group()
                        resultdict["doi"] = doi3
                    except AttributeError:
                        pass
                    try:
                        issn4 = re.search(r'(issn=)(\d{4}-\w*|\d{8}\b|\d{6}\w{2}\b|\d{8}\w{1})', refurl).group(2)
                        issn4 = issn4.replace('-', '')
                        resultdict["issn"] = issn4
                    except AttributeError:
                        pass
                    try:
                        sTitleName1 = re.search(r'(journal/)(\w*)', refurl).group(2)
                        resultdict["sTitleName"] = sTitleName1
                    except AttributeError:
                        pass
                else:
                    pass


                # combine issn and isbn as unique identifier
                resultdict["uniqueIdentifier"] = (str(resultdict["issn"])+str(resultdict["isbn"])).replace("None", "").lstrip("0")


                # standardising sTitleName

                # remove if a string is undesirable for the field
                for i in range(0, len(non_title)):
                    if any(x in str(resultdict["sTitleName"]) for x in non_title[i]):
                        try:
                           resultdict["sTitleName"] = str(resultdict["sTitleName"]).replace(non_title[i], "")
                        except AttributeError:
                            pass
                    else:
                        pass

                if ((resultdict["providerCode"] == "PRVSPB") and resultdict["sTitleName"] != None):
                    if (len(resultdict["sTitleName"]) == 4):
                        try:
                            sTitleName2 = str(resultdict["sTitleName"])[:-1]
                            resultdict["sTitleName"] = sTitleName2
                        except TypeError:
                            pass
                    else:
                        pass
                else:
                    pass


                # find ISSN/ISBN through DOI
                if resultdict["doi"] != None:
                    try:
                        resultdict["doi"] =str(resultdict["doi"]).replace(".pdf", "")
                        # doi by api, slow / a separated script is also created
                        if any(x in str(resultdict["providerCode"]) for x in doiProvider):
                            if re.search(r'(/978)', doi1) != None:
                                resultdict["year"] = 9999
                            else:
                                works = Works()
                                instance = works.doi(doi1)
                                try:
                                    resultdict["year"] = instance.get('created', {}).get('date-parts')[0][0]
                                    resultdict["issn"] = instance.get('ISSN')[0].replace('-', '')
                                except TypeError:
                                    try:
                                        resultdict["year"] = instance.get('created', {}).get('date-parts')[0][0]
                                        resultdict["isbn"] = instance.get('ISBN')[0].replace('-', '')
                                    except AttributeError:
                                        resultdict["year"] = 9999
                                        resultdict["issn"] = None
                                except AttributeError:
                                    resultdict["year"] = 9999
                                    resultdict["issn"] = None
                        else:
                            pass
                    except AttributeError:
                        pass
                else:
                    pass


                # convert empty string to null value
                for i in range(0, len(fieldnames)):
                    if resultdict[(fieldnames[i])] == "":
                        resultdict[(fieldnames[i])] = None

                ## check if a record is a unique action & db identifiable
                if resultdict["providerCode"] != None:
                    if resultdict["dbHost"] !=None:
                        if gpath != "/":
                            resultdict["category"] = 1
                        else:
                            resultdict["category"] = 0
                    elif resultdict["dbCode"] or resultdict["uniqueIdentifier"] or resultdict["sTitleName"] \
                            or resultdict["providerTitleId"] or resultdict["doi"] or resultdict["pii"]!= None:
                        resultdict["category"] = 1
                    else:
                        resultdict["category"] = 0
                else:
                    resultdict["category"] = 0


                ## data removing
                writeout_code = writeout_ip = writeout_geturl = True


                # exclude from scv file if http return code is 200. 2xx or 3xx ends up with 200.
                # Error related code are also removed(4xx - 9xx).
                if httpreturncode_int in httpcode:
                    if resultdict["providerCode"] != "PRVJST":
                        if httpreturncode_int != 200:
                            writeout_code = False
                        elif "login":
                            pass
                    else:
                        pass
                else:
                    writeout_code = False

                # exclude staffip from csv file
                if any(x in ipaddress for x in staffips):
                    writeout_ip = False

                # exclude geturl/refurl with any undesirable string
                if any(x in u_geturl for x in geturl_astring):
                    if resultdict["providerCode"] != "PRVJST":
                        writeout_geturl = False
                    else:
                        pass
                else:
                    pass
                    # print(linenumber, "as_geturl")

                # show result and write in dictionary to be exported as csv
                if writeout_code and writeout_ip and writeout_geturl:
                    print(resultdict)
                    wr.writerow(resultdict)

                    if resultdict["category"] > 0:
                        #  insert into DB

                        db = mysql.connector.connect(
                            host="[hostname]",
                            database="[databasename]",
                            user="[username]",
                            password="[password]",
                            auth_plugin="mysql_native_password")

                        print(db)

                        db_cursor = db.cursor()
                        qry = "INSERT INTO ezproxyLogs2 (ipAddress, dateTime, userName, onCampus, providerCode, comUrl, " \
                              "year, dbHost, dbCode, uniqueIdentifier, providerTitleId, providerArticleId, sTitleName, " \
                              "doi, pii, refUrl) " \
                              "VALUES (%(ipAddress)s, %(dateTime)s, %(userName)s, %(onCampus)s, %(providerCode)s, " \
                              "%(getUrl)s, %(year)s, %(dbHost)s, %(dbCode)s, %(uniqueIdentifier)s, %(providerTitleId)s, " \
                              "%(providerArticleId)s, %(sTitleName)s, %(doi)s, %(pii)s, %(refUrl)s)"

                        try:
                            db_cursor.execute(qry, resultdict)
                            db.commit()
                            print(linenumber, "new entry")

                        except mysql.connector.errors.IntegrityError:
                            pass
                        except mysql.connector.errors.DatabaseError:
                            pass

                linenumber += 1
