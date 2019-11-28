"""Parse EZproxy logfile and export results into csv"""

from urllib.parse import urlparse, unquote_plus, parse_qs
import glob
import csv
import tldextract
import re
from datetime import date
import time

# Raw Log Files, check directory name
inputfiles = glob.glob('data/ezp201924.log')

# using date/time format to create output file names
date_string = str(date.today())  # '2017-12-26'
time_string = time.strftime("%H%M", time.localtime())
outputfile = 'output/'+date_string+"-"+time_string+'.csv'

# incomplete refurl tags causing error
errortag = ['132.181.', 'app:', 'sogou', 'a:3:{s:2:', 'file:/']
#  original script: 'app:', '(null)', "blank", "132.181", "file:", "6DAC0D5BDFC3D6CFEE151", "27D9F7D4CEBE234B32122", "6DAC0D5BDFC3D6CFEE202"

# exclude staffip if necessary
staffips = []

on_campusips = ['132.181', '202.36.178', '202.36.179', ]  # IP range 132.181.*.*: oncampus wired, IP range 202.36.178.* , 202.36.179.*

# if in list, try search patterns
target_provider = ['springer', 'elsevier', 'sciencedirect', 'oxf', 'oed', 'oup', 'ebsco', 'jstor', 'hein', 'wiley', 'sage']

dbidentification_host = ['sage', 'thecochranelibrary', 'onlinelibrary.wiley', 'ebookcentral.proquest', 'parlipapers.proquest', 'ulrichsweb.serialssolutions',
                        'bbcjournalism', 'socialwork.oxfordre', 'opil.ouplaw', 'oxfordartonline', 'oxfordbibliographies',
                        'classics.oxfordre', 'oxfordhandbooks', 'academic.oup', 'oaj.oxfordjournals', 'oxfordmusiconline', 'oxfordreference', 'oxfordre.com', 'oxfordscholarship.com',]

dbidentification_path = ['/artbibliographies', 'professional/australianeducationindex', 'professional/scb', '/professional/', '/eebo', '/earthquake', '/eric',
                         '/georef', '/georefinprocess', '/healthcomplete', '/lion', '/materialsienceengineering', '/pqdt', '/hnpnewyorktimes', '/ptsdpubs',
                         '/sociologicalabstracts','/socabs', '/classics/', '/conference',  # proquest path indicator
                         '/HOL/Page', '/HOL/PrintRequest', '/HOL/OneBoxCitation', '/HOL/SelectPage', # hein path indicator
                         '/plink', '/ehost/pdfviewer', '/eds/pdfviewer','/ehost/ebookviewer', '/EbscoViewerService/ebook', # ebsco indicator
                        ]
dbindentification_query = ['db=', 'collection=', 'oso:']

name_indicator = ['publications_', '/loi', '/toc/', 'journal', 'book', 'series', 'productKey', '/stable/', '/docview/', '/book/', '/Book/', '/reference/', '/Reference', '/home']

issn_indicator = ['/publication', '/loi', '/toc/', 'issn', 'journal', '/', ]

httpcode = [200, ]


# avoid strings
geturl_astring = ['rl/exams', '/connect', 'gr2tq4rz9x', 'rss', 'maf/app', '/beacon', '/config', '/menu', '/logout', '/login', '/Login', '/api', '/_default', '/action/',
                  '/font', '/cookies', '/ImportWidget', '/botdetectcaptcha', '/metadata/suggest', '/userimages', '/retrieve', '/checkout/cart',
                  '/localization', '/truesightedgemarker', '/sdfe/arp', '/Authentication', '/ExternalImage', '/stat', '/css', 'volumesAndIssues/', 'wicket:', # springer auto-generated
                  '/companylogo', '/signup-login', '/corporate-login', '/widget', '/change-location',
                  '/topics/annotations', '/SSOCore', '/science/link', #  elsevier(sciencedirect) auto-generated
                  '/record/pubMetDataAjax.uri', '/record/recordPageLinks.uri', '/record/references.uri', '/home.uri', '/redirect/linking.uri', '/cto2/getdetails.uri',#  scopus auto-generated
                  '/HOL/Welcome', '/HOL/VMTPG', '/HOL/message', '/HOL/ajaxcalls', '/HOL/dummy', '/HOL/Citation', '/HOL/PL', '/HOL/P', '/HOL/QRCode', '/HOL/SearchVolumeSOLR',
                  '/HOL/ViewImageLocal', '/HOL/insertBeta', '/HOL/PageDummy',  '/HOL/highlights', '/HOL/PDFsearchable', '/HOL/Print', '/HOL/ErrorCatch', '/HOL/NotSubscribed', '/HOL/NotAvailable', #  hein auto-generated logs
                  '/data/Get','Data/Get', '/Books/GetRelatedChapters', '/Books/ChapterMetadata', '/Books/Metadata', '/Books/BookContentTabs', '/Search/GetTypeAheadValues', '/Search/Results',
                  '/MethodsMap/GetMethodsMapRef', 'bundles', '/embeddedvideo', #  sage auto-generated
                  '/community/society-resources', '/sdfe/ears', '/readcube/log', '/ehost/result', '/track/controlled/', '/Messages.properties',
                  '.png', '.css', 'woff', '.svg', '.jpg', 'jpeg', '.gif', '.f4v', '.ico', 'json', '.swf', 'tiff', '.tff', '.js', 'woff2', '.txt', '.sml', ':ping']

# column names
fieldnames = ['ipaddress', 'username', 'on_campus', 'date','time', 'get_url', 'get_host', 'get_path', 'get_query', 'check',
              'db_indicator_host', 'db_indicator_path', 'db_indicator_code', 'titlename', 'providerid', 'isbn', 'issn', 'doi', 'pii', 'year',
              'httpstatus', 'ref_url', 'ref_host', 'ref_path', 'ref_query', ]

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
            print("reading " + inputfile)  # check if files are all in analysis
            time.sleep(1)

            line = logfile.readline()

            while line:  # start splitting each line of the logfile up
                elements = line.split()  # split the line on spaces into elements list
                line = str(logfile.readline()).rstrip('\n')

                ipaddress = elements[0]  # define the first element as ipaddress and write value in resultdict
                resultdict["ipaddress"] = ipaddress

                if any(x in ipaddress for x in on_campusips):
                    resultdict['on_campus'] = "Yes"
                else:
                    resultdict['on_campus'] = "No"

                user = elements[2]
                resultdict["username"] = user  # user

                date = (elements[3] + elements[4]).strip('[]')  # date is in two elements, glue them together
                resultdict["date"] = date[0:11]
                resultdict["time"] = date[12:20]  # UTC time

                geturl = elements[6]  # full requesting url
                u_geturl = unquote_plus(geturl)
                resultdict["get_url"] = u_geturl

                p_geturl = urlparse(u_geturl)  # parse requesting url
                ghost = p_geturl.hostname
                resultdict["get_host"] = ghost  # get hostname

                tld_url1 = tldextract.extract(p_geturl.hostname)

                gpath = p_geturl.path
                resultdict["get_path"] = gpath
                gquery = p_geturl.query
                resultdict["get_query"] = gquery

                # find db indicator by host, path, and query
                # with hostname (sage, oxford, some wiley & proquest)
                if any(x in ghost for x in dbidentification_host):
                    resultdict["db_indicator_host"] = ghost
                else:
                    resultdict["db_indicator_host"] = None

                # with path (proquest, hein)
                if any(x in gpath for x in dbidentification_path):
                    gpath1 = re.search(r'(/\w*/\w*|/\w*)', gpath).group()
                    resultdict["db_indicator_path"] = gpath1
                else:
                    resultdict["db_indicator_path"] = None

                # with journalname(shortname included) or provider id - need to look up from other dataset
                if isinstance(u_geturl, str):
                    if any(x in u_geturl for x in name_indicator):
                        try:
                            journal1 =  re.search(r'(/loi/|/toc/|/journal/|/book/|/books/|/Book/|/reference/|/Reference//|/home/)(\D*)(/|\?|)', gpath).group(2)
                            journal1 = journal1.replace('-', '')
                            try:
                                journal1=journal1.split('/')[0]
                                resultdict["titlename"] = journal1
                            except AttributeError:
                                resultdict["titlename"] = journal1
                        except AttributeError:
                            resultdict["titlename"] = None
                        try:
                            providerid1 = re.search(r'(/bookseries/|/series/|productKey=|/journal/|/stable/pdf/|/stable/|/docview/|publications_)(\d{1,5})', u_geturl).group(2)
                            resultdict["providerid"] = providerid1
                        except AttributeError:
                                resultdict["providerid"] = None
                    else:
                        resultdict["titlename"] = None
                        resultdict["providerid"] = None

                    # with isbn (some oxford) - need to look up from other dataset (e.g. titlelist)
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
                    else:
                        resultdict["isbn"] = None

                    # with issn - need to look up from other dataset (e.g. titlelist)
                    if any(x in u_geturl for x in issn_indicator):
                        try:
                            issn1 = re.search(r'(/publication/|publication=|/loi/|/toc/|issn\.|issn/|issn=|journal:|/journal/|/)(\d{4}-\w*|\d{8}|\d{6}\w{2})', u_geturl).group(2)
                            try:
                                issn1 = issn1.replace('-', '')
                                resultdict["issn"] = issn1
                            except:
                                resultdict["issn"] = issn1
                        except AttributeError:
                            resultdict["issn"] = None
                    else:
                        resultdict["issn"] = None

                    #  with doi - need to look up from other dataset
                    if '10.' in u_geturl:
                        try:
                            doi1 = re.search(r'(10\.)(\d*/)(\w*\.)(\w*\.)(\d*\.)(\d*\.)(\w*)|(10\.)(\d*/)(\w*\.)(\w*\.)(\d*\.)(\w*)|'
                                             r'(10\.)(\d*/)(\w*\.)(\w*\.)(\w*)|(10\.)(\d*/)(\(\w*\))(\d*)-(\d*)|(10\.)(\d*/)(\w*)-(\d*)-(\d*)-(\w*)|(10\.)(\d*/)(\w*\.)(\w*)-(\w*)-(\d*)-(\w*)|'
                                             r'(10\.)(\d*/)(\w*\.)(\w*)-(\w*)|(10\.)(\d*/)(\w*\.)(\w*)|(10\.)(\d*/)(\w*)-(\w*)|(10\.)(\d*/)-(\w*)|(10\.)(\d*)(\w*:)(\w*)|(10\.)(\d*/)(\w*)|(10\.)(\d*%2F)(\w*)-(\d*)-(\d*)-(\w*)'
                                             , u_geturl).group()
                            resultdict["doi"] = doi1
                            issn2 = doi1.replace('-', '')
                            try:
                                issn2 = re.search(r'(j\.|/s|/S|/BF|/|[)])(\d{8}|\d{7}\w{1})', issn2).group(2)
                                if re.search(r'(\A978)', issn2) != None:
                                    resultdict["issn"] = None
                                else:
                                    resultdict["issn"] = issn2
                            except:
                                resultdict["issn"] = None
                            try:
                                issn2 = re.search(r'(/)(\D*|\D{1,}\d)(\.)', issn2).group(2)
                                resultdict["titlename"] = issn2
                            except:
                                resultdict["titlename"] = None
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


                # find if published year exists
                try:
                    year = re.search(r'([1][8-9][0-9]{2}|[2][0][0-3][0-9])', gquery).group()  # find published year 1800-2039
                    resultdict["year"] = year
                except:
                    resultdict["year"] = None

                # with dbcode indicator (ebscohost, hein) - need to look up (list to be created)
                if isinstance(gquery, str):
                    if any(x in gquery for x in dbindentification_query):
                        dbcode1 = re.search(r'(db=|collection=|oso:)(\w*)', gquery).group(2)
                        resultdict["db_indicator_code"] = dbcode1
                    elif "eid=2-s" in gquery:
                        resultdict["db_indicator_code"] = "SCOPUS"
                    else:
                        resultdict["db_indicator_code"] = None

                httpreturncode = elements[8]
                resultdict["httpstatus"] = httpreturncode

                # if len(elements[10]) > 3:
                # remove line that cause error due to incomplete referring url
                if any(x in elements[10] for x in errortag):
                    print("problematic text")
                else:
                    u_refurl = unquote_plus(elements[10].strip('"'))  # convert into unicode strings if non-problematictext
                    p_refurl = urlparse(u_refurl)
                    resultdict["ref_url"] = u_refurl.rstrip()

                    try:
                        unescaped_ref_host = p_refurl.hostname.replace('.ezproxy.canterbury.ac.nz', '')
                        # remove data that doesn't contribute to information (host)
                    except:
                        unescaped_ref_host = p_refurl.hostname

                    resultdict["ref_host"] = unescaped_ref_host

                    rpath = p_refurl.path
                    resultdict["ref_path"] = rpath
                    rquery = p_refurl.query
                    resultdict["ref_query"] = rquery

                try:
                    httpreturncode_int = int(httpreturncode)
                except:
                    httpreturncode_int = 399


                if resultdict["titlename"] or resultdict["providerid"] or resultdict["isbn"] or resultdict["issn"] or resultdict["doi"] or resultdict["pii"]:
                    resultdict["check"] = 2
                elif resultdict["db_indicator_host"] or resultdict["db_indicator_path"] or resultdict ["db_indicator_code"]:
                    resultdict["check"] = 1
                else:
                    resultdict["check"] = 0

                    # data cleansing
                writeout_line = writeout_code = writeout_ip = writeout_path = writeout_geturl = writeout_check = True

                if any(x in geturl for x in target_provider):
                    pass
                else:
                    writeout_line = False

                # exclude from scv file if http return code is 200
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

                """if resultdict["check"] < 1:
                    writeout_check = False"""

                # show result and write in dictionary to be exported as csv
                if writeout_line and writeout_code and writeout_ip and writeout_path and writeout_geturl and writeout_check:
                    print(resultdict)
                    wr.writerow(resultdict)

                linenumber += 1
