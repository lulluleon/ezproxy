"""Parse EZproxy logfile and export results into csv"""

from urllib.parse import urlparse
from urllib.parse import unquote_plus
import glob
import csv
import tldextract
from datetime import date
import time

# Raw Log Files, check directory name
inputfiles = glob.glob('data/*.log')


# using date/time format to create output file names
date_string = str(date.today())  # '2017-12-26'
time_string = time.strftime("%H%M", time.localtime())
outputfile = 'output/'+date_string+"-"+time_string+'.csv'


# incomplete refurl tags causing error
errortag = ['132.181.', 'app:', 'blank', 'sogou', 'a:3:{s:2:', 'file:/']
#  ('(null)', "blank", "132.181")  # "6DAC0D5BDFC3D6CFEE151", "27D9F7D4CEBE234B32122", "6DAC0D5BDFC3D6CFEE202"

# use if excluding staffips are necessary
staffips = []

on_campusips = ['132.181']

# avoid strings
geturl_astring = ['rl/exams', '/connect', 'gr2tq4rz9x', 'rss', 'maf/app', '/becon', '/search', '/config', '/menu', '/logout',
                  '/localization/en-NZ', '/truesightedgemarker', '/ImportWidget', '/action/analytics', '/botdetectcaptcha',
                  '.png', '.css', 'woff', '.svg', '.jpg', 'jpeg', '.gif', '.f4v', '.ico', 'json', '.swf', 'tiff', '.tff', '.js', 'woff2', '.txt']


refurl_astring = ['exams']

# column names
fieldnames = ['ipaddress', 'username', 'on_campus', 'date','time', 'get_url', 'get_domain', 'get_host', 'get_path', 'get_query',
              'db_indicator_domain','db_indicator_issn', 'db_indicator_doi',
              'httpstatus', 'ref_url', 'ref_domain', 'ref_host', 'ref_path', 'ref_query'
             ]

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
                resultdict["get_host"] = p_geturl.hostname  # get hostname

                tld_url1 = tldextract.extract(p_geturl.hostname)
                resultdict["get_domain"] = tld_url1.domain
                resultdict["get_path"] = p_geturl.path
                resultdict["get_query"] = p_geturl.query

                httpreturncode = elements[8]
                resultdict["httpstatus"] = httpreturncode

                if len(elements[10])>3:
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

                        resultdict["ref_path"] = p_refurl.path
                        resultdict["ref_query"] = p_refurl.query

                        tld_url2 = tldextract.extract(p_refurl.hostname)  # extract component from parsed url (maybe no need)
                        resultdict["ref_domain"] = unquote_plus(tld_url2.domain)  #maybe no need

                    try:
                        httpreturncode_int = int(httpreturncode)
                    except:
                        httpreturncode_int = 399


                    # data cleansing
                    writeout_code = writeout_ip = writeout_referrer = writeout_geturl = writeout_refurl = True

                    # exclude from scv file if http return code is 200 (success)
                    if httpreturncode_int != 200:
                        writeout_code = False
                        print(linenumber, " http return code:", httpreturncode_int)

                    # exclude staffip from csv file
                    if any(x in ipaddress for x in staffips):
                        writeout_ip = False
                        print(linenumber, " staffip:", ipaddress)

                    """                    
                    # exclude if get domain and ref host is identical
                    if resultdict["get_domain"] in resultdict["ref_host"]:
                        writeout_referrer = False
                        print(linenumber, " get_domain ref_host same", resultdict["get_domain"], resultdict["ref_host"])
                    else:
                        writeout_referrer = True
                        print(linenumber, " get_domain ref_host diffr", resultdict["get_domain"], resultdict["ref_host"])
                    """

                    # exclude geturl/refurl with any undesirable string
                    if any(x in u_geturl for x in geturl_astring):
                        writeout_geturl = False
                        print(linenumber, "as_geturl")

                    u_refurl = unquote_plus(elements[10].strip('"'))
                    if any(x in u_refurl for x in refurl_astring):
                        writeout_refurl = False
                        print(linenumber, "as_refurl")

                    # show result and write in dictionary to be exported as csv
                    if writeout_code and writeout_ip and writeout_referrer and writeout_geturl and writeout_refurl:
                        print(resultdict)
                        wr.writerow(resultdict)

                linenumber += 1
