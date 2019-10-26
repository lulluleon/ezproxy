"""Parse EZproxy logfile and export results into csv """

from urllib.parse import urlparse
import urllib.request
import json
import glob
import csv
import hashlib
import tldextract
from datetime import date
import time
import html

# Raw Log Files, check directory name
inputfiles = glob.glob('data/*.log')


# using date/time format to create output file names
date_string = str(date.today())  # '2017-12-26'
time_string = time.strftime("%H%M", time.localtime())
outputfile = 'output/'+date_string+"-"+time_string+'.csv'


# staff ip addresses
staffips = ['132.181.185.57',	'132.181.185.71',	'132.181.202.47',	'132.181.223.197',	'132.181.223.198',	'132.181.224.101',
            '132.181.224.104',	'132.181.224.144',	'132.181.224.152',	'132.181.224.163',	'132.181.224.192',	'132.181.224.200',
            '132.181.224.207',	'132.181.224.30',	'132.181.224.38',	'132.181.224.43',	'132.181.224.72',	'132.181.224.83',
            '132.181.224.96',	'132.181.224.98',	'132.181.225.107',	'132.181.225.147',	'132.181.225.169',	'132.181.225.170',
            '132.181.225.174',	'132.181.225.194',	'132.181.225.206',	'132.181.225.210',	'132.181.225.23',	'132.181.225.50',
            '132.181.225.58',	'132.181.225.82',	'132.181.224.85',	'132.181.225.95',	'132.181.226.129',	'132.181.226.154',
            '132.181.226.162',	'132.181.226.163',	'132.181.226.165',	'132.181.226.173',	'132.181.226.182',	'132.181.226.183',
            '132.181.226.22',	'132.181.226.226',	'132.181.226.230',	'132.181.226.236',	'132.181.241.103',	'132.181.241.145',
            '132.181.241.179',	'132.181.241.198',	'132.181.241.201',	'132.181.241.205',	'132.181.241.208',	'132.181.241.209',
            '132.181.241.210',	'132.181.241.214',	'132.181.241.215',	'132.181.241.218',	'132.181.241.224',	'132.181.241.228',
            '132.181.241.229',	'132.181.241.234',	'132.181.241.235',	'132.181.241.237',	'132.181.241.239',	'132.181.32.100',
            '132.181.32.104',	'132.181.32.105',	'132.181.32.120',	'132.181.32.125',	'132.181.32.115',	'132.181.32.230',
            '132.181.32.223',	'132.181.32.202',	'132.181.32.206',	'132.181.32.183',	'132.181.32.143',	'132.181.32.156',
            '132.181.32.163',	'132.181.32.165',	'132.181.32.169',	'132.181.32.174',	'132.181.32.180',	'132.181.32.182',
            '132.181.32.186',	'132.181.32.190',	'132.181.32.192',	'132.181.32.193',	'132.181.32.196',	'132.181.32.197',
            '132.181.32.203',	'132.181.32.205',	'132.181.32.209',	'132.181.32.211',	'132.181.32.222',	'132.181.32.227',
            '132.181.32.229',	'132.181.32.236',	'132.181.32.237',	'132.181.32.63',	'132.181.32.79',	'132.181.32.83',
            '132.181.32.84',	'132.181.32.86',	'132.181.32.87',	'132.181.32.88',	'132.181.32.93',	'132.181.32.94',
            '132.181.4.230',	'132.181.32.128',	'132.181.32.134',	'132.181.225.186',	'132.181.225.249',	'132.181.226.201',
            '132.181.32.214',	'132.181.32.161',	'132.181.226.178',	'132.181.4.130',]

# list of filetypes to exclude
excludedfiletypes= ('.png', '.css', 'woff', '.svg', '.jpg', 'jpeg', '.gif', '.f4v', '.ico', 'json', '.swf', 'tiff', '.tff', 'js', 'woff2')


# problematic things in URLS to avoid in a tuple
exceptionstoavoid = ('app:', '(null)', "6DAC0D5BDFC3D6CFEE151", "27D9F7D4CEBE234B32122", "6DAC0D5BDFC3D6CFEE202", "blank", "132.181", "file:")

# exams url
examsurl = 'rl/exams'

#column names
fieldnames = ['ipaddress', 'country', 'city', 'zipcode', 'username', 'date','time', 'get_url', 'get_domain', 'get_host', 'get_tld', 'get_path', 'get_query',
             'httpstatus', 'ref_url', 'ref_domain', 'ref_host', 'ref_tld', 'ref_path', 'ref_query'
             ]

# open outputfile to write to
with open(outputfile, 'w', newline='') as outputs:
    wr = csv.DictWriter(outputs, fieldnames=fieldnames)
    wr.writeheader()
    print('EZproxy analysis beginning... This may take a few minutes.\n')  # check if csv file created

# read each raw log file
    for inputfile in inputfiles:
        linenumber = 1

        with open(inputfile) as logfile:
            resultdict = {fieldnames[i]: '' for i in range(0, len(fieldnames))}  # convert list into dictionary
            print("reading " + inputfile)  # check if files are all in analysis

            line = logfile.readline()

            while line:  # start splitting each line of the logfile up
                elements = line.split()  # split the line on spaces into elements list
                line = str(logfile.readline()).rstrip('\n')

                ipaddress = elements[0]  # define the first element as ipaddress and write value in resultdict
                """
                # get location information by using ipaddress
                resultdict["ipaddress"] = ipaddress

                api = 'http://api.ipstack.com/' + ipaddress + '?access_key=4059b1006b301eec0ac07d0fcb3b75b6'

                country = api + '&fields=country_code'
                r_country = urllib.request.urlopen(country).read()
                r_country = r_country.decode()
                r_country = json.loads(r_country)
                resultdict["country"] = r_country['country_code']

                city = api +'&fields=city'
                r_city = urllib.request.urlopen(city).read()
                r_city = r_city.decode()
                r_city = json.loads(r_city)
                resultdict["city"] = r_city['city']

                zip = api + '&fields=zip'
                r_zip = urllib.request.urlopen(zip).read()
                r_zip = r_zip.decode()
                r_zip = json.loads(r_zip)
                resultdict["zipcode"] = r_zip['zip']"""

                user = elements[2]
                if user != '-':  # hash the username so it is anonymised
                    hash_object = hashlib.md5(user.encode())
                    anon_user = hash_object.hexdigest()
                else:
                    anon_user = user  # if there isn't a username, call them 'user'
                #  print(anon_user)
                resultdict["username"] = anon_user  # write result in dictionary(result: hashed or '-')

                date = (elements[3] + elements[4]).strip('[]')  # date is in two elements, glue them together
                resultdict["date"] = date[1:11]
                resultdict["time"] = date[12:]

                fullgeturl1string = elements[6]  # full & parsed requesting url
                resultdict["get_url"] = html.unescape(fullgeturl1string)

                fullurl_1 = urlparse(elements[6])
                resultdict["get_host"] = fullurl_1.hostname

                tld_url1 = tldextract.extract(fullurl_1.hostname)
                resultdict["get_domain"] = tld_url1.domain
                resultdict["get_tld"] = tld_url1.suffix
                resultdict["get_path"] = fullurl_1.path
                resultdict["get_query"] = html.unescape(fullurl_1.query)

                httpreturncode = elements[8]
                resultdict["httpstatus"] = httpreturncode

                if len(elements[10]) > 3:
                    if any(problematictext in elements[10] for problematictext in exceptionstoavoid):
                        print("problematic text")
                    else:
                        fullurl_2 = (urlparse(elements[10].strip('"')))  # parse target url if no problematictext
                        escapedurl = html.unescape(elements[10].strip('"'))  # convert all numeric into unicode strings
                        resultdict["ref_url"] = escapedurl.rstrip()
                        try:
                            unescaped_ref_host = fullurl_2.hostname.replace('.ezproxy.canterbury.ac.nz', '')
                            # remove data that doesn't contribute to information (host)
                        except:
                            unescaped_ref_host = fullurl_2.hostname
                        resultdict["ref_host"] = html.unescape(unescaped_ref_host)

                        resultdict["ref_path"] = html.unescape(fullurl_2.path)
                        escapedquery = html.unescape(fullurl_2.query)
                        resultdict["ref_query"] = escapedquery.rstrip()
                        # print(fullurl_2)

                        tld_url2 = tldextract.extract(fullurl_2.hostname)  # extract component from parsed url
                        resultdict["ref_domain"] = html.unescape(tld_url2.domain)
                        resultdict["ref_tld"] = html.unescape(tld_url2.suffix)

                try:
                    httpreturncode_int = int(httpreturncode)
                except:
                    httpreturncode_int = 399  # authentication challenge failed


            # data cleansing
                writeout_code = writeout_exams = writeout_excludedfile = writeout_ip = writeout_referrer = True

                # exclude from scv file if http return code is between 200 and 399
                if 199 < httpreturncode_int < 400:
                    writeout_code = True
                else:
                    writeout_code = False
                    print(linenumber, " http return code:", httpreturncode_int)

                # exclude staffip from csv file
                if ipaddress in staffips:
                    writeout_ip = False
                    print(linenumber, " staffip:", ipaddress)

                # exclude if get domain and ref host is the same
                if resultdict["get_domain"] in resultdict["ref_host"]:
                    writeout_referrer = False
                    print(linenumber, " get_domain ref_host same", resultdict["get_domain"], resultdict["ref_host"])
                else:
                    writeout_referrer = True
                    print(linenumber, " get_domain ref_host diffr", resultdict["get_domain"], resultdict["ref_host"])

                # exclude if filetype is not desirable
                if fullgeturl1string.endswith(excludedfiletypes):
                    writeout_excludedfile = False
                    print(linenumber, " excluded file type")

                # exclude if requesting url relating to exam paper
                if examsurl in html.unescape(fullgeturl1string):
                    writeout_exams = False
                    print(linenumber, " exam paper", html.unescape(fullgeturl1string))

                # show result and write in dictionary to be exported as csv
                if writeout_code and writeout_exams and writeout_excludedfile and writeout_ip and writeout_referrer:
                    print(resultdict)
                    wr.writerow(resultdict)
                    # time.sleep(1)

                    linenumber += 1
