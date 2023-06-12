import urllib.request, urllib.error, urllib.parse
import ssl
# import json
import sqlite3
from urlformatter import urlformat
from bs4 import BeautifulSoup
import re

# Ignore ssl certification errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

con = sqlite3.connect('urldatalake.db')
cur = con.cursor()

cur.executescript('''
    CREATE TABLE IF NOT EXISTS masterurls(id INTEGER PRIMARY KEY, url TEXT UNIQUE, name TEXT, html TEXT, error INTEGER);
    CREATE TABLE IF NOT EXISTS extractedurls(id INTEGER PRIMARY KEY, extractedfrom TEXT, url TEXT UNIQUE, name TEXT, timesappeared INTEGER)
''')

count = input('How many urls would you like to search today? ')
times = int(count)
html = None
while times > 0:
    starturl = input('Enter a url you want to parse (only authorized websites): ')
    web = urlformat(starturl)
    if len(web) > 1:
        cur.execute('INSERT OR IGNORE INTO masterurls (url) VALUES (?)', (web,))
        cur.execute('INSERT OR IGNORE INTO extractedurls (extractedfrom) VALUES (?)', (web,))
        con.commit()
    try:
        document = urllib.request.urlopen(web, context=ctx)
        html = document.read()
        if document.getcode() != 200:
            print('Error retrieving this url: ', document.getcode())
            cur.execute('UPDATE masterurls SET error = ? WHERE url = ?', (document.getcode(), web))
            con.commit()
        cur.execute('UPDATE masterurls SET html = ? WHERE url = ?', (memoryview(html), web))
        con.commit()

        soup = BeautifulSoup(html, 'html.parser')
        tags = soup('a')
        count = 0
        hreflist = []
        hreflistcount = dict()
        for tag in tags:
            href = tag.get('href', None)
            if href is None:
                continue
            uparse = urllib.parse.urlparse(href)
            if (len(uparse.scheme)) < 1:
                href = urllib.parse.urljoin(web, href)
            pos = href.rfind('#')
            if pos > 1:
                href = href[:pos]

            if href not in hreflist:
                hreflist.append(href)
            cur.execute('INSERT OR IGNORE INTO extractedurls (url) VALUES (?)', (href,))
            cur.execute('UPDATE extractedurls SET extractedfrom = ? WHERE url = ?', (web, href,))
            con.commit()
        print(hreflist)
        # Counts the number of times the urls have appeared in the html
        for h in hreflist:
            hreflistcount[h] = hreflistcount.get(h, 0) + 1

        for k, v in hreflistcount.items():
            cur.execute('UPDATE extractedurls SET timesappeared = ? WHERE url = ?', (v, k,))
            con.commit()
    except KeyboardInterrupt:
        print('Program interrupted by user')

    except Exception as e:
        print('Unable to retrieve', web)
        print('Error', e)
        cur.execute('UPDATE masterurls SET error = -1 WHERE url = ?', (web,))
        con.commit()
    except:  # I need to figure out a way here to not use bare except.
        print('Unable to retrieve or parse page, incorrect url format:', web)
        cur.execute('UPDATE masterurls SET error = -1 WHERE url = ?', (web,))
        con.commit()

    times = times - 1

cur.execute('SELECT url FROM masterurls')
urllist = list()
for row in cur:
    urllist.append(str(row[0]))

con.close()

print('Current list of Master urls:', urllist)

# Need to have a join between two tables and create a one to many relationship.
