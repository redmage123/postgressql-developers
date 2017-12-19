#!/usr/bin/env python3

import psycopg2
import csv
from collections import namedtuple
import re
import sys


try:
    connect = psycopg2.connect('dbname = ebooks user=postgres password=postgres')
except: 
    print ('Connect Error')
cur = connect.cursor()


try:
    f = open('../data/ebook2016.csv') 
except OSError as e:
    print (e)
with open('../data/ebook2016.csv') as f:
    eb = namedtuple('ebook', 'source subject isbn title year publisher available active remarks') 
    f.readline()
    data = csv.reader(f)
    for line in data:
        subjectstr = lambda x:re.search('^eBook - (.*) .*$',x).group(1)
        ebook =  eb(source=line[0],subject = subjectstr(line[1]),isbn=line[3],title=line[5],
                       year=line[6],publisher=line[10],available=line[11],active=line[12],remarks=line[13])

      
        try:
             cur.execute('INSERT INTO source (source_name) VALUES (%s) ON CONFLICT DO NOTHING;',(ebook.source,))
             cur.execute ('INSERT INTO subject(subject_desc) VALUES (%s) ON CONFLICT DO NOTHING;',(ebook.subject,))
             cur.execute ('INSERT INTO publisher(publisher_name) VALUEs (%s) ON CONFLICT DO NOTHING;',(ebook.publisher,))
             connect.commit() 
             cur.execute("SELECT source_id FROM source WHERE source_name = \'%s\';" % ebook.source)
             source_id = cur.fetchone()[0]
             cur.execute('SELECT subject_id  FROM subject WHERE subject_desc = \'%s\';' % ebook.subject)
             subject_id = cur.fetchone()[0]
             cur.execute('SELECT publisher_id  FROM publisher WHERE publisher_name = \'%s\';' % ebook.publisher)
             publisher_id = cur.fetchone()[0]
             cur.execute('''INSERT INTO ebook (isbn,source_id,subject_id,publisher_id,book_title,
                                               release_year,status,availability,remarks)
                                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);''',
                                               (ebook.isbn,source_id,subject_id,publisher_id,
                                                ebook.title,ebook.year,ebook.active,ebook.available, ebook.remarks))
             connect.commit()
        except (Exception,psycopg2.DatabaseError) as error:
            print (error)
            connect.close()
            sys.exit()

    connect.close()

    

    


'''
def main():

   drop_tables()
   create_tables()
   connect.close()

if __name__ == '__main__':
    main()
'''




