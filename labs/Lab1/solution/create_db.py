#!/usr/bin/env python3

import psycopg2
import csv
import sys

''' Solution to Lab 1 Drop and Create automation
'''

def create_tables(connect,cur):
    create_tables  = [ ''' CREATE TABLE IF NOT EXISTS source (
                                                   source_id SERIAL PRIMARY KEY NOT NULL,
                                                   source_name VARCHAR (100) UNIQUE
                                                  )
                       ''',
                       ''' CREATE TABLE IF NOT EXISTS subject (
                                                  subject_id SERIAL PRIMARY KEY NOT NULL,
                                                  subject_desc VARCHAR (100) UNIQUE
                                           )
                       ''',

                       ''' CREATE TABLE IF NOT EXISTS publisher (
                                                  publisher_id SERIAL PRIMARY KEY NOT NULL,
                                                  publisher_name VARCHAR (100) UNIQUE
                                               )
                       ''',
                       ''' CREATE TABLE IF NOT EXISTS ebook (
                                                  isbn VARCHAR (17) PRIMARY KEY NOT NULL,
                                                  source_id SERIAL REFERENCES source(source_id),
                                                  subject_id  SERIAL REFERENCES subject(subject_id),
                                                  publisher_id SERIAL REFERENCES publisher(publisher_id),
                                                  book_title VARCHAR (200),
                                                  release_year VARCHAR(4),
                                                  status VARCHAR(8),
                                                  availability DATE,
                                                  remarks VARCHAR(100)
                                             )
                       '''
                  
                 ]
    try:
        for command in create_tables:
           cur.execute (command)
    except (Exception, psycopg2.DatabaseError) as error:
        print ('Error in create: ',error)

    connect.commit()
    return

def drop_tables(connect,cur):
    drop_commands = [ 'DROP TABLE IF EXISTS source CASCADE',
                      'DROP TABLE IF EXISTS subject CASCADE  ',
                      'DROP TABLE IF EXISTS publisher CASCADE ',
                      'DROP TABLE IF EXISTS ebook CASCADE'
                    ]

    try:
        for command in drop_commands:
            cur.execute (command)
    except (Exception, psycopg2.DatabaseError) as error:
            print ('Error in drop: ',error)
    connect.commit()
    return


def main():
    try:
        connect = psycopg2.connect('dbname = ebooks user=postgres password=postgres')
    except: 
        print ('Connect Error')
        sys.exit(1)
    cur = connect.cursor()

    drop_tables(connect,cur)
    create_tables(connect,cur)
    connect.close()

if __name__ == '__main__':
    main()




