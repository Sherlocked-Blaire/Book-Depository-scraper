import psycopg2
import pandas as pd
import csv
from sqlalchemy import create_engine, types

class DatabaseError(psycopg2.Error):
    pass
class DatabaseConnector():
    def __init__(self):
        self.__connection = None
        self.__cursor = None

    def connect_database(self):
        """
        creates a connection to the postgres database.
        Returns the  connection cursor
        """
        try:
            self.__connection = psycopg2.connect(
                host="host",
                user="username",
                port="port",
                database="databasename",
                password="password"
            )
            self.__connection.autocommit = True
            self.__cursor = self.__connection.cursor()
            return self.__cursor
        except DatabaseError:
            raise DatabaseError("There was a problem connecting to the requested database.")


    def create_tables(self):
        """
        Create tables and insert dataframe in database.
        Returns None
        """

        cur = self.connect_database()
        try:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id serial PRIMARY KEY,
                    category VARCHAR(50)
                );
                ''')

            cur.execute('''
                CREATE TABLE IF NOT EXISTS booksdepo (
                id serial PRIMARY KEY,
                book_title varchar(10000),
                book_price varchar(255),
                book_url varchar(10000),
                image_url varchar(10000),
                Published_date date,
                category varchar(255)
                );
                ''')

        except (DatabaseError, Exception):
            raise DatabaseError("Could not create tables in the specified database")
        
    def delete_tables(self):
        """
        deletes Listing and Categories tables from the database
        Returns Tables successfully deleted message
        """
        cur = self.connect_database()
        try:
            cur.execute("DROP TABLE IF EXISTS booksdepo CASCADE ")
            cur.execute("DROP TABLE IF EXISTS categories CASCADE")
            print("booksdepo and Categories tables no longer in database.")
        except (Exception, DatabaseError):
            raise DatabaseError("Could not drop tables in the specified database")    
        
        
    def insert_into_tables(self,path):
        """
        inserts Listing and Categories tables from the database
        Returns Tables successfully inserted message
        """
        try:
            cur = self.connect_database()  
            df = pd.read_csv(path)
            data = df.to_records(index=False)
            host="host"
            user="user"
            port="portr"
            database="database"
            password="password"
            postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    
            engine = create_engine(postgres_str, echo=False)
            df.to_sql('booksdepo',con=engine,index=False,if_exists='append')
            
            cur.execute('''
            INSERT INTO categories (category) VALUES('finace');
            INSERT INTO categories (category) VALUES('chidren');
            INSERT INTO categories (category) VALUES('economics');
            ''')
            print("Succesfully added values to tables")        
        except (Exception, DatabaseError):
            raise DatabaseError("Could not add value to  tables in the specified database")    
        
        self.__connection.commit()
        
        
    def join_and_export():
           """
        Execute query, fetch all the records and export it to csv file.
        Returns csv file containing all the records in the database
        """
    cur = self.connect_database() 

    cur.execute(
            """select categories.category, booksdepo.book_title, booksdepo.book_price, booksdepo.book_url, booksdepo.image_url  booksdepo.Published_date 
            FROM booksdepo 
            LEFT JOIN categories on booksdepo.category = categories.category"""
        )

    df = pd.DataFrame(cur.fetchall())
    df.to_csv('data.csv')    
if __name__ == '__main__':
    db = DatabaseConnector()
    db.connect_database()
    #db.create_tables()
    path = str(input("Enter the path to the csv:\n"))
    db.insert_into_tables(path)