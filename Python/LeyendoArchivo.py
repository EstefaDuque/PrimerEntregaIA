import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

#Conectandose al motor de base de datos mysql y creando la base de datos
try:
    conn = msql.connect(host='localhost', user='root',port='33062',
                        password='admin123')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS Co2DB;")
        cursor.execute("CREATE DATABASE Co2DB ;")
        print("Co2DB database is created")
except Error as e:
    print("Error conectando a MySQL", e)

#Leyendo Archivo de Paises csv
paisesCsv = pd.read_csv('C:\\Datos\\UniversidadItm\\IA\\PrimerEntregable\\PrimerEntregaIA\\DataSet\\tabla_paises.csv', index_col=False, delimiter = ';')

try:
    conn = msql.connect(host='localhost', database='Co2DB', user='root', password='admin123',port='33062')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Tu estas conectado a la base de datos: ", record)
        cursor.execute('DROP TABLE IF EXISTS pais;')
        print('Creando tabla....')
        cursor.execute("CREATE TABLE pais(id INT PRIMARY KEY,country_name VARCHAR(100) NOT NULL);")
        print("Tabla creada....")
        for i,pais in paisesCsv.iterrows():
          
            sqlPais = "INSERT INTO Co2DB.pais VALUES (%s,%s)"
            print(tuple(pais))
            print(sqlPais)
            cursor.execute(sqlPais, tuple(pais))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
except Error as e:
    print("Error conectando a MySQL", e)


#Leyendo Archivo de Co2 csv


co2Csv = pd.read_csv('C:\\Datos\\UniversidadItm\\IA\\PrimerEntregable\\\PrimerEntregaIA\\DataSet\\tabla_co22.csv', index_col=False, delimiter = ';')

try:

    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute('DROP TABLE IF EXISTS co2;')
        print('Creando tabla....')
        cursor.execute("""CREATE TABLE co2(
            id INT  PRIMARY KEY,
            id_pais INT,
            year1960 VARCHAR(100) NULL,
            year1961 VARCHAR(100) NULL,
            year1962 VARCHAR(100) NULL ,  
            year1963 VARCHAR(100) NULL ,  
            year1964 VARCHAR(100) NULL ,  
            year1965 VARCHAR(100) NULL ,  
            year1966 VARCHAR(100) NULL ,  
            year1967 VARCHAR(100) NULL ,  
            year1968 VARCHAR(100) NULL ,  
            year1969 VARCHAR(100) NULL ,  
            year1970 VARCHAR(100) NULL ,  
            year1971 VARCHAR(100) NULL ,  
            year1972 VARCHAR(100) NULL ,  
            year1973 VARCHAR(100) NULL ,  
            year1974  VARCHAR(100) NULL ,  
            year1975  VARCHAR(100) NULL , 
            year1976  VARCHAR(100) NULL , 
            year1977  VARCHAR(100) NULL , 
            year1978  VARCHAR(100) NULL , 
            year1979  VARCHAR(100) NULL , 
            year1980  VARCHAR(100) NULL , 
            year1981 VARCHAR(100) NULL ,  
            year1982 VARCHAR(100) NULL ,  
            year1983 VARCHAR(100) NULL ,  
            year1984  VARCHAR(100) NULL ,  
            year1985  VARCHAR(100) NULL , 
            year1986  VARCHAR(100) NULL , 
            year1987  VARCHAR(100) NULL , 
            year1988  VARCHAR(100) NULL , 
            year1989  VARCHAR(100) NULL ,
            FOREIGN KEY (id_pais) REFERENCES pais(id)
            );""")
        print("Tabla es creado....")

        for i,row in co2Csv.iterrows():
            df2 = row.astype(object).where(pd.notnull(row), None)
            sql = "INSERT INTO Co2DB.co2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            print(tuple(row))
            cursor.execute(sql, tuple(df2))
            print("Record inserted")
            conn.commit()
except Error as e:
    print("Error conectando a MySQL", e)


#Ejecutar consulta
# Execute query
sql = "select * from co2 c inner join pais p on c.id_pais= p.id where id_pais=1 or id_pais=9 or id_pais=66 ;"
cursor.execute(sql)
# Fetch all the records
result = cursor.fetchall()
print('Consulta')
for i in result:
    print('Registro : \n')
    print(i)