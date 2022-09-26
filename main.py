import mysql.connector
import glob

print("Program start")
insert = []

mydb = mysql.connector.connect(
  host="87.106.170.237",
  user="dwd",
  password="101Martin",
  database='dwd'
)

arrTxts = (glob.glob("C:\\Users\\mail\\Downloads\\dwd3\\*\\produkt*.txt"))

mycursor = mydb.cursor()
for dateiname in arrTxts:
     with open(dateiname) as f:
          while True:
               val = f.readline()
               if not val:
                    break
               if not(val.startswith('STATIONS_ID')):
                  arr = val.replace(" ","").replace("\n","").split(';')
                  insert.append(arr)
sql = """INSERT INTO `klimadaten`(`STATIONS_ID`, `MESS_DATUM_BEGINN`, `MESS_DATUM_ENDE`, `QN_4`, `MO_N`, `MO_TT`, `MO_TX`, `MO_TN`, `MO_FX`, `MX_TX`, `MX_FX`, `MX_TN`, `MO_SD_S`, `QN_6`, `MO_RR`, `MX_RS`, `eor`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
mycursor.executemany(sql, insert)
mydb.commit()

print(mycursor.rowcount, "record inserted.")
print("Program end")
