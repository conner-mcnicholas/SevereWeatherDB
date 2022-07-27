import pytest
from datetime import date
import os
import mysql.connector
from mysql.connector import errorcode
import os

config = {
  'host':'sevwethmysqlserv.mysql.database.azure.com',
  'user':'conner@sevwethmysqlserv',
  'password':'Universal124!',
  'database':'defaultdb',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': f'{os.environ["HOME"]}/.ssh/DigiCertGlobalRootG2.crt.pem'
}

query = ("SELECT * FROM"
    "   (SELECT * FROM"
    "       (SELECT * FROM N_PreDelete) AS pred,"
    "       (SELECT * FROM N_PostDelete) AS postd) AS ppd,"
    "   (SELECT * FROM N_PostUpdate) AS postu;")

conn = mysql.connector.connect(**config)
cursor = conn.cursor()
cursor.execute(query)
D_PreDelete,F_PreDelete,D_PostDelete,F_PostDelete,D_PostUpdate,F_PostUpdate = cursor.fetchone()

def test_details_new():
    "verifies details table gained full row count after running new pipeline"
    assert D_PreDelete > D_PostDelete, "Details Had No Missing Rows"
    assert D_PostUpdate == D_PreDelete, "Details Did Not Fully Update"

def test_fatalities_new():
    "verifies fatalities table gained full row count after running new pipeline"
    assert F_PreDelete > F_PostDelete, "Fatalities Had No Missing Rows"
    assert F_PostUpdate == F_PreDelete, "Fatalities Did Not Fully Update"
