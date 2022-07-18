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

query = (
    "SELECT * FROM"
    "   (SELECT * FROM"
    "       (SELECT * FROM tPreDelete) AS pred,"
    "       (SELECT * FROM tPostDelete) AS postd) AS ppd,"
    "   (SELECT * FROM tPostUpdate) AS postu")

conn = mysql.connector.connect(**config)
cursor = conn.cursor()
cursor.execute(query)
d_PreDelete,f_PreDelete,d_PostDelete,f_PostDelete,d_PostUpdate,f_PostUpdate = cursor.fetchone()

def test_details_new():
    "verifies details table gained full row count after running new pipeline"
    assert d_PreDelete > d_PostDelete, "Details Had No Missing Rows"
    assert d_PostUpdate == d_PreDelete, "Details Did Not Fully Update"

def test_fatalities_new():
    "verifies fatalities table gained full row count after running new pipeline"
    assert f_PreDelete > f_PostDelete, "Fatalities Had No Missing Rows"
    assert f_PostUpdate == f_PreDelete, "Fatalities Did Not Fully Update"
