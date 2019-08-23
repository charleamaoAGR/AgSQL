from packages import sql_maintainer
from packages import cleanData
import csv


MAWP_60_TABLE_INIT_QUERY = """CREATE TABLE IF NOT EXISTS mawp60 (
TMPSTAMP DATE,
RECNBR SMALLINT,
StmID SMALLINT,
BatMin FLOAT(4, 2),
Air_T FLOAT(4, 2),
AvgAir_T FLOAT(4, 2),
MaxAir_T FLOAT(4, 2),
MinAir_T FLOAT(4, 2),
RH FLOAT(5, 2),
AvgRH FLOAT(5, 2),
Pluvio_Rain FLOAT(6, 2),
Pluvio_Rain24RT FLOAT(6, 2),
WS_10Min FLOAT(6, 3),
WD_10Min FLOAT(4, 1),
AvgWS FLOAT(6, 3),
AvgWD FLOAT(4, 1),
AvgSD FLOAT(6, 3),
MaxWS_10 FLOAT(6, 3),
MaxWD_10 FLOAT(4, 1),
MaxWS FLOAT(6, 3),
HmMaxWS DATE,
MaxWD FLOAT(4, 1),
Max5WS_10 FLOAT(6, 3),
Max5wd_10 FLOAT(4, 1),
WS_2Min FLOAT(6, 3),
WD_2Min FLOAT(4, 1),
Soil_T05 FLOAT(5, 2),
AvgRS_kw FLOAT(5, 3),
TotRS_MJ FLOAT(7, 5),
TBRG_Rain FLOAT(4, 1),
TBRG_Rain24RT2 FLOAT(4, 1),
Soil_TP5_TempC FLOAT(5, 2),
Soil_TP5_VMC FLOAT(5, 2),
Soil_TP20_TempC FLOAT(5, 2),
Soil_TP20_VMC FLOAT(5, 2),
Soil_TP50_TempC FLOAT(5, 3),
Soil_TP50_VMC FLOAT(5, 3)
);
"""


def main():

    connection_var = sql_maintainer.open_connection()
    # cleanData('mawp60raw.txt')
    with open('mawp60raw.csv', 'r') as csv_stream:
        csv_contents = list(csv.reader(csv_stream))
    cursor, result = sql_maintainer.csv_to_sql_insert_queries(csv_contents, connection_var, 'mawp60')
    connection_var.commit()
    sql_maintainer.close_connection(connection_var, cursor)



main()

