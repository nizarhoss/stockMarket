import config
import psycopg2
import psycopg2.extras
import alpaca_trade_api as tradeapi
import csv


connection = psycopg2.connect("dbname='stockmarket' user='postgres' host='localhost' password='admin12345' port='5430'")

cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
cursor.execute("SELECT * FROM stock where is_etf = true;")

api =tradeapi.REST(config.API_KEY,  config.API_SECRET,  base_url=config.API_URL)

etfs =cursor.fetchall()

dates = ['2021-02-05','2021-02-08']
for current_date in dates:
    for etf in etfs:
        print(etf['symbol'])
        
        with open(f"ark-data/data/{current_date}/{etf['symbol']}.csv") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                ticker =row[3]

                if ticker:
                    shares =row[5]
                    weight =row[7]
                    cursor.execute("""
                    SELECT * FROM stock WHERE symbol = %s
                    """ , (ticker,))
                    stock = cursor.fetchone()
                    if stock:
                        cursor.execute("""
                        INSERT INTO etf_holding (etf_id, holding_id, dt, shares, weight)   
                        VALUES (%s, %s, %s, %s, %s)
                        """ ,(etf['id'] , stock['id'], current_date, shares, weight))
                
connection.commit()
    
# assets =api.list_assets()
# for asset in assets:
#    print(f"Inserting stock {asset.name} {asset.symbol}")
#    cursor.execute("""INSERT INTO stock (name, symbol, exchange, is_etf) VALUES (%s,%s,%s, false) """ , (asset.name, asset.symbol, asset.exchange))
# connection.commit()


