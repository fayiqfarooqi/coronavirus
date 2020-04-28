import json
import urllib.request
import psycopg2 as pg2


def insert_data(y):
    conn = pg2.connect(database='Corona', user='postgres', password='new12345', host="localhost", port="5432")
    cur = conn.cursor()
    sql = '''INSERT INTO coronavirus_Latest (State_id, State, District, Confirmed, Active, Deceased,
           Recovered, Delta_Confirmed, Delta_Deceased, Delta_Recovered)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
    val = (str(y[0]), str(y[1]), str(y[3]), str(y[4]), str(y[5]), str(y[6]), str(y[7]), str(y[8]), str(y[9]), str(y[10]))
    cur.execute(sql, val)
    conn.commit()


def create_table():
    conn = pg2.connect(database='Corona', user='postgres', password='new12345', host="localhost", port="5432")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE Coronavirus_Latest(
	State_id serial,
	State VARCHAR (50) NOT NULL,
	District VARCHAR (50) NOT NULL,
	Confirmed int,
	Active int,
	Deceased int,
	Recovered int,
	Delta_Confirmed int,
	Delta_Deceased int,
	Delta_Recovered int,
	created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    ''')
    print("table Created Successfully")
    conn.commit()




def print_results(data):
    the_Json = json.loads(data)
    for i, j in enumerate(the_Json, start=1):
        for x in j['districtData']:
            x = (
            i, j['state'], j['statecode'], x['district'], x['confirmed'], x['active'], x['deceased'], x['recovered'],
            x['delta']['confirmed'], x['delta']['deceased'], x['delta']['recovered'])
            y = list(x)
            insert_data(y)


def main():
    urldata = "https://api.covid19india.org/v2/state_district_wise.json"
    WebUrl = urllib.request.urlopen(urldata)
    print("result code :", str(WebUrl.getcode()))
    create_table()
    if WebUrl.getcode() == 200:
        data = WebUrl.read()
        print_results(data)


if __name__ == '__main__':
    main()