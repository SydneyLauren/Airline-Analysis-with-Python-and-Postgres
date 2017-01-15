import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import ConfigParser
import argparse
import data_prep
from datetime import datetime
import geopy
import pickle
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from matplotlib.patches import Polygon
from geopy.geocoders import Nominatim


def database_connect(db, server, port, user_name, pwd):
    '''
    INPUT: name of existing postgres database, username
    OUTPUT: database connection and cursor

    Take database name and username and establish conncetion to database
    '''
    conn = psycopg2.connect(dbname=db, host=server, port=port, user=user_name, password=pwd)
    c = conn.cursor()
    return conn, c


def get_login(filepath):
    '''
    INPUT: path to file containing login credentials for database
    OUTPUT: list of login credentials to be passed to database_connect

    Take file containing database login credentials and return credentials
    '''
    with open(filepath) as f:
        return [line.strip('\n').split(',')[1] for line in f]


def print_output(data):
    '''
    INPUT: fetchall data from sql query
    OUTPUT: None

    Take SQL output and print to terminal
    '''
    for row in data:
        string = "{} "*len(row)
        print string.format(*row)


def sql_to_dataframe(sql_string, cols, create_temp=False):
    '''
    INPUT: sql query as string, inputs to sql string (will be column names in df)
    OUTPUT: pandas dataframe resulting from query

    Take a sql query and return pandas dataframe
    '''
    if create_temp:
        c.execute("SELECT *, 'N'+nnumber AS tailnum INTO temp_ac FROM aircraft")

    #
    c.execute(sql_string.format(*cols))
    data = c.fetchall()

    df = pd.DataFrame(columns=(cols))
    for i in xrange(len(data)):
        df.loc[i] = list(data[i])
    return df


def load_map(pickled_map):
    '''
    INPUT: pickled basemap object, city coordinates file
    OUTPUT: basemap object, basemap coordinates of cities
    '''
    # load the basemap object for plotting
    with open(pickled_map) as f:
        m = pickle.load(f)

    return m


def plot_ontime_map(m, coord_file, query_output, image_name):
    '''
    INPUT: basemap, city coordinates and names, image name and query output
    OUTPUT: map image path
    create map image by ontime performance
    '''
    fig = plt.figure(figsize=(15, 12))
    # load the city coordinates
    coord_dict = {}
    with open(coord_file) as f:
        for line in f:
            coord_dict[line.split('|')[0]] = np.array([float(coord) for coord in line.strip('\n').split('|')[1:]])
    city_dict = data_prep.clean_city_text(cities)

    for row in query_output:
        city_key = city_dict[row[2]]
        city_coords = coord_dict[city_key]
        alp = 0.2
        if row[1] < 75:
            m_size = 3
        elif row[1] < 80:
            m_size = 6
            alp = 0.4
        elif row[1] < 85:
            m_size = 9
            alp = 0.6
        elif row[1] < 90:
            m_size = 14
            alp = 0.75
        else:
            m_size = 20
            alp = 0.9

        plt.plot(city_coords[0], city_coords[1], 'c.', markersize=m_size, alpha=alp)
        m.bluemarble()
    plt.savefig(image_name, bbox_inches='tight')
    plt.close('all')


def plot_pctchange(m, coord_file, query_output, image_name):
    '''
    INPUT: basemap, city coordinates and names, image name and query output
    OUTPUT: map image path
    create map image by ontime performance
    '''
    fig = plt.figure(figsize=(15, 12))
    # load the city coordinates
    coord_dict = {}
    with open(coord_file) as f:
        for line in f:
            coord_dict[line.split('|')[0]] = np.array([float(coord) for coord in line.strip('\n').split('|')[1:]])
    city_dict = data_prep.clean_city_text(cities)

    for row in query_output:
        city_key = city_dict[row[0]]
        city_coords = coord_dict[city_key]
        alp = 0.8
        if row[1] > .05:
            color = [.3, 1, .2]
        elif row[1] < -.05:
            color = 'r'
        else:
            color = [1, .85, 0]

        plt.plot(city_coords[0], city_coords[1], '.', markersize=12, color=color, alpha=alp)
        m.bluemarble()
    plt.savefig(image_name, bbox_inches='tight')
    plt.close('all')


if __name__ == '__main__':
    login_data = get_login('logins.csv')  # retrieve database login credentials
    conn, c = database_connect(*login_data)  # connect to database

    '''0. Database exploration '''
    # get list of databases
    c.execute('''SELECT datname FROM pg_database WHERE datistemplate = false;''')
    databases = [dbnm[0] for dbnm in c.fetchall()]
    # print databases

    # get list of tables
    c.execute('''SELECT table_name FROM information_schema.tables
    where table_schema = 'public'
    ORDER BY table_schema,table_name;''')
    tables = [tblname[0] for tblname in c.fetchall()]
    # print tables

    c.execute("SELECT * FROM ontime LIMIT 1")
    colnames = [desc[0] for desc in c.description]
    # print ', '.join(colnames)

    c.execute("SELECT * FROM airlines LIMIT 1")
    colnames = [desc[0] for desc in c.description]
    # print ', '.join(colnames)

    c.execute("SELECT * FROM aircraft LIMIT 1")
    colnames = [desc[0] for desc in c.description]
    # print ', '.join(colnames)


    '''1. get count of flights in database by carrier and year'''
    sql_str = '''SELECT DISTINCT b.{0}, a.{1}, COALESCE(COUNT(a.{2}), 0)
    FROM ontime a LEFT JOIN airlines b
    ON a.AirlineID = b.AirlineID
    GROUP BY b.AirlineName, a.Year'''
    inputs = ['AirlineName', 'Year', 'FlightNum']

    df = sql_to_dataframe(sql_str, inputs)  # convert query output to dataframe
    df = df.pivot(index='AirlineName', columns='Year', values='FlightNum').fillna(0)
    df = df.astype(int)
    df.to_csv('Flight_Count.csv')


    '''2. As measured by on-time percentage, what was the performance for each aircraft manufacturer
    (e.g. Airbus, Boeing, Bombardier, etc), and how did that compare to the average of all aircraft,
    month-by month in 2015?'''

    sql_str = "SELECT SUBSTRING(t.mfrmdlcode, 1, 3) AS {0} , o.{1} \
    , ROUND(100*(1 - SUM(o.DepDel15)/COUNT(o.DepDel15)::float), 2) as {2} \
    FROM temp_ac t JOIN ontime o ON t.tailnum = o.tailnum \
    JOIN airlines a on a.AirlineID = o.AirlineID WHERE o.Year = 2015 \
    GROUP BY {0}, o.month ORDER BY {0}, o.month, {2}"
    inputs = ['mfr', 'month', 'ontime_pct']
    df_2015 = sql_to_dataframe(sql_str, inputs, create_temp=True)

    # Read manufacturer names
    mfr_df = pd.read_csv('ACFTREF.txt')
    cols = list(mfr_df.columns)
    column_names = [col.decode('ascii', 'ignore') for col in cols]
    mfr_df.columns = column_names
    mfr_df[['CODE', 'MFR']]

    def find_mfr(input):
        return input[0:3]

    # Add manufacturer names to dataframe based on manufacturer code
    mfr_df['mfr_code'] = mfr_df.apply(lambda x: find_mfr(x['CODE']), axis=1)
    code_df = mfr_df[['mfr_code', 'MFR']]
    code_df = code_df.drop_duplicates()
    df_2015['manufacturer'] = [code_df.loc[code_df.mfr_code == val, 'MFR'].iloc[0].strip() for val in df_2015.mfr]
    df_2015 = df_2015.drop('mfr', axis=1)

    # Pivot the dataframe and add average ontime percentage per month
    df_2015 = df_2015.pivot(index='manufacturer', columns='month', values='ontime_pct')
    df_2015.loc['Average'] = list(df_2015.mean())
    df_2015.sort_index(axis=0)
    df_2015.to_csv('Ontime_Manufacturer_2015.csv')


    '''3. Where were the hot-spots geographically for on-time performance? How did that vary over time?'''
    c.execute('''SELECT DISTINCT OriginCityName FROM ontime''')
    cities = c.fetchall()

    c.execute('''SELECT DISTINCT Year FROM ontime''')
    years = c.fetchall()
    m = load_map('base_map.pkl')

    plotyears = False
    if plotyears:
        year_ranges = ['(2006, 2007)', '(2008, 2009)', '(2010, 2011)', '(2012, 2013)', '(2014, 2015)']
        for year in year_ranges:
            print year
            c.execute('''SELECT Year
                              , ROUND(100*(1 - SUM(DepDel15)/COUNT(DepDel15)::float), 2) as ontime_pct
                              , OriginCityName, Origin
                         FROM ontime GROUP BY Year, OriginCityName, Origin
                         HAVING COUNT(DepDel15) > 100 AND Year IN {}'''.format(year))
            plot_ontime_map(m, 'city_coordinates.txt', c.fetchall(), '../images/{}.png'.format(year))

    # Create a temporary table containing on-time percent
    c.execute('''SELECT OriginCityName
    , Year
    , ROUND(100*(1 - SUM(DepDel15)/COUNT(DepDel15)::float), 2) as ontime_pct
    INTO temp_ontime_pct
    FROM ontime GROUP BY Year, OriginCityName''')

    c.execute('''SELECT a.OriginCityName, (a.ontime_pct - b.ontime_pct) / b.ontime_pct :: float as pct_improve
    FROM temp_ontime_pct a
    JOIN temp_ontime_pct b
    ON a.Year = b.Year + 9 AND a.OriginCityName = b.OriginCityName
    Where a.Year = 2015 ORDER BY pct_improve''')

    plot_pctchange(m, 'city_coordinates.txt', c.fetchall(), '../images/pct_change06-15.png')
    plt.show()


    '''4. What happened to Sea-Tac Airport on-time performance during the 2006-2015 time period?
    How did that compare to the average of all airports?'''
    # get Sea-Tac ontime performance per year
    c.execute("SELECT Year, AVG(ontime_pct) AS avg FROM temp_ontime_pct \
    WHERE OriginCityName = 'Seattle, WA' GROUP BY Year ORDER BY Year")
    sea_ontime = c.fetchall()

    # get average of all airports
    c.execute("SELECT Year, AVG(ontime_pct) AS avg FROM temp_ontime_pct\
    GROUP BY Year ORDER BY Year")
    avg_ontime = c.fetchall()

    years_sea = [d[0] for d in sea_ontime]
    ontime_sea = [d[1] for d in sea_ontime]
    years_avg = [d[0] for d in avg_ontime]
    ontime_all = [d[1] for d in avg_ontime]

    plt.plot(years_avg, ontime_all, 'k.-', linewidth=2)
    plt.plot(years_sea, ontime_sea, 'b.-', linewidth=2)
    plt.xlabel('Year')
    plt.ylabel('Percent of Flights On Time')
    plt.ylim([50, 100])
    plt.grid(True, which='both')
    plt.legend(['Average of all airports', 'Sea-Tac'], loc=1)
    plt.savefig('../images/seatac_comparison.png', bbox_inches='tight')
    plt.close('all')
