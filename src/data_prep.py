import psycopg2
import geopy
import pickle
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


def generate_basemap(cities, file_out):
    '''
    INPUT: list of cities
    OUTPUT: pickled basemap object and text file containing basemap coordinates for cities
    generate a basemap object and list of city basemap coordinates for future plotting
    '''
    # create the map
    m = Basemap(width=12000000*1.5, height=9000000*1.5, projection='lcc',
                resolution=None, lat_1=45., lat_2=55, lat_0=50, lon_0=-107.)
    m.bluemarble()

    ax = plt.gca()  # get current axes instance
    geolocator = Nominatim()
    timeouts = 0

    with open(file_out, 'w') as f:
        for city in cities:
            try:
                location = geolocator.geocode(city)
            except geopy.exc.GeocoderTimedOut:
                print 'timeout on ', city
                timeouts += 1
                continue
            xpt, ypt = m(location.longitude, location.latitude)
            f.write('{}|{}|{}\n'.format(city, xpt, ypt))
            plt.plot(xpt, ypt, 'o', markersize=4, color=[.7, .7, .7], alpha=1)
            # plt.text(xpt, ypt, city)
        if timeouts == 0:
            plt.savefig('airport_map.png', bbox_inches='tight')
            pickle.dump(m, file('base_map.pkl', 'w'))


def clean_city_text(cities):
    '''
    INPUT: list of cities from query
    OUTPUT: cleaned list of cities from sql query

    Take text containing cities and clean for future use with geopy
    '''
    cleaned_citytext = {}
    for city in cities:
        slash = city[0].find('/')
        comma = city[0].find(',')
        if slash > 0:
            cty = city[0][0:slash] + city[0][comma:]
        elif city[0][comma:] == ', TT':
            cty = city[0][0:comma]
        elif ', CA' in city[0]:
            cty = city[0][0:comma] + ', California'
        elif ', LA' in city[0]:
            cty = city[0][0:comma] + ', Louisiana'
        elif ', PR' in city[0]:
            cty = city[0][0:comma] + ', Puerto Rico'
        else:
            cty = city[0]
        cleaned_citytext[city[0]] = cty
    return cleaned_citytext


if __name__ == '__main__':
    login_data = get_login('logins.csv')  # retrieve database login credentials
    conn, c = database_connect(*login_data)  # connect to database

    c.execute('''SELECT DISTINCT OriginCityName FROM ontime''')  # Get list of all origin cities
    city_dict = clean_city_text(c.fetchall())  # clean up the text to get coordinates for basemap plotting using geopy

    generate_basemap(city_dict.values(), 'city_coordinates.txt')  # generate basemap of the US with origin cities
    plt.show()
