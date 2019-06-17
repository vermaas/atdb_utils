"""
    File name: atdb_utils.py
    version: 1.0.0 (24 may 2019)
    Author: Copyright (C) 2019 - Nico Vermaas (vermaas@astron.nl)
    Date created: 2019-01-13
    Description: just a bunch of examples and tests of how to access ATDB from Python
"""

import psycopg2
import argparse

import matplotlib.pyplot as plt
#matplotlib.rcParams["interactive"] = True

from pylab import *
rc('text', usetex=True)
rc('font',**{'family':'serif','serif':['serif'],'size':14})

# some constants
ATDB_HOST_DEV = "http://localhost:8000/atdb"
ATDB_HOST_VM = "http://192.168.22.22/atdb"
ATDB_HOST_TEST = "http://atdb-test.astron.nl/atdb"
ATDB_HOST_ACC = "http://192.168.22.25/atdb"
ATDB_HOST_PROD = "http://atdb.astron.nl/atdb"

ALTA_HOST_DEV = "http://localhost:8000/altapi"
ALTA_HOST_TEST = "http://alta-sys.astron.nl/altapi"
ALTA_HOST_ACC = "https://alta-acc.astron.nl/altapi"
ALTA_HOST_PROD = "https://alta.astron.nl/altapi"

DJANGO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('execution time: %r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed

@timeit
def count_dataproducts():

    print('count_dataproducts()')
    from atdb_interface.atdb_interface import ATDB
    atdb_interface = ATDB(ATDB_HOST_PROD)

    query = 'taskID__icontains=1'

    ids = atdb_interface.do_GET_LIST(key='dataproducts:id', query=query)
    print(len(ids))


@timeit
def add_dataproduct():

    print('add_dataproduct()')

    # this is an example of how functions from the (higher) atdb_services layer can be called
    from atdb_services import atdb_io
    from atdb_services.service_add_dataproduct import do_add_dataproduct

    atdb_service = atdb_io.ATDB_IO(atdb_io.ATDB_HOST_TEST, '', '', '', '', False, False, False)
    do_add_dataproduct(atdb_service, taskid='190405034', node=None, data_dir='my_data_dir', filename='my_file.fits')

    atdb_service.do_change_status('observations','taskid:190405034','aborted (reason)')


@timeit
def add_dataproducts():
    # calling the atdb_services.do_add_dataproducts
    from atdb_services import atdb_io
    from atdb_services.service_add_dataproduct import do_add_dataproducts

    print('add_dataproducts()')

    my_taskid = '190516032'
    my_data_dir = '/data2/output'

    dp1 = {'node' : 'arts003', 'data_dir' : my_data_dir, 'filename' : 'NV_190516032_B014.MS' }
    dp2 = {'node' : 'arts004','data_dir': my_data_dir, 'filename': 'NV_190516032_B015.MS', 'size': 1234567890}
    dp3 = {'node' : 'arts005','filename': 'NV_190516032_B016.fits', 'size': 1234567890}
    dp4 = {'node' : 'arts006','filename': 'NV_190516032_B017.fits', 'new_status': 'valid', 'size': 1234567890}
    dp5 = {'node' : 'arts007','filename': 'NV_190516032_B018.fits', 'new_status': 'valid', 'size': 1234567890}
    dps = [dp1,dp2,dp3,dp4,dp5]

    print(dps)
    #atdb_interface = ATDB(ATDB_HOST_TEST)

    atdb_service = atdb_io.ATDB_IO(atdb_io.ATDB_HOST_PROD)
    # do_add_dataproduct(atdb_service, taskid='190405034', node=None, data_dir='my_data_dir', filename='my_file.fits')
    taskid = do_add_dataproducts(atdb_service, taskid=my_taskid, dataproducts=dps)
    print(taskid)


@timeit
def get_observation(taskid):
    print('get_observation('+taskid+')')

    from atdb_interface.atdb_interface import ATDB
    atdb_interface = ATDB(ATDB_HOST_PROD)
    observation = atdb_interface.do_GET_Observation(taskid='190409006')
    parset_template = observation['parset_location']
    print(str(observation))
    print(str(parset_template))


@timeit
def connect_to_database_example(host):

    # connection parameters
    port ="5432"
    database = "atdb"
    user = "atdbread"
    password = "atdbread123"

    connection = None

    try:
        # connect to the PostgreSQL server
        connection = psycopg2.connect(host = host, port = port, database = database, user = user, password = password)

        # create a cursor
        cursor = connection.cursor()

        # display the PostgreSQL database server version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        # close the communication with the PostgreSQL
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')



@timeit
def get_size_per_date_dataproducts(taskid_start, taskid_end, status):

    # connection parameters
    host = "192.168.22.25"
    port ="5432"
    database = "atdb"
    user = "atdbread"
    password = "atdbread123"

    connection = None

    try:
        # connect to the PostgreSQL server
        connection = psycopg2.connect(host = host, port = port, database = database, user = user, password = password)

        # create a cursor
        cursor = connection.cursor()

        # display the PostgreSQL database server version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        # close the communication with the PostgreSQL
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')



@timeit
def list_dataproducts_status(taskID, status):
    print('list_dataproducts('+taskID+')')

    # http://atdb.astron.nl/atdb/dataproducts/?taskID=190311040&my_status=invalid
    from atdb_interface.atdb_interface import ATDB
    atdb_interface = ATDB(ATDB_HOST_PROD)
    query = 'taskID=' + taskID + '&my_status='+status
    print(ATDB_HOST_PROD+'/'+query+":")

    ids = atdb_interface.do_GET_LIST(key='dataproducts:id', query=query)
    print(len(ids))

    for id in ids:
        filename = atdb_interface.do_GET(key='dataproducts:filename', id=id, taskid=None)
        print(filename)

    # example: has 1811130001 been on 'running?'
    # http://localhost:8000/atdb/status/?&taskID=181130001&name=running
    # first try to post a new record
    # get all valid
    # http://atdb.astron.nl/atdb/status/?&taskID=190311040&name=invalid
    # q = "/status/?taskID="+taskID+"&name="+status
    # url = ATDB_HOST_PROD + q

    #response = requests.get(
    #    url,
    #    auth=('admin', 'admin'),
    #)
    #j = response.json()
    #count = j['count']
    #results = j['results']
    #print(len(results))
    #print(results)

    # http://atdb.astron.nl/atdb/dataproducts/?taskID=190311038 (gives a count of 444)
    # and a next page

@timeit
def change_status(taskID,status):
    print('change_status('+taskID+','+status+')')

    ATDB_HOST = "http://atdb-test.astron.nl/atdb"
    #taskID = '190507001'
    #status = 'removing'

    from atdb_interface.atdb_interface import ATDB
    atdb_interface = ATDB(ATDB_HOST)
    atdb_interface.do_PUT(key='observations:new_status', taskid=taskID,value=status)
    atdb_interface.do_PUT_LIST(key='dataproducts:new_status', taskid=taskID,value=status)

    # alternative method, but with an extra dependency on atdb_services
    # from atdb_services import atdb_io
    # atdb_service = atdb_io.ATDB_IO(atdb_io.ATDB_HOST_VM, '', '', '', '', False, False, False)
    # atdb_service.do_change_status('observations','taskid:190405034','aborted (reason)')


@timeit
def set_ingest_progress(taskID,progress):
    # example : progress = '80.03% done, ETA to finish: 0.07 hr'
    print('set_ingest_progress('+taskID+','+progress+')')

    ATDB_HOST = "http://atdb-test.astron.nl/atdb"
    taskID = 190503009
    progress = "80.03% done, ETA to finish: 0.07 hr"

    from atdb_interface.atdb_interface import ATDB
    atdb_interface = ATDB(ATDB_HOST)
    atdb_interface.do_PUT(key='observations:control_parameters', taskid=taskID,value=progress)


@timeit
def list_observations(status):
    print('list_observations('+status+')')

    # http://atdb.astron.nl/atdb/dataproducts/?taskID=190311040&my_status=invalid
    from atdb_interface.atdb_interface import ATDB
    atdb_interface = ATDB(ATDB_HOST_PROD)
    query = 'taskid__gt190325102&taskid__lt190401001&observing_mode__icontains=imaging'
    taskIDs = atdb_interface.do_GET_LIST(key='observations:taskID', query=query)

    print(ATDB_HOST_PROD+'/'+query+":")

    ids = atdb_interface.do_GET_LIST(key='observations:id', query=query)
    print(len(ids))

    i=0
    for taskid in taskIDs:
        #starttime = atdb_interface.do_GET(key='observations:starttime', id=None, taskid=taskid)
        #print(taskid+' - '+starttime)
        i=i+1

    print(i)


def print_query(cursor, query):
    cursor.execute(query)
    record = cursor.fetchone()
    print(record, "\n")

def query_database(host):

    # connection parameters
    port ="5432"
    database = "atdb"
    user = "atdbread"
    password = "atdbread123"
    #user = "dbadmin"
    #password = "dbadmin123"

    connection = None

    try:

        # connect to the PostgreSQL server
        connection = psycopg2.connect(host = host, port = port, database = database, user = user, password = password)

        # create a cursor
        cursor = connection.cursor()

        # display the PostgreSQL database server version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        for table in cursor.fetchall():
            print(table)

        cursor.execute('SELECT * FROM taskdatabase_dataproduct')
        colnames = [desc[0] for desc in cursor.description]
        print(colnames)

        # input
        start_date = datetime.datetime.strptime('2019-03-17 12:00', '%Y-%m-%d %H:%M')
        end_date = datetime.datetime.strptime('2019-03-25 12:00', '%Y-%m-%d %H:%M')
        curr_date = start_date

        dates = []
        arts_list = []
        imaging_list = []

        print('DATE ARTS IMAGING')

        while curr_date <= end_date:

            datestr = datetime.datetime.strftime(curr_date, '%y%m%d')

            cursor.execute(
                "SELECT sum(size) FROM public.taskdatabase_dataproduct where filename like 'ARTS%s%%'" % datestr)
            arts = cursor.fetchone()

            cursor.execute(
                "SELECT sum(size) FROM public.taskdatabase_dataproduct where filename like 'WSRTA%s%%'" % datestr)
            imaging = cursor.fetchone()

            print(datestr, arts[0], imaging[0])
            dates.append(curr_date)

            if arts[0] != None:
                arts_list.append(float(arts[0]) / 1e12)
            else:
                arts_list.append(0)

            if imaging[0] != None:
                imaging_list.append(float(imaging[0]) / 1e12)
            else:
                imaging_list.append(0)

            curr_date = curr_date + datetime.timedelta(days=1)

        # Make cumulative
        arts_cumu = []
        imaging_cumu = []
        for i in range(0, len(dates)):
            j = sum(arts_list[0:i + 1])
            arts_cumu.append(j)
            j = sum(imaging_list[0:i + 1])
            imaging_cumu.append(j)

        print(arts_cumu[-1] / 134.40)
        # sys.exit()

        x = np.linspace(0, 2, 100)

        plt.plot(x, x, label='linear')
        plt.plot(x, x ** 2, label='quadratic')
        plt.plot(x, x ** 3, label='cubic')

        plt.xlabel('x label')
        plt.ylabel('y label')

        plt.title("Simple Plot")

        plt.legend()

        plt.show()

        plt.figure(figsize=(12, 4))
        plt.step(dates, arts_list, label='ARTS', color='orange', linewidth=2)
        step(dates, imaging_list, label='IMAGING', color='cornflowerblue', linewidth=2)
        legend(loc=0)
        xlabel('Time')
        ylabel('TB ingested')
        title('SVC Week 1: ARTS (18-25th March 2019)')
        grid(True, alpha=0.3)
        savefig('alta_svc_week1.png', bbox_inches='tight', dpi=300)

        figure(figsize=(12, 4))
        step(dates, arts_cumu, label='ARTS', color='orange', linewidth=2)
        step(dates, imaging_cumu, label='IMAGING', color='cornflowerblue', linewidth=2)
        legend(loc=0)
        xlabel('Time')
        ylabel('TB ingested')
        title('SVC Week 1: ARTS (18-25th March 2019)')
        grid(True, alpha=0.3)
        savefig('alta_svc_week1_cumu.png', bbox_inches='tight', dpi=300)
        #print_query(cursor,"SELECT sum(size) FROM public.taskdatabase_dataproduct where filename like 'WSRTA190304045%'")

        #print_query(cursor,"SELECT sum(size) FROM public.taskdatabase_dataproduct where filename like 'WSRTA190304046%'")


        # close the communication with the PostgreSQL
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')


#------------------------------------------------------------------------------------

def main():
    """
    The main module.
    """
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--operation","-o",
                        default="list_dataproducts")
    parser.add_argument("--taskid", nargs="?",
                        default=None,
                        help="Optional taskID which can be used instead of '--id' to lookup Observations or Dataproducts.")
    parser.add_argument("--taskid_start", nargs="?", default=None)
    parser.add_argument("--taskid_end", nargs="?", default=None)
    parser.add_argument("--value", nargs="?", default=None)
    parser.add_argument("--status",
                        default="valid",
                        help="status to be set by the set-status operation")
    parser.add_argument("--host", nargs="?", default="192.168.22.25")

    print('--- atdb_utils.py - version 1.0.0 (10 mar 2019) ---')

    args = parser.parse_args()

    # connect to ATDB (and potentially ALTA)
    # atdb_interface = ATDB(ATDB_HOST_PROD)
    # atdb_services = atdb_io.ATDB_IO(ATDB_HOST_PROD, ALTA_HOST_PROD, "atdb", "V5Q3ZPnxm3uj", None)

    if (args.operation=='list_dataproducts'):
        list_dataproducts_status(args.taskid, args.status)
        return

    if (args.operation=='count_dataproducts'):
        count_dataproducts()
        return

    if (args.operation=='add_dataproduct'):
        add_dataproduct()
        return

    if (args.operation=='add_dataproducts'):
        add_dataproducts()
        return

    if (args.operation=='get_observation'):
        get_observation(args.taskid)
        return

    if (args.operation=='query_database'):
        query_database(args.host)
        return

    if (args.operation=='get_size_per_date'):
        get_size_per_date_dataproducts(args.taskid_start, args.taskid_end, args.status)
        return

    if (args.operation=='list_observations'):
        list_observations(args.status)
        return

    if (args.operation=='change_status'):
        change_status(args.taskid, args.status)
        return

    if (args.operation=='set_ingest_progress'):
        set_ingest_progress(args.taskid, args.value)
        return

if __name__ == "__main__":
        main()
