from flask import Flask, request, jsonify, json, abort, session
from flask_cors import CORS, cross_origin
import numpy as np

import dateutil.parser
import requests
import pandas as pd
import re
import time
import datetime
import csv
import pytz

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

methods = ('GET', 'POST')
local_tz = pytz.timezone('Pacific/Auckland')
metric_finders= {}
metric_readers = {}
annotation_readers = {}
panel_readers = {}
Splat_Header_Request = ""
Splat_Header_Get_Data = ""


# ********************** Used to convert Grafana UTC timestamps back to NZ timestamps *************************

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

#********************************************** Insert Generated Data in a veriable called GrafanaResponseArray
def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


#**********************************************************************************************************************************************************
#**********************************************************************************************************************************************************
#*************************************** Based on the URLs passed to this function, the function will attempt to pull *************************************
#************************************** data off the Pieman server and reformat the data into something that Grafana **************************************
#****************************************************************    can work with  ***********************************************************************
#**********************************************************************************************************************************************************


def get_formatted_data_from_splat(url_q, url):
   
    #url_q = "http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=config&action=update&MCM188=on&MCM204=on&MCM216=on"
    #         http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=config&action=update&MCM216=on&MCM204=on&MCM216=on
    #         http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=config&action=update&MCM216=on&MCM204=on&MCM216=on
    #url =   "http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=download&start=20191120&startTime=04&end=20191120&endTime=10&download_period=5&csv=Download+CSV"
    #         http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=download&start=20191202&startTime=23&end=20191203&endTime=11&download_period=5&csv=Download+CSV
    #         http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=download&start=20191204&startTime=03&end=20191204&endTime=09&download_period=5&csv=Download+CSV
    #         http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=download&start=20191204&startTime=04&end=20191204&endTime=10&download_period=5&csv=Download+CSV20191204&startTime=04&end=20191204&endTime=10&download_period=5&csv=Download+CSV
    #         http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=DD609_Double&productSerial=EEG283738&command=download&start=20191204&startTime=04&end=20191204&endTime=10&download_period=5&csv=Download+CSV
    print ("url_q = " + url_q + "\n")
    print ("url = " + url + "\n")
    
    file_directory = "c:/FolderForCSV/"
    cookies_jar = requests.cookies.RequestsCookieJar()

    response_q = requests.get(url_q, allow_redirects=True)
    cookies_jar.update(response_q.cookies)
    print ("Cookie\n")
    print (cookies_jar)
    for cookie in cookies_jar:
        print (cookie.name, cookie.value, cookie.domain)
    
    h = requests.get(url, allow_redirects=True, cookies=cookies_jar)

    
    filename = get_filename_from_cd(h.headers.get('content-disposition'))
    filename = file_directory + filename
    filename = filename.replace('"', '')
    print ("File name = " + filename)
    
    
    longstring = h.content.decode("utf-8")
    lines  = longstring.split('\n')
    
    # Convert each line into a array of formatted values.
    timeseries_pd = []
    dataseries_pd = []
    
    for line in lines:
        row = line.split(',')
        if (len(row)>=3):
            if (row[0]!="Date"):
                if (row[2]!=""):
                    pieman_date_string = row[0].replace("/","-") + "T" + row[1] + ".000000+1300" # Some sort of adjustment for daylight savings time?
                    timeseries_pd.append(np.datetime64(pieman_date_string))
                    dataseries_pd.append(float(row[2]))
    return pd.Series(dataseries_pd, index = timeseries_pd).to_frame('value')  


#**********************************************************************************************************

def left(s, amount):
    return s[:amount]

def mid(s, start, amount):
    return s[start:(start+amount)]    

def add_reader(name, reader):
    metric_readers[name] = reader


def add_finder(name, finder):
    metric_finders[name] = finder


def add_annotation_reader(name, reader):
    annotation_readers[name] = reader


def add_panel_reader(name, reader):
    panel_readers[name] = reader


@app.route('/', methods=methods)
@cross_origin()
def hello_world():
    print (request.headers, request.get_json())
    return 'Jether\'s python Grafana datasource, used for rendering HTML panels and timeseries data.'

@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    print (request.headers, request.get_json())
    req = request.get_json()

    target = req.get('target', '*')

    if ':' in target:
        finder, target = target.split(':', 1)
    else:
        finder = target

    if not target or finder not in metric_finders:
        metrics = []
        if target == '*':
            metrics += metric_finders.keys() + metric_readers.keys()
        else:
            metrics.append(target)

        return jsonify(metrics)
    else:
        return jsonify(list(metric_finders[finder](target)))


def dataframe_to_response(target, df, freq=None):
    response = []

    if df.empty:
        return response

    if freq is not None:
        orig_tz = df.index.tz
        df = df.tz_convert('UTC').resample(rule=freq, label='right', closed='right', how='mean').tz_convert(orig_tz)

    if isinstance(df, pd.Series):
        response.append(_series_to_response(df, target))
    elif isinstance(df, pd.DataFrame):
        for col in df:
            response.append(_series_to_response(df[col], target))
    else:
        abort(404, Exception('Received object is not a dataframe or series.'))

    return response


def dataframe_to_json_table(target, df):
    response = []

    if df.empty:
        return response

    if isinstance(df, pd.DataFrame):
        response.append({'type': 'table',
                         'columns': df.columns.map(lambda col: {"text": col}).tolist(),
                         'rows': df.where(pd.notnull(df), None).values.tolist()})
    else:
        abort(404, Exception('Received object is not a dataframe.'))
    print(response)
    return response


def annotations_to_response(target, df):
    response = []

    # Single series with DatetimeIndex and values as text
    if isinstance(df, pd.Series):
        for timestamp, value in df.iteritems():
            response.append({
                "annotation": target, # The original annotation sent from Grafana.
                "time": timestamp.value // 10 ** 6, # Time since UNIX Epoch in milliseconds. (required)
                "title": value, # The title for the annotation tooltip. (required)
                #"tags": tags, # Tags for the annotation. (optional)
                #"text": text # Text for the annotation. (optional)
            })
    # Dataframe with annotation text/tags for each entry
    elif isinstance(df, pd.DataFrame):
        for timestamp, row in df.iterrows():
            annotation = {
                "annotation": target,  # The original annotation sent from Grafana.
                "time": timestamp.value // 10 ** 6,  # Time since UNIX Epoch in milliseconds. (required)
                "title": row.get('title', ''),  # The title for the annotation tooltip. (required)
            }

            if 'text' in row:
                annotation['text'] = str(row.get('text'))
            if 'tags' in row:
                annotation['tags'] = str(row.get('tags'))

            response.append(annotation)
    else:
        abort(404, Exception('Received object is not a dataframe or series.'))

    return response

def _series_to_annotations(df, target):
    if df.empty:
        return {'target': '%s' % (target),
                'datapoints': []}

    sorted_df = df.dropna().sort_index()
    timestamps = (sorted_df.index.astype(pd.np.int64) // 10 ** 6).values.tolist()
    values = sorted_df.values.tolist()

    return {'target': '%s' % (df.name),
            'datapoints': zip(values, timestamps)}


def _series_to_response(df, target):
    if df.empty:
        return {'target': '%s' % (target),
                'datapoints': []}

    sorted_df = df.dropna().sort_index()
    #print( sorted_df.index )
    try:
        timestamps = (sorted_df.index.astype(pd.np.int64) // 10 ** 6).values.tolist() # New pandas version
    except:
        timestamps = (sorted_df.index.astype(pd.np.int64) // 10 ** 6).tolist()

    values = sorted_df.values.tolist()

  #  return {'target': '%s' % (df.name),
  #          'datapoints': zip(values, timestamps)}
    #print ( {'target': '%s' % (df.name), 'datapoints': list(zip(values, timestamps))})
    #return {'target': '%s' % (df.name),
    #        'datapoints': list(zip(values, timestamps))}
    #return {'target': '%s' % (df.name), 'datapoints': [(174152, 1574200765000), (174152, 1574200770000), (174152, 1574200775000), (174152, 1574200780000), (174152, 1574200785000), (174152, 1574200790000), (174152, 1574200795000), (174152, 1574200800000)]}
    
    return {'target': '%s' % (df.name),
            'datapoints': zip(values, timestamps)}
    #return {'target': '%s' % (target), 'datapoints': []}
    #return get_formatted_data_from_splat(Splat_Header_Request, Splat_Header_Get_Data)

@app.route('/query', methods=methods)
@cross_origin(max_age=600)
def query_metrics(): 
    
    
    print (request.headers, request.get_json())
    req = request.get_json()

    results = []
    
    ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
                '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}

    if 'intervalMs' in req:
        freq = str(req.get('intervalMs')) + 'ms'
    else:
        freq = None

    for target in req['targets']:
        if ':' not in target.get('target', ''):
            abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + target['target']))

        req_type = target.get('type', 'timeserie')

        finder, target = target['target'].split(':', 1)
        if (finder=="machine_details"):
            #print ("The target part of the querry instruction is : " + target)
            Splat_Header_Request = "http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=#ModelType#&productSerial=#SerialType#&command=config&action=update&#Parameter#=on"
            Splat_Header_Get_Data = "http://pieman.fp.co.nz/index.php?meost=&productType=DishWashers&modelType=#ModelType#&productSerial=#SerialType#&command=download&start="
            machine_type, machine_serial_number, machine_parameter = target.split(',', 3)
            #print ("The following split data strings are " + machine_type + " .. " + machine_serial_number + " .. " + machine_parameter)
            Splat_Header_Request = Splat_Header_Request.replace("#ModelType#", machine_type)
            Splat_Header_Request = Splat_Header_Request.replace("#SerialType#", machine_serial_number)
            Splat_Header_Request = Splat_Header_Request.replace("#Parameter#", machine_parameter)
            Splat_Header_Get_Data = Splat_Header_Get_Data.replace("#ModelType#", machine_type)
            Splat_Header_Get_Data = Splat_Header_Get_Data.replace("#SerialType#", machine_serial_number)

            results = []
    
            UTCstartdate = dateutil.parser.parse(req['range']['from'])
            UTCfinishdate = dateutil.parser.parse(req['range']['to'])
            NZstartdate = utc_to_local(UTCstartdate)
            NZfinishdate = utc_to_local(UTCfinishdate)
            print ("The start time is " + NZstartdate.strftime("%m/%d/%Y, %H:%M:%S") + '\n')
            print ("The finish time is " + NZfinishdate.strftime("%m/%d/%Y, %H:%M:%S") + '\n')
            Splat_Header_Get_Data = Splat_Header_Get_Data + NZstartdate.strftime("%Y%m%d") + "&startTime=" + NZstartdate.strftime("%H") + "&end=" + NZfinishdate.strftime("%Y%m%d") + "&endTime=" + NZfinishdate.strftime("%H") + "&download_period=5&csv=Download+CSV"
            #print (target)
            #print ("Modified Splat Header Request = " + Splat_Header_Request + '\n')
            #print ("Modified Splat Header Get Data = " + Splat_Header_Get_Data + '\n') 
            json_target = { 'url_q':Splat_Header_Request, 'url':Splat_Header_Get_Data }
            #print ("JSON dump of json_target")
            #print ( json.dumps(json_target))
            target = json_target
            freq=None
        
        query_results = metric_readers[finder](target, ts_range)  # data needs to returned from here. In this case the statement resolves to 
                                                                  # query_results = get_machine_details(machine_details_arguments, ts_range)
        #print ("Query Results Are : \n")
        #print (query_results)                                                          
        if req_type == 'table':
            results.extend(dataframe_to_json_table(target, query_results))
        else:
            results.extend(dataframe_to_response(target, query_results, freq=freq))
    #print (jsonify(results))
    return jsonify(results)


@app.route('/annotations', methods=methods)
@cross_origin(max_age=600)
def query_annotations():
    print (request.headers, request.get_json())
    req = request.get_json()

    results = []

    ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
                '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}

    query = req['annotation']['query']

    if ':' not in query:
        abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + query))

    finder, target = query.split(':', 1)
    results.extend(annotations_to_response(query, annotation_readers[finder](target, ts_range)))

    return jsonify(results)


@app.route('/panels', methods=methods)
@cross_origin()
def get_panel():
    print (request.headers, request.get_json())
    req = request.args

    ts_range = {'$gt': pd.Timestamp(int(req['from']), unit='ms').to_pydatetime(),
                '$lte': pd.Timestamp(int(req['to']), unit='ms').to_pydatetime()}

    query = req['query']

    if ':' not in query:
        abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + query))

    finder, target = query.split(':', 1)
    return panel_readers[finder](target, ts_range)

    
if __name__ == '__main__':
    # Sample annotation reader : add_annotation_reader('midnights', lambda query_string, ts_range: pd.Series(index=pd.date_range(ts_range['$gt'], ts_range['$lte'], freq='D', normalize=True)).fillna('Text for annotation - midnight'))
    # Sample timeseries reader : 
    def get_sine(freq, ts_range):
                freq = int(freq)
                #print (freq)
                ts = pd.date_range(ts_range['$gt'], ts_range['$lte'], freq='H')
                #print (ts)
                
                return pd.Series(pd.np.sin(pd.np.arange(len(ts)) * pd.np.pi * freq * 2 / float(len(ts))), index=ts).to_frame('value')

    # this routine will catch all the necessary details issued from a Grafana interface
    def get_machine_details(target, ts_range):
        #print ("JSON target dump\n")
        #print (json.dumps(target))
        return get_formatted_data_from_splat(target['url_q'], target['url'])
        

    add_reader('sine_wave', get_sine)
    add_reader('machine_details', get_machine_details)

    # To query the wanted reader, use `<reader_name>:<query_string>`, e.g. 'sine_wave:24' 

    app.run(host='0.0.0.0', port=3003, debug=True)