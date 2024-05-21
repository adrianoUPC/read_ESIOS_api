import urllib
import json
import datetime
import pandas as pd
import numpy as np

# This is how the URL is built
def get_headers(token):
    """
    Prepares the CURL headers
    :return:
    """
    # Prepare the arguments of the call
    headers = dict()
    headers['Accept'] = 'application/json; application/vnd.esios-api-v1+json'
    headers['Content-Type'] = 'application/json'
    headers['Host'] = 'api.esios.ree.es'
    headers['x-api-key'] = token
    headers['Cookie'] = ''
    return headers

def get_query_json(ind, start_str, end_str):
    try:
        url = 'https://api.esios.ree.es/indicators/' + ind + '?start_date=' + start_str + '&end_date=' + end_str
    except:
        url = 'https://api.esios.ree.es/indicators/' + str(ind) + '?start_date=' + start_str + '&end_date=' + end_str
    # Perform the call
    req = urllib.request.Request(url, headers=get_headers(token))
    with urllib.request.urlopen(req) as response:
        try:
            json_data = response.read().decode('utf-8')
        except:
            json_data = response.readall().decode('utf-8')
        result = json.loads(json_data)
    return result


def get_data(ind, start, end):
    dateformat = '%Y-%m-%dT%H:%M:%S'
    # check types: Pass to string for the url
    if type(start) is datetime.datetime:
        start_str = start.strftime(dateformat)
    else:
        start_str = start

    if type(end) is datetime.datetime:
        end_str = end.strftime(dateformat)
    else:
        end_str = end
    # Get the json data
    result = get_query_json(ind, start_str, end_str)

    d = result["indicator"]["values"]
    if len(d) > 0:
        hdr = list(d[0].keys())  # headers
        data = np.empty((len(d), len(hdr)), dtype=object)

        for i in range(len(d)):  # iterate the data entries
            for j in range(len(hdr)):  # iterate the headers
                h = hdr[j]
                val = d[i][h]
                data[i, j] = val

        df = pd.DataFrame(data=data, columns=hdr)  # make the DataFrame
        df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])  # convert to datetime
        df = df.set_index('datetime_utc')  # Set the index column
        return df
    else:
        return None

#%% Testing the functions
token = "0216e69520d75bd95dfd68f582faf42b5b2424839a6d53dbe80881da0625a802"

#%% Dictionary of target indicators (see excel file for full list)
el_price_indicators = {"Marginal price day ahead market": 600,
                       "Real time generation nuclear": 549,
                       "Residual demand forecast": 10249,
                       "Co2-free generation percentage": 10033,
                       "Real time co2 addociated generation": 10355,
                       "Real time generation c.c. gt": 550,
                       "Peninsular wind power generation forecast": 541,
                       "Forecasted demand": 544
                       }

#%% Get API files from indicator number
indicators = [600, 549]
#indicators = [el_price_indicators[key] for key in el_price_indicators.keys()]

end = datetime.datetime.today() - datetime.timedelta(days=1)
start = end - datetime.timedelta(days=3)

df_dict = dict()
for ind in indicators:
    df_new = get_data(ind, start, end)
    if df_new is not None:
        df_new.rename(columns={'value': str(ind)}, inplace=True)
    df_dict[ind] = df_new
