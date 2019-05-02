
# [START gae_flex_quickstart]
import json
import math
import time
import os

import flask

from gcloud import storage as gcs

app = flask.Flask(__name__)

TIMESTAMP_STEP_SIZE = 10800

DEBUG = False
JOB_HEX = ""

os.chdir("/home/cc/gcloud/")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "my-test-project-3fc9b0714516.json"
BUCKET = gcs.Client(project="my-test-project-239201").get_bucket("datastorm")


def truncate(number, digits) -> float:
    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper


def get_data(prefix):
    data_blob = BUCKET.blob(prefix + "_" + JOB_HEX)
    ret_string = data_blob.download_as_string().decode("utf-8")
    return json.loads(ret_string)


def get_water():
    data_blob = BUCKET.blob("is_water.json")
    ret_string = data_blob.download_as_string().decode("utf-8")
    return json.loads(ret_string)


def update_state():
    data_blob = BUCKET.blob("state_" + JOB_HEX)
    data_blob.upload_from_string("done")


def store_results(raw_dict):
    json_payload = json.dumps(raw_dict)
    data_blob = BUCKET.blob("results_" + JOB_HEX)
    data_blob.upload_from_string(json_payload)


@app.route("/")
def index():
    return "CSE 546 Group 1 - StormCloud"


@app.route("/status")
def status():
    return "running!"


@app.route("/process/<job_hex>")
def process(job_hex):
    global JOB_HEX

    start_time = time.time()

    JOB_HEX = job_hex

    # create an spatiotemporal index scaffolding for the current context
    context = get_data("context")

    # job info, including sampling
    job_details = get_data("job")
    job_variables = job_details["variables"]  # scaling_factor (used), evaporation_rate, transpiration_rate

    # load land/water legend
    is_water = get_water()

    # create hierarchical index for results

    # temporal first
    begin = context["temporal"]["begin"]
    end = context["temporal"]["end"]
    times = []
    while begin <= end:
        times.append(begin)
        begin = begin + TIMESTAMP_STEP_SIZE

    # then longitude
    lons = []
    west = truncate(context["spatial"]["left"], 1)
    east = truncate(context["spatial"]["right"], 1)
    while west <= east:
        lons.append(west)
        west = truncate(west + context["spatial"]["x_resolution"], 1)

    # then latitude
    lats = []
    top = truncate(context["spatial"]["top"], 1)
    bottom = truncate(context["spatial"]["bottom"], 1)
    while top >= bottom:
        lats.append(top)
        top = truncate(top - context["spatial"]["y_resolution"], 1)

    # set up zero water depths within the scaffold
    water_depth = dict()
    for timestamp in times:
        water_depth[timestamp] = dict()
        for lon in lons:
            water_depth[timestamp][lon] = dict()
            for lat in lats:
                water_depth[timestamp][lon][lat] = 0.0

    # load data from job gateway folder (only from hurricane, which I expect to see)
    dsfr_data = get_data("dsfr")

    # for each time step, compute the water that arrived in that step
    print("DSFR count: {0}".format(len(dsfr_data)))
    with_rain = 0
    for dsfr in dsfr_data:
        lon, lat = dsfr["coordinate"]

        lon = truncate(lon, 1)  # force coordinates into a format I can use
        lat = truncate(lat, 1)

        ts = dsfr["timestamp"]
        rainfall = dsfr["observation"][0]  # rainfall is in the first key
        lon_key = str(truncate(lon, 1))
        lat_key = str(truncate(lat, 1))
        if lon_key not in is_water:
            continue  # TODO fix this, scrape more data
        if lat_key not in is_water[lon_key]:
            continue  # TODO fix this, scrape more data
        if is_water[lon_key][lat_key]:
            continue  # no rainfall on the ocean
        try:
            if rainfall != 0:
                with_rain = with_rain + 1
                #print(str(ts) + "\t" + str(lon) + "\t" + str(lat) + "\t" + str(rainfall))  # *******
            water_depth[ts][truncate(lon, 1)][truncate(lat, 1)] = rainfall * job_variables["scaling_factor"]
        except Exception as e:
            log(e)

    print("DSFRs with rain: {0}".format(with_rain))

    # add each time step's depth to the sum of the previous time steps (water accumulates)
    for i in range(1, len(times)):
        timestamp = times[i]
        last_timestamp = times[i - 1]
        for lon in lons:
            for lat in lats:
                lon_key = str(truncate(lon, 1))
                lat_key = str(truncate(lat, 1))
                if lon_key not in is_water:
                    continue  # TODO fix this, scrape more data
                if lat_key not in is_water[lon_key]:
                    continue  # TODO fix this, scrape more data
                if is_water[lon_key][lat_key]:
                    continue  # no accumulation on ocean

                rain_this_step = water_depth[timestamp][lon][lat]
                #if rain_this_step != 0:
                    #print(str(time) + "\t" + str(lon) + "\t" + str(lat) + "\t" + str(rain_this_step))  # *******
                last_depth = water_depth[last_timestamp][lon][lat]
                water_depth[timestamp][lon][lat] = \
                    rain_this_step + last_depth

    # package results for storage by the job gateway
    record_list = []
    for timestamp in times:
        for lon in lons:
            for lat in lats:
                record = dict()
                record["timestamp"] = timestamp
                record["coordinate"] = [lon, lat]
                depth = water_depth[timestamp][lon][lat]
                if depth == 0:
                    continue  # exclude results with no water
                record["observation"] = [depth]
                record_list.append(record)

    store_results(record_list)
    update_state()

    log("JobGateway is all done! Results have been saved.")


def log(my_string):
    try:
        with open("water_log.txt", "a") as errorlog:
            errorlog.write(json.dumps(str(my_string)) + "\n")
    except FileNotFoundError:
        print("No log file found.")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

# [END gae_flex_quickstart]
