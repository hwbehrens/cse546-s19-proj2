# the goal of this program is to provide a stand-alone, self-contained, and simple way for
# non-Python applications to update their status in the state database without having to
# know anything about Mongo, SSH, etc.

import json
import os
import shutil
import sys
import time
import urllib.request

from bson.json_util import loads, dumps
from bson.objectid import ObjectId
from gcloud import storage as gcs
from pymongo import MongoClient as PyMongoClient
from sshtunnel import SSHTunnelForwarder

DEBUG = False
MONGO_SERVER = None
MONGO_CLIENT = None
STATE_CONFIG = None
EXIT_CODE = 1
ROOT_PATH = "/home/cc/job_gateway/"

CURR_JOB_HEX = None


def log(my_string):
    try:
        with open("gateway_log.txt", "a") as errorlog:
            errorlog.write(json.dumps(str(my_string)) + "\n")
    except FileNotFoundError:
        print("No log file found.")


#
# def recursive_strip_objectid(thing_to_clean):
#     if type(thing_to_clean) == dict:
#         new_dict = dict()
#         for each in thing_to_clean.keys():
#             new_dict[each] = recursive_strip_objectid(thing_to_clean[each])
#         return new_dict
#     elif type(thing_to_clean) == list:
#         new_list = []
#         for each in thing_to_clean:
#             new_list.append(recursive_strip_objectid(each))
#         return new_list
#     elif type(thing_to_clean) == ObjectId:
#         return "obid-" + str(thing_to_clean)
#     else:
#         return thing_to_clean  # no need to clean


def main():
    global MONGO_SERVER, MONGO_CLIENT, STATE_CONFIG

    if MONGO_CLIENT is None:
        log("Establishing Mongo connection...")

        # read the configuration file from next to this file
        with open(ROOT_PATH + "mongo.json") as json_data:
            raw_data = json_data.read().replace('\\"', '\"')
            mongo_config = loads(raw_data)

        # connect to the mongo database using that configuration
        MONGO_SERVER = SSHTunnelForwarder((mongo_config["mongo_ip"], int(mongo_config["ssh_port"])),
                                          ssh_pkey=(ROOT_PATH + mongo_config["ssh_key"]),
                                          ssh_username=mongo_config["ssh_username"],
                                          remote_bind_address=('127.0.0.1', 27017),
                                          )
        MONGO_SERVER.start()  # open the SSH tunnel to the mongo server
        MONGO_CLIENT = PyMongoClient('127.0.0.1', MONGO_SERVER.local_bind_port)  # connect to mongo

        with open(ROOT_PATH + "instance.json") as json_data:
            raw_data = json_data.read().replace('\\"', '\"')
            STATE_CONFIG = loads(raw_data)

        log("Mongo connection established!")

    # implicitly a fetch job call from EM
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (ROOT_PATH + "gauth.json")
    fetch_job()

    log("Closing mongo connection...")
    MONGO_SERVER.stop()
    log("Mongo connection terminated.")


def _get_st_context():
    ds_config = MONGO_CLIENT.ds_config.collection.find_one()
    my_model = STATE_CONFIG["model_type"]
    kepler_state = MONGO_CLIENT.ds_state.kepler.find_one({"model_type": my_model})

    temporal = kepler_state["temporal_context"]
    temporal["window_size"] = ds_config["model"][my_model]["input_window"]
    temporal["shift_size"] = ds_config["model"][my_model]["shift_size"]

    spatial = ds_config["simulation_context"]["spatial"]
    spatial["x_resolution"] = ds_config["model"][my_model]["x_resolution"]
    spatial["y_resolution"] = ds_config["model"][my_model]["y_resolution"]

    return spatial, temporal


def fetch_job():
    global EXIT_CODE, CURR_JOB_HEX

    gcds_client = gcs.Client(project="my-test-project-239201")
    ds_bucket = gcds_client.get_bucket("datastorm")

    log("Fetching job...")

    # pull the current job from the 'running' queue, and write to disk as a JSON file

    # are we done working on the previous job? let's check
    # if not DEBUG and STATE_CONFIG["current_job"] is not None:
    #     log("Cannot fetch a new job until previous job is completed.")
    #     return

    instance_id = STATE_CONFIG["instance_id"]
    model_type = STATE_CONFIG["model_type"]

    curr_state = MONGO_CLIENT.ds_state.cluster. \
        find_one({"instance": instance_id, "model_type": model_type})  # only one cluster state

    if curr_state is None:
        log("Could not access instance ID {0} with model {1}; "
            "check your instance.json and try again.".
            format(instance_id, model_type))
        return

    jobs = curr_state["pool"]["waiting"]  # these are the currently pending jobs

    if len(jobs) == 0:
        log("No jobs pending - skipping.")
        return

    # fetch and store the job details
    job_id = jobs[0]
    CURR_JOB_HEX = str(job_id)
    job_data = MONGO_CLIENT.ds_results.jobs.find_one({"_id": job_id})
    log("Job found! Fetching and storing job {0} as 'job.json'...".format(job_id))

    job_blob = ds_bucket.blob("job" + "_" + CURR_JOB_HEX)
    job_blob.upload_from_string(dumps(job_data))

    # determine the upstream jobs
    upstream_models = MONGO_CLIENT.ds_config.collection.find_one()
    my_model = STATE_CONFIG["model_type"]
    upstream_models = upstream_models["model"][my_model]["upstream_models"]
    if len(upstream_models) == 0:
        upstream_models = ["undefined"]  # there will always be upstream data, to set the context

    log("Upstream models fetched:\n" + str(upstream_models))

    # fetch any necessary data for the job (model-specific!)
    for model in upstream_models:
        log("Caching input data from {0}...".format(model))

        # there will be min(1, [number of upstream models]) DSARs, and >=0 DSIRs and DSFRs
        model_dict = dict()
        for key in ["dsar", "dsir", "dsfr"]:
            model_dict[key] = dict()

        # pull from mongo into memory (possible bottleneck here!)
        dsar_list = job_data["input_dsars"]
        for dsar_id in dsar_list:
            dsar_details = MONGO_CLIENT.ds_results.dsar.find_one({"_id": dsar_id})
            if dsar_details["metadata"]["model_type"] != model:
                continue  # don't store results for other models, obviously
            model_dict["dsar"][str(dsar_id)] = dsar_details
            for dsir_id in dsar_details["children"]:
                dsir_details = MONGO_CLIENT.ds_results.dsir.find_one({"_id": dsir_id})
                model_dict["dsir"][str(dsir_id)] = dsir_details

                # simply dump DSFRs, for speed
                model_dict["dsfr"] = list(MONGO_CLIENT.ds_results.dsfr.find({"parent": dsir_id}))

        # check and repair directory structures
        if not os.path.isdir(ROOT_PATH + "output_data"):
            os.mkdir(ROOT_PATH + "output_data")  # missing "output_data"
        path = os.path.join(ROOT_PATH + "input_data")
        if not os.path.isdir(path):
            os.mkdir(path)  # missing "input_data"
        path = os.path.join(path, model)
        if not os.path.isdir(path):
            os.mkdir(path)  # missing [model_name]

        # store to disk
        for key in ["dsar", "dsir", "dsfr"]:
            outpath = os.path.join(path, key + ".json")
            log("Writing to " + str(outpath))
            outdata = model_dict[key]
            if not outdata:
                continue  # empty dicts don't get stored

            data_blob = ds_bucket.blob(key + "_" + CURR_JOB_HEX)
            data_blob.upload_from_string(dumps(outdata))

            log("... dumped {0} file".format(key))

    log("All input data cached locally.")

    # fetch and store the window context
    job_context = dict.fromkeys(["spatial", "temporal"])
    job_context["spatial"], job_context["temporal"] = _get_st_context()
    context_blob = ds_bucket.blob("context" + "_" + CURR_JOB_HEX)
    context_blob.upload_from_string(dumps(job_context))
    log("Context saved.")

    # update state to indicate this instance is now busy
    STATE_CONFIG["current_job"] = job_id
    state_blob = ds_bucket.blob("state_" + CURR_JOB_HEX)
    state_blob.upload_from_string("ready")

    # # move the job from the inbox to "running" and update remote state
    curr_state["pool"]["waiting"].remove(job_id)
    curr_state["pool"]["running"] = [job_id]
    curr_state["status"] = "running"
    curr_state["time_updated"] = time.time()
    if not DEBUG:
        MONGO_CLIENT.ds_state.cluster.save(curr_state)
        log("Mongo instance state updated.")

    log("Ready for processing!" +
        " Don't forget to save your timestamped DSIRs as you go, and your DSAR at the end.")

    url_to_call = "https://my-test-project-239201.appspot.com/process/{0}".format(CURR_JOB_HEX)
    complete = urllib.request.urlopen(url_to_call).read().decode()
    if "success" in complete:
        finish_job()
    else:
        log("ERROR WITH APP ENGINE CALL:\n{0}".format(complete))

    EXIT_CODE = 0


# noinspection PyTypeChecker
def finish_job():
    global EXIT_CODE

    log("Finishing job...")

    # see if it's ready to finish, if not return false
    gcds_client = gcs.Client(project="my-test-project-239201")
    ds_bucket = gcds_client.get_bucket("datastorm")
    state_blob = ds_bucket.get_blob("state_" + CURR_JOB_HEX)

    curr_state = state_blob.download_as_string().decode("utf-8")
    if curr_state != "done":
        return False

    # take the current job in the 'running' queue, write some result to the ds_results database,
    #      move it to the 'done' queue, and update the local state

    # are we done working on the previous job? let's  check
    # if STATE_CONFIG["current_job"] is None:
    #     log("No currently-running job detected; nothing to finish!")
    #     return

    # ensure local state and server state agree (if not, two instances have the same id - bad!)
    instance_id = STATE_CONFIG["instance_id"]
    local_id = ObjectId(CURR_JOB_HEX)
    my_model = STATE_CONFIG["model_type"]
    curr_state = MONGO_CLIENT.ds_state.cluster.find_one({"instance": instance_id})
    # server_id = curr_state["pool"]["running"][0]
    # if local_id != server_id:
    #     log("Local job id ({0}) and server job id ({1}) do not match! Fix this.")
    #     return
    job_id = local_id

    log("Error checks passed, continuing...")

    # fetch some details
    instance_state = MONGO_CLIENT.ds_state.cluster.find_one({"instance": instance_id})

    # generate a DSIR to receive the keys
    new_dsir_id = ObjectId()  # need to store this to establish
    new_dsir = dict()
    new_dsir["_id"] = new_dsir_id
    new_dsir["parent"] = None  # this is decided by the SyncManager
    new_dsir["metadata"] = dict()
    new_dsir["metadata"]["spatial"], new_dsir["metadata"]["temporal"] = _get_st_context()
    new_dsir["metadata"]["model_type"] = my_model
    new_dsir["metadata"]["job_id"] = job_id

    # set the linkage from the job to the DSIR (reverse horizontal linkage)
    job_details = MONGO_CLIENT.ds_results.jobs.find_one({"_id": job_id})
    job_details["output_dsir"] = new_dsir_id
    MONGO_CLIENT.ds_results.jobs.save(job_details)

    log("DSIR skeleton constructed, loading data...")

    # load the data to upload (assume a single json file with a list of dicts)
    # and pre-process to set context and prepare for upload
    # MODEL MUST PROVIDE: timestamp, coordinate, observation
    timestamp_set = set()

    result_blob = ds_bucket.get_blob("results_" + CURR_JOB_HEX)
    raw_json = result_blob.download_as_string().decode("utf-8")
    results = loads(raw_json)

    for entry in results:
        # mutate dictionaries in-place
        timestamp_set.add(entry["timestamp"])
        entry["model_type"] = my_model
        entry["parent"] = new_dsir_id
        entry["_id"] = ObjectId()
    new_dsir["timestamp_list"] = sorted(list(timestamp_set))

    log("DSFRs loaded into memory and mutated, uploading...")

    # do the uploads
    MONGO_CLIENT.ds_results.dsfr.insert_many(results)
    MONGO_CLIENT.ds_results.dsir.save(new_dsir)

    log("DSFRs and DSIR uploaded! Cleaning up...")

    # add the new DSIR id to the "to_sync" pool
    kepler_state = MONGO_CLIENT.ds_state.kepler.find_one({"model_type": my_model})
    kepler_state["result_pool"]["to_sync"].append(new_dsir_id)
    MONGO_CLIENT.ds_state.kepler.save(kepler_state)
    log("Kepler state updated for SyncManager...")

    # update the instance state remotely
    instance_state["pool"]["running"] = []
    instance_state["status"] = "idle"
    curr_state["time_updated"] = time.time()
    MONGO_CLIENT.ds_state.cluster.save(instance_state)
    log("Cluster state updated...")

    # finally, let's delete blobs
    ds_bucket.delete_blob("job_" + CURR_JOB_HEX)
    ds_bucket.delete_blob("state_" + CURR_JOB_HEX)
    ds_bucket.delete_blob("context_" + CURR_JOB_HEX)
    ds_bucket.delete_blob("results_" + CURR_JOB_HEX)
    ds_bucket.delete_blob("dsar_" + CURR_JOB_HEX)
    ds_bucket.delete_blob("dsir_" + CURR_JOB_HEX)
    ds_bucket.delete_blob("dsfr_" + CURR_JOB_HEX)

    log("Local instance state updated...")

    # delete old inputs and outputs locally
    for folder in [ROOT_PATH + "input_data", ROOT_PATH + "output_data"]:
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
    log("Old files removed...")

    log("Job {0} was finished successfully!".format(str(job_id)))

    EXIT_CODE = 0
    return True


if __name__ == "__main__":
    input_command = None

    if DEBUG:
        ROOT_PATH = "./"
        log("***** RUNNING DEBUG *****")

    try:
        main()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        log("Encountered error with command {0}:\nLine {1}\n{2}".format(input_command, exc_tb.tb_lineno, e))

    if DEBUG:
        log("***** FINISHED DEBUG *****")

    sys.exit(EXIT_CODE)
