from __future__ import print_function
import glob
import json
import logging
import multiprocessing
import os
import subprocess
import time

import boto3
import numpy
import pandas
import scipy.stats
import skimage.exposure
import skimage.io
import skimage.transform
import watchtower

#################################
# CONSTANT PATHS IN THE CONTAINER
#################################

DATA_ROOT = "/home/ubuntu/bucket"
LOCAL_OUTPUT = "/home/ubuntu/local_output"
QUEUE_URL = os.environ["SQS_QUEUE_URL"]
AWS_BUCKET = os.environ["AWS_BUCKET"]
LOG_GROUP_NAME= os.environ["LOG_GROUP_NAME"]
CHECK_IF_DONE_BOOL= os.environ["CHECK_IF_DONE_BOOL"]
EXPECTED_NUMBER_FILES= os.environ["EXPECTED_NUMBER_FILES"]
if "MIN_FILE_SIZE_BYTES" not in os.environ:
    MIN_FILE_SIZE_BYTES = 1
else:
    MIN_FILE_SIZE_BYTES = int(os.environ["MIN_FILE_SIZE_BYTES"])
if "USE_PLUGINS" not in os.environ:
    USE_PLUGINS = "False"
else:
    USE_PLUGINS = os.environ["USE_PLUGINS"]
if "NECESSARY_STRING" not in os.environ:
    NECESSARY_STRING = False
else:
    NECESSARY_STRING = os.environ["NECESSARY_STRING"]

#################################
# CLASS TO HANDLE THE SQS QUEUE
#################################

class JobQueue():

    def __init__(self, queueURL):
        self.client = boto3.client("sqs")
        self.queueURL = queueURL
    
    def readMessage(self):
        response = self.client.receive_message(QueueUrl=self.queueURL, WaitTimeSeconds=20)
        if "Messages" in response.keys():
            data = json.loads(response["Messages"][0]["Body"])
            handle = response["Messages"][0]["ReceiptHandle"]
            return data, handle
        else:
            return None, None

    def deleteMessage(self, handle):
        self.client.delete_message(QueueUrl=self.queueURL, ReceiptHandle=handle)
        return

    def returnMessage(self, handle):
        self.client.change_message_visibility(QueueUrl=self.queueURL, ReceiptHandle=handle, VisibilityTimeout=60)
        return

#################################
# AUXILIARY FUNCTIONS
#################################

def monitorAndLog(process,logger):
    while True:
        output= process.stdout.readline().decode()
        if output== "" and process.poll() is not None:
            break
        if output:
            print(output.strip())
            logger.info(output)  

def printandlog(text,logger):
    print(text)
    logger.info(text)

def loadConfig(configFile):
    data = None
    with open(configFile, "r") as conf:
        data = json.load(conf)
    return data

def download_data(args):
    bucket,bucket_prefix,local_prefix = args[0]
    if not os.path.exists(os.path.split(local_prefix)[0]):
        os.makedirs(os.path.split(local_prefix)[0],exist_ok=True)
    s3temp = boto3.client("s3")
    s3temp.download_file(bucket,bucket_prefix,local_prefix)

def file_download_generator(df,bucketname):
    for i, row in df.iterrows():
        yield [bucketname]+row.to_list()

#################################
# IMAGE PREPROCESSING FUNCTIONS
#################################

#mostly taken from the DeepProfiler function, we're just batching differently
def preprocess_image(args):
    image_path, illum_file, preprocess = args
    image = skimage.io.imread(image_path)

    if illum_file != None:
        illum = numpy.load(illum_file)
        image = image/illum
    if preprocess:
        vmin, vmax = scipy.stats.scoreatpercentile(image, (0.05, 99.95))
        image = skimage.exposure.rescale_intensity(image, in_range=(vmin, vmax))
        
        image = skimage.img_as_ubyte(image)
        image_path = image_path[:image_path.index(".")]+".png"

    skimage.io.imsave(image_path, image)
    return image_path

################################################################################
## PARALLELIZATION UTILITIES
################################################################################

class Parallel():

    def __init__(self, fixed_args, numProcs=None):
        self.fixed_args = fixed_args
        cpus =  multiprocessing.cpu_count()
        if numProcs is None or numProcs > cpus or numProcs < 1:
            numProcs = cpus
        self.pool = multiprocessing.Pool(numProcs)

    def compute(self, operation, data):
        iterable = [ [d, self.fixed_args] for d in data ]
        return self.pool.map(operation, iterable)

    def close(self):
        self.pool.close()
        self.pool.join()    

#################################
# RUN DEEP PROFILER
#################################

def runSomething(message):
    #List the directories in the bucket- this prevents a strange s3fs error
    rootlist=os.listdir(DATA_ROOT)
    for eachSubDir in rootlist:
        subDirName=os.path.join(DATA_ROOT,eachSubDir)
        if os.path.isdir(subDirName):
            trashvar=os.system("ls "+subDirName)

    # Configure the logs
    logger = logging.getLogger(__name__)

    # Parse your message somehow to pull out a name variable that's going to make sense to you when you want to look at the logs later
    group_to_run = message["group"]
    groupkeys = list(group_to_run.keys())
    groupkeys.sort()
    metadataID = "-".join(groupkeys)

    # Add a handler with 
    watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=str(metadataID),create_log_group=False)
    logger.addHandler(watchtowerlogger)

    
    remoteOut = os.path.join(message["default_parameters"]["output_directory"],metadataID)
    localIn = os.path.join(LOCAL_OUTPUT,metadataID)
    if not os.path.exists(localIn):
          os.makedirs(localIn,exist_ok=True)
    localOut = os.path.join(LOCAL_OUTPUT,metadataID,"outputs")
    if not os.path.exists(localOut):
          os.makedirs(localOut,exist_ok=True)
    
    # See if this is a message you've already handled, if you've so chosen
    if CHECK_IF_DONE_BOOL.upper() == "TRUE":
        try:
            s3client=boto3.client("s3")
            bucketlist=s3client.list_objects(Bucket=AWS_BUCKET,Prefix=remoteOut+"/")
            objectsizelist=[k["Size"] for k in bucketlist["Contents"]]
            objectsizelist = [i for i in objectsizelist if i >= MIN_FILE_SIZE_BYTES]
            if NECESSARY_STRING:
                if NECESSARY_STRING != "":
                    objectsizelist = [i for i in objectsizelist if NECESSARY_STRING in i]
            if len(objectsizelist)>=int(EXPECTED_NUMBER_FILES):
                printandlog("File not run due to > expected number of files",logger)
                logger.removeHandler(watchtowerlogger)
                return "SUCCESS"
        except KeyError: #Returned if that folder does not exist
            pass	

    # Let's do this, shall we?
    s3 = boto3.client("s3")
    remote_root = os.path.join(message["project_path"],message["default_parameters"]["root_path"],message["experiment_name"],"inputs")
    
    # get the config, put it somewhere called config_location, and load it
    remote_config = os.path.join(remote_root,message["default_parameters"]["config_file"])
    config_location = os.path.join(localIn,"inputs",message["default_parameters"]["config_file"])
    if not os.path.exists(os.path.split(config_location)[0]):
          os.makedirs(os.path.split(config_location)[0],exist_ok=True)
    s3.download_file(AWS_BUCKET,remote_config,config_location)
    config = loadConfig(config_location)
    printandlog("Loaded config file",logger)
    
    process = Parallel(config, numProcs=message["default_parameters"]["cores"])
    
    channels = config["dataset"]["images"]["channels"]
    
    # get the index csv, put it somewhere called csv_location
    remote_csv = os.path.join(remote_root,message["default_parameters"]["index_file"])
    csv_location = os.path.join(localIn,"inputs",message["default_parameters"]["index_file"])
    if not os.path.exists(os.path.split(csv_location)[0]):
          os.makedirs(os.path.split(csv_location)[0],exist_ok=True)
    s3.download_file(AWS_BUCKET,remote_csv,csv_location)
    printandlog("Downloaded index file",logger)

    # parse the csv
    df = pandas.read_csv(csv_location)
    for eachkey in groupkeys:
       df = df[df[eachkey]==group_to_run[eachkey]]
    sitecount = df.shape[0]
    printandlog("Parsed CSV, found "+str(sitecount)+" sites to run",logger)
    
    # get the location files based on the parsed index file
    remote_location_folder = os.path.join(remote_root,message["default_parameters"]["single_cells"])
    local_location_folder = os.path.join(localIn,"inputs/locations")
    if not os.path.exists(os.path.split(local_location_folder)[0]):
          os.makedirs(os.path.split(local_location_folder)[0],exist_ok=True)
    location_file_mapping = message["default_parameters"]["filename_used_for_locations"]
    df_metadata_keys = [x for x in df.columns if "Metadata_" in x if x in location_file_mapping]
    to_run = []
    #may need to parallelize this better later, for now let's get it running
    for i, row in df.iterrows():
        formatted = location_file_mapping
        for eachmetadata in df_metadata_keys:
            formatted = formatted.replace(eachmetadata,row[eachmetadata])
        to_run.append(formatted)
    location_df = pandas.DataFrame({"remote":to_run,"local":to_run})
    location_df["remote"]=remote_location_folder+"/"+location_df["remote"]
    location_df["local"]=local_location_folder+"/"+location_df["remote"]
    to_dl = file_download_generator(location_df,AWS_BUCKET)
    process.compute(download_data,to_dl)
    printandlog("Downloaded location files",logger)

    # get the image files based on the parsed index file 
    remote_image = os.path.join(message["project_path"],message["batch_name"],"images")
    image_location = os.path.join(localIn,"inputs","images")
    do_illum = message["apply_illum"].lower() == "true"
    if do_illum:
        platelist = list(df['Metadata_Plate'].unique())
        remote_illum = os.path.join(message["project_path"],message["batch_name"],"illum")
        illum_files = [x["Key"] for x in s3.list_objects(Bucket=AWS_BUCKET,Prefix=remote_illum)["Contents"]]
        illum_mapping_remote = {(eachchannel,eachplate):x for x in illum_files for eachchannel in channels for eachplate in platelist if eachchannel in x if eachplate in x}
        illum_mapping_local = {k:os.path.join(image_location,illum_mapping_remote[k].split(message["batch_name"])[1]) for k in list(illum_mapping_remote.keys())}
        illum_mapping_remote_values = list(illum_mapping_remote.values())
        illum_mapping_local_values = [os.path.join(image_location,k.split(message["batch_name"])[1]) for k in illum_mapping_remote_values]
        illum_df = pandas.DataFrame({"remote":illum_mapping_remote_values,"local":illum_mapping_local_values},columns=["remote","local"])
        to_dl = file_download_generator(illum_df,AWS_BUCKET)
        process.compute(download_data,to_dl)
        printandlog("Downloaded illum data",logger)

    for eachchannel in channels:
        temp_df = pandas.DataFrame()
        temp_df["remote_"+eachchannel] = remote_image+"/"+df[eachchannel]
        temp_df["local_"+eachchannel] = image_location+"/"+df[eachchannel]
        to_dl = file_download_generator(temp_df,AWS_BUCKET)
        process.compute(download_data,to_dl)
        printandlog("Downloaded "+eachchannel,logger)
        df[eachchannel] = temp_df["local_"+eachchannel]

    do_preprocess = message["preprocess"].lower() == "true"

    if do_illum or do_preprocess:
        #see if we can chuck the forloops later for Parallel (TODO)
        #just trying to figure out what we actually need to do at first
        for eachchannel in channels:
            for eachplate in platelist:
                if do_illum:
                    illum_file = illum_mapping_local[(eachchannel,eachplate)]
                else:
                    illum_file = None
                for eachimage in df[eachchannel]: 
                    preprocess_image([eachimage,illum_file,do_preprocess])
                if do_preprocess:
                    sample_file_name = list(df[eachchannel])[0]
                    extension = sample_file_name[sample_file_name.index("."):]
                    if extension!= ".png":
                        df[eachchannel].replace(extension,".png",regex=True,inplace=True)

    df.to_csv(csv_location,index=False)

    cmd = "python3 deepprofiler --root="+localIn+" --config="+config_location+" profile" 

    print("Running", cmd)
    logger.info(cmd)
    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    monitorAndLog(subp,logger)

    # Check if we have the expected number of sites, and if so, say done
    # I don't know what happens when a site has 0 cells (or if it's possible for that to happen here)
    # If we figure out a site can have 0 cells, and if so that it does not make an npz, we'll have to do something
    # I suspect in that case the right thing will be to match the glob to the expected site list, then query the locations file for any missing sites
    nsites = len(glob.glob(os.path.join(localOut,"outputs","results","features","**","*.npz"),recursive=True))
    done = nsites == sitecount
    
    # Get the outputs and move them to S3
    if done:
        time.sleep(30)
        mvtries=0
        while mvtries <3:
            try:
                    printandlog("Move attempt #"+str(mvtries+1),logger)
                    cmd = "aws s3 mv " + localOut + " s3://" + AWS_BUCKET + "/" + remoteOut + " --recursive" 
                    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
                    out,err = subp.communicate()
                    out=out.decode()
                    err=err.decode()
                    printandlog("== OUT \n"+out, logger)
                    if err == "":
                        break
                    else:
                        printandlog("== ERR \n"+err,logger)
                        mvtries+=1
            except:
                printandlog("Move failed",logger)
                printandlog("== ERR \n"+err,logger)
                time.sleep(30)
                mvtries+=1
        if mvtries < 3:
            printandlog("SUCCESS",logger)
            logger.removeHandler(watchtowerlogger)
            return "SUCCESS"
        else:
            printandlog("SYNC PROBLEM. Giving up on trying to sync "+metadataID,logger)
            import shutil
            shutil.rmtree(localIn, ignore_errors=True)
            logger.removeHandler(watchtowerlogger)
            return "PROBLEM"
    else:
        printandlog("PROBLEM: Failed exit condition for "+metadataID,logger)
        logger.removeHandler(watchtowerlogger)
        import shutil
        shutil.rmtree(localIn, ignore_errors=True)
        return "PROBLEM"
    

#################################
# MAIN WORKER LOOP
#################################

def main():
    queue = JobQueue(QUEUE_URL)
    # Main loop. Keep reading messages while they are available in SQS
    while True:
        msg, handle = queue.readMessage()
        if msg is not None:
            result = runSomething(msg)
            if result == "SUCCESS":
                print("Batch completed successfully.")
                queue.deleteMessage(handle)
            else:
                print("Returning message to the queue.")
                queue.returnMessage(handle)
        else:
            print("No messages in the queue")
            break

#################################
# MODULE ENTRY POINT
#################################

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Worker started")
    main()
    print("Worker finished")

