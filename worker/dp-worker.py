from __future__ import print_function
import glob
import json
import logging
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

from deepprofiler.dataset.utils import Parallel

#################################
# CONSTANT PATHS IN THE CONTAINER
#################################

DATA_ROOT = '/home/ubuntu/bucket'
LOCAL_OUTPUT = '/home/ubuntu/local_output'
QUEUE_URL = os.environ['SQS_QUEUE_URL']
AWS_BUCKET = os.environ['AWS_BUCKET']
LOG_GROUP_NAME= os.environ['LOG_GROUP_NAME']
CHECK_IF_DONE_BOOL= os.environ['CHECK_IF_DONE_BOOL']
EXPECTED_NUMBER_FILES= os.environ['EXPECTED_NUMBER_FILES']
if 'MIN_FILE_SIZE_BYTES' not in os.environ:
    MIN_FILE_SIZE_BYTES = 1
else:
    MIN_FILE_SIZE_BYTES = int(os.environ['MIN_FILE_SIZE_BYTES'])
if 'USE_PLUGINS' not in os.environ:
    USE_PLUGINS = 'False'
else:
    USE_PLUGINS = os.environ['USE_PLUGINS']
if 'NECESSARY_STRING' not in os.environ:
    NECESSARY_STRING = False
else:
    NECESSARY_STRING = os.environ['NECESSARY_STRING']
if 'DOWNLOAD_FILES' not in os.environ:
    DOWNLOAD_FILES = False
else:
    DOWNLOAD_FILES = os.environ['DOWNLOAD_FILES']

localIn = '/home/ubuntu/local_input'


#################################
# CLASS TO HANDLE THE SQS QUEUE
#################################

class JobQueue():

    def __init__(self, queueURL):
        self.client = boto3.client('sqs')
        self.queueURL = queueURL
    
    def readMessage(self):
        response = self.client.receive_message(QueueUrl=self.queueURL, WaitTimeSeconds=20)
        if 'Messages' in response.keys():
            data = json.loads(response['Messages'][0]['Body'])
            handle = response['Messages'][0]['ReceiptHandle']
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
        if output== '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            logger.info(output)  

def printandlog(text,logger):
    print(text)
    logger.info(text)

def loadConfig(configFile):
    data = None
    with open(configFile, 'r') as conf:
        data = json.load(conf)
    return data

#################################
# IMAGE PREPROCESSING FUNCTIONS
#################################

#mostly taken from the DeepProfiler function, we're just batching differently
def preprocess_image(image_path,illum_file=None, preprocess=True):
    image = skimage.io.imread(image_path)

    if illum_file != None:
        illum = numpy.load(illum_file)
        image = image/illum
    if preprocess:
        vmin, vmax = scipy.stats.scoreatpercentile(image, (0.05, 99.95))
        image = skimage.exposure.rescale_intensity(image, in_range=(vmin, vmax))
        
        image = skimage.img_as_ubyte(image)
        image_path = image_path[:image_path.index('.')]+'.png'

    skimage.io.imsave(image_path, image)

#################################
# RUN SOME PROCESS
#################################

def runSomething(message):
    #List the directories in the bucket- this prevents a strange s3fs error
    rootlist=os.listdir(DATA_ROOT)
    for eachSubDir in rootlist:
        subDirName=os.path.join(DATA_ROOT,eachSubDir)
        if os.path.isdir(subDirName):
            trashvar=os.system('ls '+subDirName)

    # Configure the logs
    logger = logging.getLogger(__name__)

    # Parse your message somehow to pull out a name variable that's going to make sense to you when you want to look at the logs later
    group_to_run = message["group"]
    groupkeys = list(group_to_run.keys())
    groupkeys.sort()
    metadataID = '-'.join(groupkeys)

    # Add a handler with 
    watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=str(metadataID),create_log_group=False)
    logger.addHandler(watchtowerlogger)

    
    remoteOut = os.path.join(message["output_directory"],metadataID)
    localOut = os.path.join(LOCAL_OUTPUT,metadataID)
    if not os.path.exists(localOut):
          os.makedirs(localOut)
    
    # See if this is a message you've already handled, if you've so chosen
    if CHECK_IF_DONE_BOOL.upper() == 'TRUE':
        try:
            s3client=boto3.client('s3')
            bucketlist=s3client.list_objects(Bucket=AWS_BUCKET,Prefix=remoteOut+'/')
            objectsizelist=[k['Size'] for k in bucketlist['Contents']]
            objectsizelist = [i for i in objectsizelist if i >= MIN_FILE_SIZE_BYTES]
            if NECESSARY_STRING:
                if NECESSARY_STRING != '':
                    objectsizelist = [i for i in objectsizelist if NECESSARY_STRING in i]
            if len(objectsizelist)>=int(EXPECTED_NUMBER_FILES):
                printandlog('File not run due to > expected number of files',logger)
                logger.removeHandler(watchtowerlogger)
                return 'SUCCESS'
        except KeyError: #Returned if that folder does not exist
            pass	

    # Let's do this, shall we?
    s3 = boto3.resource('s3')
    # get the config, put it somewhere called configlocation(TODO), and load it
    config = loadConfig(configlocation)
    channels = config["images"]["channels"]
    
    # get the index csv, put it somewhere called csvlocation
    #TODO

    # parse the csv
    df = pandas.read_csv(csvlocation)
    for eachkey in groupkeys:
       df = df[df[eachkey]==group_to_run[eachkey]]
    sitecount = df.shape[0]
    
    # get the location files based on the parsed index file
    #TODO

    # get the image files based on the parsed index file (TODO)
    """
    #This is from DCP, needs to be fixed for DDP
    for channel in channels:
        for field in range(df.shape[0]):
            full_old_file_name = os.path.join(list(df['PathName_'+channel])[field],list(df['FileName_'+channel])[field])
            prefix_on_bucket = full_old_file_name.split(DATA_ROOT)[1][1:]
            new_file_name = os.path.join(localIn,prefix_on_bucket)
            if not os.path.exists(os.path.split(new_file_name)[0]):
                os.makedirs(os.path.split(new_file_name)[0])
                printandlog('made directory '+os.path.split(new_file_name)[0],logger)
            if not os.path.exists(new_file_name):
                s3.meta.client.download_file(AWS_BUCKET,prefix_on_bucket,new_file_name)
                downloaded_files.append(new_file_name)
    """

    do_illum = message["apply_illum"].lower() == 'true'
    
    # if doing illum, get the illum files based on the input message, and make a dictionary for their channel mapping
    if do_illum:
        illum_dict = {}
        #Go to the illum topdir, figure what files are there
        #Map them to our channels
        #Get the files, put locations in the dictionary

    do_preprocess = message["preprocess"].lower() == 'true'

    if do_illum or do_preprocess:
        #see if we can chuck the forloops later, just trying to figure out what we actually need to do
        for eachchannel in channels:
            if do_illum:
                illum_file = illum_dict[eachchannel]
            else:
                illum_file = None
            for eachimage in df[eachchannel]: 
                preprocess_image(eachimage,illum_file=illum_file,preprocess=do_preprocess)
            if do_preprocess:
                sample_file_name = df[eachchannel][0]
                extension = sample_file_name[sample_file_name.index('.'):]
                if extension!= '.png':
                    df[eachchannel].replace(extension,'.png',regex=True,inplace=True)

    df.to_csv(csvlocation,index=False)

    cmd = "python3 deepprofiler --root=ourroot --config=ourconfig profile" #TODO - fix root and config here

    print('Running', cmd)
    logger.info(cmd)
    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    monitorAndLog(subp,logger)

    # Check if we have the expected number of sites, and if so, say done
    # I don't know what happens when a site has 0 cells (or if it's possible for that to happen here)
    # If we figure out a site can have 0 cells, and if so that it does not make an npz, we'll have to do something
    # I suspect in that case the right thing will be to match the glob to the expected site list, then query the locations file for any missing sites
    nsites = len(glob.glob(os.path.join(localOut,'outputs','results','features','**','*.npz'),recursive=True))
    done = nsites == sitecount
    
    # Get the outputs and move them to S3
    if done:
        time.sleep(30)
        mvtries=0
        while mvtries <3:
            try:
                    printandlog('Move attempt #'+str(mvtries+1),logger)
                    cmd = 'aws s3 mv ' + localOut + ' s3://' + AWS_BUCKET + '/' + remoteOut + ' --recursive' 
                    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
                    out,err = subp.communicate()
                    out=out.decode()
                    err=err.decode()
                    printandlog('== OUT \n'+out, logger)
                    if err == '':
                        break
                    else:
                        printandlog('== ERR \n'+err,logger)
                        mvtries+=1
            except:
                printandlog('Move failed',logger)
                printandlog('== ERR \n'+err,logger)
                time.sleep(30)
                mvtries+=1
        if mvtries < 3:
            printandlog('SUCCESS',logger)
            logger.removeHandler(watchtowerlogger)
            return 'SUCCESS'
        else:
            printandlog('SYNC PROBLEM. Giving up on trying to sync '+metadataID,logger)
            import shutil
            shutil.rmtree(localOut, ignore_errors=True)
            logger.removeHandler(watchtowerlogger)
            return 'PROBLEM'
    else:
        printandlog('PROBLEM: Failed exit condition for '+metadataID,logger)
        logger.removeHandler(watchtowerlogger)
        import shutil
        shutil.rmtree(localOut, ignore_errors=True)
        return 'PROBLEM'
    

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
            if result == 'SUCCESS':
                print('Batch completed successfully.')
                queue.deleteMessage(handle)
            else:
                print('Returning message to the queue.')
                queue.returnMessage(handle)
        else:
            print('No messages in the queue')
            break

#################################
# MODULE ENTRY POINT
#################################

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print('Worker started')
    main()
    print('Worker finished')

