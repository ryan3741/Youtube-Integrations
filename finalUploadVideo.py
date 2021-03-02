#!/usr/bin/python
import os
import random
import sys
import time
import httplib2
import http
import pandas as pd
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow


# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
httplib2.RETRIES = 1

# Maximum number of times to retry before giving up.
MAX_RETRIES = 10

#Different exceptions that will trigger retry of upload
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, http.client.NotConnected,
  http.client.IncompleteRead, http.client.ImproperConnectionState,
 http.client.CannotSendRequest, http.client.CannotSendHeader,
  http.client.ResponseNotReady, http.client.BadStatusLine)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the Google API Console at
# https://console.developers.google.com/.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#   https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets2.json file format, see:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets

#Have your client_secrets file in the same folder as this python file
CLIENT_SECRETS_FILE = "client_secrets3.json"

# This OAuth 2.0 access scope allows an application to upload files to the
# authenticated user's YouTube channel, but doesn't allow other types of access.
YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# This variable defines a message to display if the CLIENT_SECRETS_FILE is
# missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0
To make this sample run you will need to populate the client_secrets2.json file
found at:
   %s
with information from the API Console
https://console.developers.google.com/
For more information about the client_secrets2.json file format, please visit:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
""" % os.path.abspath(os.path.join(os.path.dirname(__file__),
                                   CLIENT_SECRETS_FILE))
#possible privacy statuses
VALID_PRIVACY_STATUSES = ("public", "private", "unlisted")

#Triggers the workflow for getting authentication from YouTube and sets the correct scopes
#Parameters: None
def get_authenticated_service():
  flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE,
    scope=YOUTUBE_UPLOAD_SCOPE,
    message=MISSING_CLIENT_SECRETS_MESSAGE)

  storage = Storage("%s-oauth2.json" % sys.argv[0])
  credentials = storage.get()

  if credentials is None or credentials.invalid:
    credentials = run_flow(flow, storage)

  return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    http=credentials.authorize(httplib2.Http()))

#Uploads the video to YouTube using the meta-data specified in the csv file
#Parameters: youtube authenticated credentials
def initialize_upload(youtube):
    #csv file with the meta-data, example provided
    currFile = pd.read_csv("ExampleData.csv")
    indexList = []
    for ind in currFile.index:
      indexList.append(ind)

    #loop through all of the rows in the csv
    for r in range(0,len(indexList)-1):

      currTagString = currFile['Tags'][indexList[r]]
      tagList = currTagString.split(", ")
      currTitle = currFile['Title'][indexList[r]]

      currDescription = currFile['Description'][indexList[r]]

      currDescription = currDescription + "\n\n****************\nABOUT THE CONCUSSION STORY WALL\n\nThis video is a part of the Concussion Story Wall, a massive interactive database featuring nearly 4000 stories of those impacted by concussions addressing the following questions:\n\n1) What was the cause of your concussion?\n2) What were your symptoms? (Physical/mental struggles?)\n3) What was the greatest challenge about your concussion?\n4) What tips helped you get through your recovery process?\n5) In retrospect, what is something helpful you could have said to yourself during recovery?\n6) Were you comfortable sharing experiences about your concussion? Were your family, friends, team, coach, school helpful in this process?\n\nAn additional highlight of the Concussion Story Wall is a panel of leading medical experts addressing specific aspects of concussions related to diagnosis, treatment and recovery.\n\nThe Concussion Story Wall is a part of the CrashCourse by TeachAids product suite. Learn more at: https://ConcussionStoryWall.org\n\n\nDISCLAIMER:\n\nThe opinions expressed in Story Wall interviews are not necessarily those of TeachAids or its affiliates. These are personal stories and are not intended to serve as medical advice. For such, contact your physician or other qualified health provider. This video contains difficult topics including mental health that may be triggering. TeachAids can make no guarantees as to the accuracy, currency, or completeness of any content or information contained herein nor that any such information will not be superseded by subsequent developments.\n\nThis content is provided only for noncommercial, informational use. It is not medical advice and is not a substitute for clinical diagnosis, advice from your physician, or the practice or provision of medical care. Do not rely on this content to assess your health—instead, consult with your physician or another qualified healthcare professional in all matters relating to your health. TeachAids and/or its interviewees or licensees are not responsible or liable for any decisions you may make in reliance on this content.\n\n\nABOUT TEACHAIDS:\n\nCrashCourse Concussion Education is the second free health education program launched by the nonprofit TeachAids in collaboration with Stanford University. It follows the global success of the award-winning HIV/AIDS interactive software, which is used in 82 countries around the world.\n\nThe CrashCourse suite consists of four products: CrashCourse Football (12min), CrashCourse Brain Fly-Through (8min), CrashCourse Concussion Story Wall, and CrashCourse Multi-Sport Concussion Education (coming 2021). Several of these products have been developed and disseminated in collaboration with the US Olympic and Paralympic Committee’s National Sports Governing Bodies.\n\nFor more information about the use of TeachAids’ website, services, materials or content, please see: https://teachaids.org/terms-use.\n\nCrashCourse Concussion Education: https://crashcourse.teachaids.org/\nTeachAids: https://teachaids.org/\nCrashCourse Football: https://bit.ly/3ocGBv6"

      currClip = currFile['ID'][indexList[r]]
      currID = currClip[0: len(currClip) - 2]

      #update to include the path to the folder with all of your videos
      initialPath = ""
      currPath = initialPath + currID + "_d/" + currClip + "_d.mp4"

      if not os.path.isfile(currPath):
        print(currClip)
        print("No file found for " + currClip)
        continue
      else:
        #create a YouTube video request including title, description,tags, category, and privacy status
        body=dict(
          snippet=dict(
          title= currTitle,
          description= currDescription,
          tags=tagList,
          categoryId="22"
        ),
        status=dict(
          privacyStatus= "private"
        )
      )

      # Call the API's videos.insert method to create and upload the video.
      insert_request = youtube.videos().insert(
        part=",".join(body.keys()),
        body=body,
        # The chunksize parameter specifies the size of each chunk of data, in
        # bytes, that will be uploaded at a time. Set a higher value for
        # reliable connections as fewer chunks lead to faster uploads. Set a lower
        # value for better recovery on less reliable connections.
        #
        # Setting "chunksize" equal to -1 in the code below means that the entire
        # file will be uploaded in a single HTTP request. (If the upload fails,
        # it will still be retried where it left off.) This is usually a best
        # practice, but if you're using Python older than 2.6 or if you're
        # running on App Engine, you should set the chunksize to something like
        # 1024 * 1024 (1 megabyte).

        #update this to be the correct file

        media_body=MediaFileUpload(currPath, chunksize=-1, resumable=True)

      )
      print(currClip)
      resumable_upload(insert_request)

# This method implements an exponential backoff strategy to resume a failed upload.
#Parameters: It takes in a youtube.videos insert request 
def resumable_upload(insert_request):
  response = None
  error = None
  retry = 0
  while response is None:
    try:
      status, response = insert_request.next_chunk()
      if response is not None:
        if 'id' in response:
          print(response['id'])
        else:
          exit("The upload failed with an unexpected response: %s" % response)

    #Handle errors thrown by the process
    except Exception as e:
      if e.resp.status in RETRIABLE_STATUS_CODES:
        error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
      else:
        raise

    if error is not None:
      print(error)
      retry += 1
      if retry > MAX_RETRIES:
        exit("No longer attempting to retry.")

      max_sleep = 2 ** retry
      sleep_seconds = random.random() * max_sleep
      print("Sleeping %f seconds and then retrying..." % sleep_seconds)
      time.sleep(sleep_seconds)

if __name__ == '__main__':

  #get authentication
  youtube = get_authenticated_service()

  #run the uploads
  try:
    initialize_upload(youtube)
  except Exception as e:
    print(str(e))
    print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))

  print("done")

