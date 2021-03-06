import pickle
import os.path
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/drive']


def main():
    """List & download files from Google Drive.
    """
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    drive_service = build('drive', 'v3', credentials=creds)

    page_token = None
    dtime = download_time("get")
    query = f"name contains 'machine' and name contains '.csv' and modifiedTime > '{dtime}'"
    while True:
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token).execute()
        for file in response.get('files', []):
            file_id = file.get('id')
            file_name = file.get('name')
            download(drive_service, file_id, file_name)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    download_time("set")


def download(service, file_id, file_name):
    """Download files from Google Drive.

    Args:
        service: Google Drive serive
        file_id: id of file to be downloaded
        file_name: file to be downloaded
    """
    request = service.files().get_media(fileId=file_id)
    with open(os.path.join("staging", file_name), 'wb') as file_io:
        downloader = MediaIoBaseDownload(file_io, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()


def download_time(action):
    """Set or get the last download time from Google Drive

    Args:
        action (string): can have two values - 'set' or 'get'
        for setting or getting last download time

    Returns:
        dtime (only when 'action'='get'): last download time
    """
    filename = ".last_download_date"
    if action == "set":
        download_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        with open(filename, "w") as fhandle:
            fhandle.write(f"{download_time}")
    elif action == "get":
        if os.path.exists(filename):
            with open(filename) as fhandle:
                dtime = fhandle.read()
        else:
            dtime = "1970-01-01T00:00:00"
        return dtime


if __name__ == '__main__':
    main()
