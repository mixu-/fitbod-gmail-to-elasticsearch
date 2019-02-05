# Fitbod to Elasticsearch Indexer

Get the CSV file that Fitbod can export via GMail API and index the data in elasticsearch.

## Preconditions

* Export some data from Fitbod and have it sent to a Gmail account.
* Python 3.6
* A server running Elasticsearch. Modify the code if it's not running in localhost:9201
* Packages installed:
    pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib elasticsearch
* Turn on the GMail API:
    https://developers.google.com/gmail/api/quickstart/python
* Download the credentials.json from the above link.
* Currently the initial authentication process requires you to login via a web browser, so the script will not work on a headless server.
