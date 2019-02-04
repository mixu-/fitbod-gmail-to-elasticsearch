"""Get the CSV file that Fitbod can export via GMail API and index the data in elasticsearch.

See: https://www.fitbod.me/

"""

import os
import csv
import logging
import datetime
from dateutil import parser
from elasticsearch import Elasticsearch
import gmail_api

logging.basicConfig(level=logging.INFO)

def GetAttachmentsWithQuery(query, tgt_dir):
    """Returns a list of attachments that match the query. Saves files to tgt_dir.
    
    Will overwrite files if they already exist.
    """
    
    service = gmail_api.GetService('credentials.json')
    # Call the Gmail API
    filtered_msgs = gmail_api.ListMessagesMatchingQuery(service, 'me', query)
    logging.debug(filtered_msgs)
    
    attachments = []
    for msg in filtered_msgs:
        attachments.extend(gmail_api.GetAttachments(service, 'me', msg["id"], tgt_dir))
    logging.info("%s attachment(s) found and saved to disk." %(len(filtered_msgs), ))
    return attachments


def index_to_es(es, index, doc_type, body, doc_id):
    """Indexes the data in 'body' object to Elasticsearch.
    
    Args:
        es: elasticsearch instance
        index: Index name on the ES server
        doc_type: ES doc type
        body: The data to be indexed
        doc_id: The ID for ES to use
    """

    ret = es.index(index=index, doc_type=doc_type, body=body, id=doc_id)
    if ret["result"] != "created" and ret["result"] != "updated":
        logging.error("Indexing returned %s, expected 'created' or 'updated'" %(ret["result"]))

def main():
    ES_INDEX = "fitbod-2"
    ES_DOC_TYPE = "workout_sets"
    TMP_DIR = os.path.join(os.sep, "tmp", "fitbod2elastic")

    attachments = GetAttachmentsWithQuery('fitbod', TMP_DIR)

    es = Elasticsearch([{'host': '127.0.0.1', 'port': 9201}])
    
    for att in attachments:
        logging.info("Indexing %s" %(att, ))
        if not att.endswith(".csv"):
            logging.warning("...skipping due to unknown file type. Expecting CSV.")
            continue
    
        # We can't distinguish similar sets performed on the same day.
        # So we add row numbers to the CSV, the oldest set being number 1.
        # The assumption is that old workout data will not be modified.
        # If the history changes, re-index everything with nr_of_days=0
        workout_data = csv_to_workout_obj(att, nr_of_days=7)
        for workout_set in workout_data:
            index_to_es(es, ES_INDEX, ES_DOC_TYPE, workout_set, workout_set["id"])

    #Cleanup
    for att in attachments:
        os.unlink(att)


def csv_to_workout_obj(csv_file, nr_of_days=0):
    """A CSV is converted to a list of dictionairies."""

    #First, we add an ID field.
    with open(csv_file, "r") as f:
        lines = f.readlines()
        line_nr = 0
        lines[0] = '"id","timestamp","exercise","sets","reps","weight","is_warmup","note"\n'
        for line_nr in range(1, len(lines)):
            lines[line_nr] = '"' + str(line_nr) + '",' + lines[line_nr]

    with open(csv_file, "w") as f:
        f.writelines(lines)

    #Second, we read the CSV to a dictionary.
    obj = []
    with open(csv_file, "r") as f:
        rd = csv.DictReader(f, delimiter=',')
        for row in rd:
            #Convert the date to a proper timestamp
            row["timestamp"] = parser.parse(row["timestamp"])
            if row["timestamp"] > datetime.datetime.now()-datetime.timedelta(days=nr_of_days):
                obj.append(row)
    logging.info("Found %s sets within the past %s days" %(len(obj), nr_of_days, ))
    return obj

main()

