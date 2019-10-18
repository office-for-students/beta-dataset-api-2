import os
import logging

import azure.cosmos.cosmos_client as cosmos_client


class DataSetHelper:
    def __init__(self, client, collection_link):
        self.client = client
        self.collection_link = collection_link

    def get_highest_successful_version_number(self):
        query = "SELECT VALUE MAX(c.version) from c WHERE c.status = 'succeeded'"
        options = {"enableCrossPartitionQuery": True}
        max_version_number_list = list(
            self.client.QueryItems(self.collection_link, query, options)
        )
        version = max_version_number_list[0]
        logging.info(f"Highest successful dataset version: {version}")
        return version
