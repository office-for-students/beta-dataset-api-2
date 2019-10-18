"""Functions shared by Azure Functions"""

import json
import os

import azure.cosmos.cosmos_client as cosmos_client


def get_collection_link(db_id, collection_id):
    """Create and return collection link based on values passed in"""

    # Return a link to the relevant CosmosDB Container/Document Collection
    return "dbs/" + db_id + "/colls/" + collection_id


def get_cosmos_client(cosmosdb_uri, cosmosdb_key):

    master_key = "masterKey"

    return cosmos_client.CosmosClient(
        url_connection=cosmosdb_uri, auth={master_key: cosmosdb_key}
    )

def get_http_error_response_json(error_title, error_key, error_value):
    """Returns a JSON object indicating an Http Error"""
    http_error_resp = {}
    http_error_resp["errors"] = []
    http_error_resp["errors"].append({
        'error': error_title,
        'error_values': [{
            error_key: error_value
        }]
    })
    return json.dumps(http_error_resp)
