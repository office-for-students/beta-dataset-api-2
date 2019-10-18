from flask import Flask, Response, request, jsonify
import logging
import os
import traceback
import json
import datetime

from SharedCode.utils import (
    get_collection_link,
    get_cosmos_client,
    get_http_error_response_json,
)
from SharedCode.dataset_helper import DataSetHelper

from .course_fetcher import CourseFetcher
from .course_param_validator import valid_course_params

from .institution_fetcher import InstitutionFetcher
from .validators import valid_institution_params


# Create an instance of flask
app = Flask(__name__)

cosmosdb_uri = os.environ["AzureCosmosDbUri"]
cosmosdb_key = os.environ["AzureCosmosDbKey"]
cosmosdb_database_id = os.environ["AzureCosmosDbDatabaseId"]
cosmosdb_dataset_collection_id = os.environ["AzureCosmosDbDataSetCollectionId"]
cosmosdb_courses_collection_id = os.environ["AzureCosmosDbCoursesCollectionId"]
cosmosdb_inst_collection_id = os.environ["AzureCosmosDbInstitutionsCollectionId"]

# Intialise cosmos db client
client = get_cosmos_client(cosmosdb_uri, cosmosdb_key)

@app.route("/institutions/<institution>/courses/<course>/modes/<mode>", methods=['GET'])
def course(institution, course, mode):
    """Implements the REST API endpoint for getting course documents.

    The endpoint implemented is:
        /institutions/{institution_id}/courses/{course_id}/modes/{mode}

    The API is fully documented in a swagger document in the same repo
    as this module.
    """

    try:
        logging.info(f"Process a request for an course resource\nurl: {request.url}")

        params = dict({"institution_id": institution, "course_id": course, "mode": mode})
        logging.info(f"Parameters: {params}")

        #
        # The params are used in DB queries, so let's do
        # some basic sanitisation of them.
        #
        if not valid_course_params(params):
            logging.error(f"valid_course_params returned false for {params}")
            return Response(
                get_http_error_response_json(
                    "Bad Request", "Parameter Error", "Invalid parameter passed"
                ),
                headers={"Content-Type": "application/json"},
                status=400,
            )

        logging.info("The parameters look good")
        
        courses_collection_link = get_collection_link(cosmosdb_database_id, cosmosdb_courses_collection_id)
        dataset_collection_link = get_collection_link(cosmosdb_database_id, cosmosdb_dataset_collection_id)

        # Intialise a CourseFetcher
        course_fetcher = CourseFetcher(client, courses_collection_link)

        # Initialise dataset helper - used for retrieving latest dataset version
        dsh = DataSetHelper(client, dataset_collection_link)
        version = dsh.get_highest_successful_version_number()

        # Get the course
        course = course_fetcher.get_course(version=version, **params)

        if course:
            return Response(
                course, headers={"Content-Type": "application/json"},
                status=200
            )
        else:
            return Response(
                get_http_error_response_json(
                    "Not Found", "course", "Course was not found."
                ),
                headers={"Content-Type": "application/json"},
                status=404,
            )

    except Exception as e:
        logging.error(traceback.format_exc())

        # Raise so Azure sends back the HTTP 500
        raise e


@app.route("/institutions/<institution>", methods=['GET'])
def institution(institution):
    """Implements the REST API endpoint for getting an institution document.

    The endpoint implemented is:
        /institutions/{institution_id}/

    The API is documented in a swagger document.
    """

    try:
        logging.info(f"Process a request for an institution resource\nurl: {request.url}")

        params = dict({"institution_id": institution})
        logging.info(f"Parameters: {params}")

        if not valid_institution_params(params):
            logging.error(f"valid_institution_params returned false for {params}")
            return Response(
                get_http_error_response_json(
                    "Bad Request", "Parameter Error", "Invalid parameter passed"
                ),
                headers={"Content-Type": "application/json"},
                status=400,
            )

        inst_collection_link = get_collection_link(cosmosdb_database_id, cosmosdb_inst_collection_id)
        dataset_collection_link = get_collection_link(cosmosdb_database_id, cosmosdb_dataset_collection_id)

        # Initialise dataset helper - used for retrieving latest dataset version
        dsh = DataSetHelper(client, dataset_collection_link)
        version = dsh.get_highest_successful_version_number()

        # Intialise an InstitutionFetcher
        institution_fetcher = InstitutionFetcher(client, inst_collection_link)
        institution = institution_fetcher.get_institution(version=version, **params)

        if institution:
            logging.info(f"Found an institution {institution}")
            
            return Response(
                institution,
                headers={"Content-Type": "application/json"},
                status=200,
            )

        return Response(
            get_http_error_response_json(
                "Not Found", "institution", "Institution was not found."
            ),
            headers={"Content-Type": "application/json"},
            status=404,
        )

    except Exception as e:
        logging.error(traceback.format_exc())

        # Raise so Azure sends back the HTTP 500
        raise e
