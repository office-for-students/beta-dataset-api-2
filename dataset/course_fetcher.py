import json
import logging

import azure.cosmos.cosmos_client as cosmos_client


class CourseFetcher:
    """Handles retrieving courses from Cosmos DB"""

    def __init__(self, client, collection_link):
        self.client = client
        self.collection_link = collection_link

    def get_course(self, version, institution_id, course_id, mode):
        """Retrieves a course document from Cosmos DB.

        Queries the Cosmos DB container for a course using the
        arguments passed in. If a course is found, it removes
        the additonal fields Cosmos DB added before returning it
        to the caller. If no course is found it returns None.

        """

        # Create an SQL query to retrieve the course document
        query = (
            "SELECT * from c "
            f"where c.institution_id = '{institution_id}' "
            f"and c.course_id = '{course_id}' "
            f"and c.course_mode = {mode} "
            f"and c.version = {version} "
        )

        logging.info(f"query: {query}")

        options = {"enableCrossPartitionQuery": True}

        # Query the course container using the sql query and options
        courses_list = list(
            self.client.QueryItems(self.collection_link, query, options)
        )

        # If no course matched the arguments passed in return None
        if not len(courses_list):
            return None

        # Log an error if more than one course is returned by query.
        if len(courses_list) > 1:
            # Something's wrong; there should be only one matching course.
            course_count = len(courses_list)
            logging.error(f"{course_count} courses returned. There should be only one.")

        # Get the course from the list.
        course = courses_list[0]
        logging.info(
            f"Fetched course: institution_id: {course['institution_id']}"
            f" course_id: {course['course_id']}"
        )

        # Remove unnecessary keys from the course.
        tidied_course = CourseFetcher.tidy_course(course)

        # Convert the course to JSON and return
        return json.dumps(tidied_course)

    @staticmethod
    def tidy_course(course):
        """Removes our internal items and those that Cosmos DB adds"""

        keys_to_delete = [
            "_rid",
            "_self",
            "_etag",
            "_attachments",
            "_ts",
            "institution_id",
            "course_id",
            "course_mode",
        ]
        for key in keys_to_delete:
            try:
                del course[key]
            except KeyError:
                logging.warning(f"The expected Comsos DB key was not found: {key}")
        return course
