import json
import logging


class InstitutionFetcher:
    """Retrieves institutions from Cosmos DB"""

    def __init__(self, client, collection_link):
        self.client = client
        self.collection_link = collection_link

    def get_institution(self, version, institution_id):
        """Retrieves an institution document from Cosmos DB.

        Queries the Cosmos DB container for an institution using the
        arguments passed in. If an institution is found, it removes
        the additional internal fields and fields that Cosmos DB
        added and returns it. If no institution is found it returns None.

        """

        logging.info(f"client {self.collection_link}")

        # Create an SQL query to retrieve the institution document
        query = (
            "SELECT * from c "
            f"where c.institution_id = '{institution_id}' "
            f"and c.version = {version} "
        )

        logging.info(f"query: {query}")

        options = {"enableCrossPartitionQuery": True}

        # Query the institution container using the sql query and options
        institutions_list = list(
            self.client.QueryItems(self.collection_link, query, options)
        )

        # If no institution matched the arguments passed in return None
        if not institutions_list:
            return None

        # Log an error if more than one institution is returned by query.
        if len(institutions_list) > 1:
            # Something's wrong; there should be only one matching institution.
            institutions_count = len(institutions_list)
            logging.error(
                f"{institutions_count} institutions returned."
                " There should only be one returned."
            )

        # Get the institution from the list.
        institution = institutions_list[0]

        # Remove unnecessary keys from the institution.
        # tidied_institution = InstitutionFetcher.tidy_institution(institution)

        # Convert the course to JSON and return
        return json.dumps(institution, indent=4, sort_keys=True)

    @staticmethod
    def tidy_institution(institution):
        """Removes our internal use only fields and the ones Cosmos DB adds"""

        # Our internal use only field is institution_id, the rest are
        # added by CosmosDB.
        keys_to_delete = [
            "institution_id",
            "id",
            "_rid",
            "_self",
            "_etag",
            "_attachments",
            "_ts",
        ]
        for key in keys_to_delete:
            try:
                del institution[key]
            except KeyError:
                logging.warning(f"The expected key was not found: {key}")
        return institution
