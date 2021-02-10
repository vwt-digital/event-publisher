import logging

from gobits import Gobits

from publisher import Publisher
from formatter import Formatter
from configuration import Configuration
from datastore import GoogleCloudDatastore
from storage import GoogleCloudStorage

config = Configuration()


def handler(data, context):
    """
    Handler method that calculates the difference of a dataset
    and sends messages to Google Cloud Pub/Sub.

    :param: data    Dictionary like object that holds trigger information.
    :param: context Google Cloud Function context.
    """

    try:
        bucket_name = data["bucket"]
        file_name = data["name"]

        # Exit when file does not need to be processed
        if not file_name.startswith(config.prefix_filter):
            logging.info("Do not process file, exiting...")
            return 'OK', 204

        file = GoogleCloudStorage().read(file_name, bucket_name)
        file.top_level_attribute = config.top_level_attribute
        file.csv_dialect_parameters = config.csv_dialect_parameters

        records = file.to_json(Formatter(config.template))

        if not config.full_load:
            if config.state.type == "datastore":
                records = GoogleCloudDatastore().difference(
                    records,
                    config.state.kind,
                    config.state.property)
            else:
                raise NotImplementedError("Unkown state type!")

        # Exit when no new records exist
        if not len(records):
            logging.info("No new records found, exiting...")
            return 'OK', 204

        metadata = Gobits.from_context(context=context)
        publisher = Publisher(config.topic.batch_settings)
        publisher.publish(
            config.topic.project_id,
            config.topic.id,
            records,
            metadata.to_json(),
            config.topic.batch_size,
            config.topic.subject)

        # Store the new state records
        if not config.full_load:
            if config.state.type == "datastore":
                logging.info("Adding new items to state")
                GoogleCloudDatastore().put_multi(
                    records,
                    config.state.kind,
                    config.state.property)

    except Exception as e:
        logging.exception(e)
        return 'Bad Request', 400

    return 'OK', 204
