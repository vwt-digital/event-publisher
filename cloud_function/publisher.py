import json
import logging
import time

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import types


class Publisher:
    """
    Publisher that publishes messages to Google Cloud Pub/Sub.

    :param batch_settings: pubsub_v1.types.BatchSettings to
                           initialize pubsub PublisherClient.
    """

    def __init__(self, batch_settings: dict = {}):
        self._client = pubsub_v1.PublisherClient(
            batch_settings=types.BatchSettings(**batch_settings),
        )

    def publish(self, project_id: str, topic_id: str, messages: list,
                gobits: dict, batch_size: int, subject: str = "data"):
        """
        Publishes messages to pub/sub.

        :param project_id:  Google Cloud Platform project id.
        :param topic_id:    Google Cloud Platform topic id.
        :param subject:     Subject of the message data, defaults to "data".
        :param messages:    A list of messages to be send.
        :param gobits:      Gobits dictionary that has metadata about the messages.
        :param batch_size:  Indicates whether messages should be send as a list or stand alone.
        """

        topic_path = self._client.topic_path(project_id, topic_id)
        logging.info(f"Publishing {len(messages)} new records to {topic_path}")

        batches = list(self._chunks(messages, batch_size))
        logging.info(f"Sending messages in {len(batches)} batches with batch_size {batch_size}")

        futures = dict()

        def get_callback(f, data):
            def callback(f):
                try:
                    futures.pop(data)
                except:  # noqa
                    print("Please handle {} for {}.".format(f.exception(), data))
                    raise

            return callback

        for idx, batch in enumerate(batches):

            msg = {
                "gobits": [gobits],
                subject: batch
            }

            futures.update({idx: None})

            future = self._client.publish(topic_path, json.dumps(msg).encode('utf-8'))

            futures[idx] = future
            future.add_done_callback(get_callback(future, idx))

        while futures:
            time.sleep(0.1)

    def _chunks(self, lst: list, n: int):
        """
        Yield successive n-sized chunks from lst.

        :param list: The list to chunk.
        :param n:    The number of items per list.
        """

        for i in range(0, len(lst), n):
            yield lst[i:i + n]
