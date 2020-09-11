from google.cloud import datastore


class GoogleCloudDatastore:
    """
    Class to interact with Google Cloud Datastore.
    """

    chunk_size = 300

    def __init__(self):
        self._client = datastore.Client()

    def put_multi(self, data: list, kind: str, property: str):
        """
        Put multiple entities in datastore.

        :param kind:     Datastore kind name.
        :param property: Datastore property name.
        """

        for chunk in self._chunks(data, GoogleCloudDatastore.chunk_size):
            entities = []
            for item in chunk:
                entity = datastore.Entity(
                    key=self._client.key(kind, item[property]))
                entity.update(item)
                entities.append(entity)
            self._client.put_multi(entities)

    def _chunks(self, lst: list, n: int):
        """
        Yield successive n-sized chunks from lst.

        :param list: The list to chunk.
        :param n:    The number of items per list.
        """

        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def difference(self, data: list, kind: str, property: str):
        """
        Returns a list of objects that are not in datastore,
        given an entity and property.

        :param kind:     Datastore kind name.
        :param property: Datastore property name.
        """

        result = []
        for chunk in self._chunks(data, GoogleCloudDatastore.chunk_size):
            records = {item[property]: item for item in chunk}
            keys = [self._client.key(kind, key) for key in records.keys()]
            missing_items = []
            state = self._client.get_multi(keys, missing=missing_items)
            for record in state:
                new = records[record.key.id_or_name]
                for key, value in new.items():
                    if key not in record:
                        result.append(new)
                        break
                    elif value != record.get(key):
                        result.append(new)
                        break
            result.extend([records[missing.key.id_or_name] for missing in missing_items])
        return result
