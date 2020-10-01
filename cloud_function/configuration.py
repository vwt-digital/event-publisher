import yaml


class Configuration:
    """
    Class that holds configuration variables.
    """

    def __init__(self):
        self._configuration = self._read()

    def _read(self) -> dict:
        """
        Reads configuration from a config.py file.
        """

        with open('config.yaml') as f:
            configuration = yaml.safe_load(f)

        return configuration

    @property
    def prefix_filter(self):
        """Prefix to filter files on."""
        return self._configuration.get('prefix_filter', '')

    @property
    def top_level_attribute(self):
        """Top level attribute in json that contains list with records."""
        return self._configuration.get('top_level_attribute')

    @property
    def csv_dialect_parameters(self):
        """Pandas csv dialect options."""
        return self._configuration.get('csv_dialect_parameters', {})

    @property
    def template(self):
        """Formatting template."""
        return self._configuration.get('format', {})

    @property
    def full_load(self):
        """Load all messages instead of increments."""
        return self._configuration.get('full_load', False)

    @property
    def state(self):
        """Configuration about where to store state."""
        content = self._configuration.get('state', {})
        return StateConfiguration(content)

    @property
    def topic(self):
        """Topic configuration"""
        content = self._configuration.get('topic', {})
        return TopicConfiguration(content)


class TopicConfiguration:
    """
    Class that holds pub/sub topic configuration.

    :state: Dictionary topic configuration.
    """

    def __init__(self, configuration: dict):
        self._project_id = configuration.get("project_id")
        self._id = configuration.get("id")
        self._subject = configuration.get("subject")
        self._batch_size = configuration.get("batch_size")
        self._batch_settings = configuration.get("batch_settings", {})

    @property
    def project_id(self):
        """GCP project id."""
        return self._project_id

    @project_id.setter
    def project_id(self, value):
        """Project_id setter."""
        self._project_id = value

    @property
    def id(self):
        """GCP topic id."""
        return self._id

    @id.setter
    def id(self, value):
        """Id setter."""
        self._id = value

    @property
    def subject(self):
        """Message subject name."""
        return self._subject

    @subject.setter
    def subject(self, value):
        """Subject setter."""
        self._subject = value

    @property
    def batch_size(self):
        """Number of messages to bundle."""
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value):
        """Batch_size setter."""
        self._batch_size = value

    @property
    def batch_settings(self):
        """Input for pubsub_v1.types.BatchSettings"""
        return self._batch_settings

    @batch_settings.setter
    def batch_settings(self, value):
        """Batch_settings setter."""
        self._batch_settings = value


class StateConfiguration:
    """
    Class that holds state configuration.

    :state: Dictionary with (datastore) state information.
    """

    def __init__(self, state: dict):
        self._type = state.get("type")
        self._kind = state.get("kind")
        self._property = state.get("property")

    @property
    def type(self):
        """"Type to store the state."""
        return self._type

    @type.setter
    def type(self, value):
        """"Type setter."""
        self._type = value

    @property
    def kind(self):
        """"Datastore kind name."""
        return self._kind

    @kind.setter
    def kind(self, value):
        """Kind setter"""
        self._kind = value

    @property
    def property(self):
        """"Datastore property name."""
        return self._property

    @property.setter
    def property(self, value):
        """Property setter."""
        self._property = value
