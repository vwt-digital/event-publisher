from datetime import datetime, timezone


class Formatter:
    """
    Formatter class to format json records.

    :layout: Contains information for the formatter.
    """

    def __init__(self, template: dict = {}):
        self._template = template

    def _is_float(self, x) -> bool:
        """
        Returns true if parameter is a float.
        """

        try:
            float(x)
        except ValueError:
            return False
        else:
            return True

    def _is_int(self, x) -> bool:
        """
        Returns true if parameter is an integer.
        """

        try:
            a = float(x)
            b = int(a)
        except ValueError:
            return False
        else:
            return a == b

    def _convert(self, message: dict):
        """
        Converts fields of a message.

        :param message: A message with key and values.
        """

        for key, value in self._template.items():
            if key.get('conversion') == 'lowercase':
                message[key] = message[key].lower()
            elif key.get('conversion') == 'uppercase':
                message[key] = message[key].lower()
            elif key.get('conversion') == 'capitalize':
                message[key] = message[key].capitalize()
            elif key.get('conversion') == 'numeric':
                value = message[key]
                if self._is_int(value):
                    message[key] = int(float(value))
                elif self._is_float(value):
                    message[key] = float(value)
            elif key.get('conversion') == 'datetime':
                if isinstance(message[key], int):
                    # the datetime was converted by Pandas to Unix epoch in milliseconds
                    date_object = datetime.fromtimestamp(int(message[key] / 1000), timezone.utc)
                else:
                    date_object = datetime.strptime(message[key], value.get(
                        'format_from', '%Y-%m-%dT%H:%M:%SZ'))
                message[key] = str(datetime.strftime(date_object, value.get(
                    'format_to', '%Y-%m-%dT%H:%M:%SZ')))

            if key.get('prefix_value'):
                message[key] = f"{key['prefix_value']}{message[key]}"

    def format(self, messages: list) -> list:
        """
        Formats a message.

        :param message: A list of json messages.
        """

        formatted = []
        for message in messages:
            msg = {}
            for key, value in self._template.items():
                msg[key] = None
                if isinstance(key, dict):
                    if (key.get('conversion') == 'geojson_point' and
                            message.get(key['longitude_attribute']) and
                            message.get(key['latituted_attribute'])):
                        msg['key'] = {
                            "type": "Point",
                            "coordinates": [float(message[key['longitude_attribute']]),
                                            float(message[key['latitude_attribute']])]
                        }
                    for item in key.get('source_attribute', []):
                        for attribute in key.get('source_attribute', []):
                            if message.get(attribute):
                                msg[key] = message.get(attribute)
                    else:
                        msg[key] = msg.get(key['source_attribute'], None)
                    if msg.get(key):
                        self._convert(msg)
                else:
                    msg[key] = message.get(value)
            formatted.append(msg)
        return formatted
