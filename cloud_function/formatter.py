from datetime import datetime
from dateutil.parser import parse
from hashlib import sha256


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

        :param x: Integer or float to validate.
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

        :param x: Integer or float to validate.
        """

        try:
            a = float(x)
            b = int(a)
        except ValueError:
            return False
        else:
            return a == b

    def _to_numeric(self, value):
        """
        Converts a numeric variable to an int or float.

        :param value: Value to convert to int or float.
        """

        if self._is_int(value):
            return int(float(value))
        elif self._is_float(value):
            return float(value)

    def _to_timestamp(self, value, format) -> str:
        """
        Converts a timestamp to uniform string format.

        :param value: Value to convert to string timestamp.
        """

        if not format:
            format = '%Y-%m-%dT%H:%M:%SZ'

        if self._is_int(value) or self._is_float(value):
            if len(str(int(value))) == 13:
                value = value / 1000
            date_object = datetime.fromtimestamp(value)
        else:
            date_object = parse(value)

        date_string = str(datetime.strftime(date_object, format))

        return date_string

    def _geojson(self, message: dict) -> dict:
        """
        Converts a longitude and latitude to geojson.

        :param message: Message to extract geojson from.
        """

        longitude = None
        latitude = None

        for key, value in message.items():
            mapping = self._template.get(key, {})

            for map in get_mapping_list(mapping):
                conversion = map.get('conversion', {})
                if conversion.get('type') == 'geojson':
                    if conversion.get('format') == 'longitude':
                        longitude = value
                    elif conversion.get('format') == 'latitude':
                        latitude = value

        geojson = {
            "type": "Point",
            "coordinates": [float(longitude),
                            float(latitude)]
        }

        return geojson

    def _convert(self, value, type: str, format: str = None):
        """
        Converts fields of a message.

        :param  value: The value to format.
        :param   type: Formatting type to lookup.
        :param format: Optional formatting format.

        """

        result = {
          'lowercase': lambda x: x.lower(),
          'uppercase': lambda x: x.upper(),
          'capitalize': lambda x: x.capitalize(),
          'numeric': lambda x: self._to_numeric(x),
          'datetime': lambda x: self._to_timestamp(x, format),
          'prefix_value': lambda x: f"{format}{x}",
          'hash': lambda x: sha256(x.encode('utf-8')).hexdigest(),
          'no_conversion': lambda x: x
        }[type](value)

        return result

    def format(self, messages: list) -> list:
        """
        Formats a message.

        :param message: A list of json messages.
        """

        formatted = []
        for message in messages:
            msg = {}
            for key, value in message.items():
                mapping = self._template.get(key)
                if mapping:
                    for map in get_mapping_list(mapping):
                        conversion = map.get('conversion', {})
                        if conversion.get('type') == 'geojson':
                            msg[map['name']] = self._geojson(message)
                        else:
                            msg[map['name']] = self._convert(
                                value,
                                conversion.get('type', 'no_conversion'),
                                conversion.get('format'))
            formatted.append(msg)
        return formatted


def get_mapping_list(mapping):
    """
    Returns the mappings in list format

    :param mapping: A mapping.
    :type: dict | list

    :return: A list of mappings.
    :rtype: list
    """

    if isinstance(mapping, list):
        return [item for item in mapping]
    elif isinstance(mapping, dict):
        return [mapping]
    else:
        return None
