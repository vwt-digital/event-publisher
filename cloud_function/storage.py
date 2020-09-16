import json
import brotli
import pandas as pd

from io import BytesIO
from google.cloud import storage
from defusedxml import ElementTree as ET

from formatter import Formatter


class File:
    """
    Class that represents a file with records, which can be converted
    to json from other formats, such as xml, csv and xlsx.

    :param name:             The name of the file.
    :param content:          The content of the file in string format.
    :csv_dialect_parameters: Parameters for reading csv files.
    :top_level_attribute:    Top level json attribute holding the records.
    """

    def __init__(self, name: str, content: str):
        self.name = name
        self.content = content
        self.csv_dialect_parameters = {}
        self.top_level_attribute = None

    @property
    def type(self):
        """Type of the file, based on the extension"""
        return self.name.split('.')[-1]

    def _is_xml(self):
        """Indicates whether it is an xml file"""
        if self.type == 'atom':
            return True

    def _is_xlsx(self):
        """Indicates whether it is an xlsx file"""
        if self.type == 'xlsx':
            return True

    def _is_json(self):
        """Indicates whether it is a json file"""
        if self.type == 'json':
            return True

    def _is_csv(self):
        """Indicates whether it is a csv file"""
        if self.type == 'csv':
            return True

    def to_json(self, formatter: Formatter):
        """
        Transforms multiple file formats to json.

        :formatter: Formatter to format a list of json records
                    given a formatting template.
        """

        if self._is_xlsx():
            df = pd.read_excel(BytesIO(self.content), dtype=str)
            df[df.isnull()] = None
            data = df.to_dict(orient='records')
        elif self._is_csv():
            df = pd.read_csv(BytesIO(self.content), **self.csv_dialect_parameters)
            data = df.to_dict(orient='records')
        elif self._is_xml():
            data = self._xml_to_json(self.content)
        elif self._is_json():
            data = json.loads(self.content)
            data = data.get(self.top_level_attribute, data)
        else:
            raise NotImplementedError("Unknown file type!")

        data = formatter.format(data)

        return data

    def _xml_to_json(self, xml: str):
        """
        Transforms xml to json.

        :xml: XML content in string format.
        """

        tree = ET.fromstring(xml)
        namespace = tree.tag[:-4]
        contents = [entry.find(f'{namespace}content') for
                    entry in tree.findall(f'{namespace}entry')]

        result = []
        for content in contents:
            items = {}
            for properties in content:
                if properties.tag.endswith('}properties'):
                    for prop in properties:
                        if prop.text:
                            items[prop.tag.split('}')[-1]] = prop.text
                    result.append(items)

        return result


class GoogleCloudStorage:
    """
    Class that interacts with Google Cloud Storage.
    """

    def __init__(self):
        self._client = storage.Client()

    def read(self, file_name: str, bucket_name: str):
        """
        Reads a file from Google Cloud Storage.

        :file_name:   The name of the file object.
        :bucket_name: The source bucket of the file object.
        """

        bucket = self._client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_name)

        data = blob.download_as_string(raw_download=True)
        data = self._decompress(data, blob.content_encoding)

        file = File(file_name, data)

        return file

    def _decompress(self, data: str, content_encoding: str):
        """
        Decompresses data with Brotli algorithm.

        :data:             Data in string format.
        :content-encoding: The encoding of the file.
        """

        if content_encoding == 'br':
            return brotli.decompress(data)

        return data
