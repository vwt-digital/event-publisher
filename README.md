# Event Publisher

This repository contains a Google Cloud function which can be readily deployed to the Google Cloud Platform. The purpose of this cloud function is to read a file with records from a storage bucket in different formats. It then checks whether there are new records in the file according to a state saved in Google Cloud Datastore or Google Cloud Storage. Finally it publishes new records according to a predefined schema and saves the those records in the state for the next run.

## Configuration

See [config.yaml.example](cloud_function/config.yaml.example) for an example configuration file. This file contains the following configuration options:

| Variable             | Description                                       | Optional  |
| -------------------- |-------------------------------------------------- | --------- |
| topic.id             | Pub/Sub topic id.                                 | False     |
| topic.project_id     | Project containing the Pub/Sub topic.             | False     |
| topic.subject        | Message subject key.                              | True      |
| topic.batch_size     | Number of events to put in a single message.      | True      |
| topic.batch_settings | Configuration for pubsub_v1.types.BatchSettings.  | True      |
| topic.csv_dialect_parameters | Used when reading csv files ([information](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)).                   | True      |
| topic.full_load      | Full or incremental load.                         | True      |
| topic.top_level_attribute | Top level attribute when reading json files. | True      |
| topic.prefix_filter  | Skip when file matches the prefix filter.         | True      |
| state.type           | Indicates where the state is stored, currently only Datastore is supported              | True       |
| state.kind           | Datastore kind name.                              | True      |
| state.property       | Datastore property name.                          | True      |
| format               | Maps records to a specific format, changing column names, converting values, etc. See `config.yaml.example` for specific cases. | True |

### Configuration format
The format field in the configuration can look as follows:
~~~YAML
format:
  Column1:
    name: first_name
  Column2:
    name: last_name
    conversion:
      type: lowercase
  Column3:
    subfields:
      Subfield_Column1:
        name: geometry
        conversion:
          type: geojson_point
~~~
Where the ```Column``` fields are the fields coming from the message and the ```name``` field is the field you want to have the value of the ```Column``` field published under.  

The field ```conversion``` can be of type:
- geojson_point
- lowercase
- datetime, which also needs a ```format``` field

### Example format
When you get for example the following message:
~~~JSON
{
  "action": "add",
  "employee": {
    "first_and_last_name": "John Doe",
    "age": 45
  }
}
~~~

The format in the config file could look as follows:
~~~YAML
  action:
    name: to_do
  employee:
    subfields:
      first_and_last_name:
        name: full_name
        conversion:
          type: lowercase
~~~

Which will result in the following message being published:
~~~JSON
{
  "to_do": "add",
  "full_name": "john doe"
}
~~~

## Deployment

```
gcloud functions deploy event-publisher \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-bucket=my-bucket \
  --project=my-project \
  --region=europe-west1 \
  --memory=512MB \
  --timeout=120s
```

## Testing

Create a `config.yaml` from the example file and install `requirements.txt`. Place a file in a bucket and call execute the following command, where `[BUCKET_NAME]` is the name of the bucket and `[FILE_NAME]` is the full name of the file.

```
python3 -c "from main import handler; handler({'bucket': '[BUCKET_NAME]', 'name': '[FILE_NAME]'}, '')"
```
