# Event Publisher

This repository contains a Google Cloud function which can be readily deployed to the Google Cloud Platform. The purpose of this cloud function is to read a file with records from a storage bucket in different formats. It then checks whether there are new records in the file according to a state saved in Google Cloud Datastore or Google Cloud Storage. Finally it publishes new records according to a predefined schema and saves the those records in the state for the next run.

## Configuration

See `config.yaml.example` for an example configuration file. This file contains the following configuration options:

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
| state.type           | Indicates where the state is stored, currently only Datastore is supported              | False      |
| state.kind           | Datastore kind name.                              | False     |
| state.property       | Datastore property name.                          | False     |
| format               | Maps records to a specific format, changing column names, converting values, etc. See `config.yaml.example` for specific cases. | True |

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
