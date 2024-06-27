# Table of Contents
1. [Intro to LakeView](#Intro-to-LakeView)
1. [Feature Highlights](#Feature-Highlights)
1. [Setup Guide](#Setup-Guide)
    1. [Sign Up](##Sign-Up)
    1. [Push Metadata to LakeView](##Push-Metadata-to-LakeView)
1. [FAQ](#FAQ)
1. [LICENSE](#LICENSE)

# Intro to LakeView

LakeView is a free product provided by Onehouse for the Apache Hudi community. LakeView exposes an interactive interface with pre-built metrics, charts, and alerts to help you monitor, optimize, and debug your data lakehouse tables.

All of this is possible in just a few steps without sending any table base data to LakeView:
1. Contact the Onehouse team at gtm@onehouse.ai to get your email allowlisted
2. Sign up [here](https://cloud.onehouse.ai/signup/lakeview)  and create an API token in the Onehouse UI
3. Run the metadata extractor tool in this repo to continuously push table metadata to LakeView (no base data files are pushed)

LakeView currently supports Apache Hudi tables stored on Amazon S3 and Google Cloud Storage. We plan to support additional clouds and table formats in the future.

**Feature Highlights:**
- UI view of all tables with key metrics and charts (table size, updates over time, etc.)
- Visual searchable timeline history
- Optimization insights for data skew, partition sizing, and file sizing
- Compaction backlog monitoring for Merge on Read tables
- Email & Slack updates and notifications for common issues

# Product Walkthrough

## Explore your Tables

After you have pushed your metadata, you should see a “Metadata received” banner on the homepage. You can view your tables by clicking the link on this banner or the “Data” tab on the sidebar.

<img width="1404" alt="Screenshot 2024-06-17 at 5 59 54 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/454134f0-bf77-4511-9c32-fe8364bec7f0">

On the Data page, you will see all of your tables organized under Lakes and Databases. Click on a table to view the relevant dashboards, metrics, and history.

## Table Stats

Table statistics give you a birds-eye view of the table’s status and key trends. You can track the volume of data written and other important metrics to help you understand your tables.

Click the “Overview” tab under the table to see statistics about the table.

<img width="914" alt="Screenshot 2024-06-17 at 6 00 00 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/20294a96-9069-49af-80e1-6898101d7bb7">
<img width="791" alt="Screenshot 2024-06-17 at 6 00 04 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/b4e5f087-d667-4101-b91c-9e024c4ef31f">


## Timeline History

Timeline History gives you a searchable, visual interface for the Hudi timeline. This can be particularly useful when debugging issues with your tables.

Click the “History” tab under the table to see the Hudi timeline history. This view shows timeline events such as Commits, Cleaning, and Clustering. You can search and filter for particular table events.

<img width="833" alt="Screenshot 2024-06-17 at 6 00 12 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/b7059603-05e5-489a-87b2-3ab85cab8b09">

If you want to view additional details about a timeline event, click “Details”.

<img width="796" alt="Screenshot 2024-06-17 at 6 00 20 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/45eeadaa-4981-430d-b47a-0c1786eafeca">

## Compaction Backlog Monitoring

Compaction Backlog Monitoring (available for Merge on Read tables only) can help you identify log file build-ups that cause out-of-date data, or identify opportunities to optimize your compaction settings.

Click the “Compaction Backlog” tab under the table to see the status of recent compactions and log file build-up.

<img width="944" alt="Screenshot 2024-06-17 at 6 00 24 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/c99fcf6a-4dec-4ae6-bb0d-a8e297923efb">

**Identifying compaction issues**
For Merge on Read tables, your real-time views will be outdated if data is not compacted. You can identify uncompacted file groups by checking the number of log files and the time of last compaction. If log files are increasing significantly or compaction has not run in a long time on a file group, you should ensure that compaction is running and consider adjusting your compaction strategy.

## Partition Insights

Partition Insights can help you optimize your table performance by improving partition schemes. You may also track partition skew and file size issues that affect performance.

Click the “Partition Insights” tab under the table to see charts and statistics about your partition sizes.

<img width="878" alt="Screenshot 2024-06-17 at 6 00 30 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/e202c9b9-8ac3-494c-af59-ee9ba7dcebcb">

**Optimizing partition sizes:** Operations are typically most efficient when partitions are similar in size. Onehouse provides insights to identify partition sizing inefficiencies:
The Partition Size Distribution chart shows the number of partitions that fall into each size range. Ideally, this chart should look like a bell curve with most partitions near the average size and the range (difference between the max and min partition size) should be small.
Data Skew indicates if a partition is significantly smaller or larger in total size than the average partition in the table. Onehouse If you find that the Data Skew is high in magnitude for certain partitions, you may reconsider your partitioning strategy to even out partition sizes.

**Optimizing file sizes:** Small file sizes can lead to poor query performance because they require you to scan more files in order to query data. You can use the p10, p50, etc. file sizes to understand if a partition contains small files. If you identify small files, review your file sizing configs and ensure that these services are running properly. Feel free to leverage the Hudi Slack community for further support on any questions.

## Notifications

Notifications allow you to receive alerts to proactively identify issues and inefficiencies in your tables.

Click the “Notifications” page in the sidebar or the bell icon at the top of the screen to open notifications. By default, notifications are turned off. To turn on notifications, click the gear icon on the notifications page and configure your notification settings.

<img width="852" alt="Screenshot 2024-06-17 at 6 00 35 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/bdf00d9c-d9fc-4053-8783-b1280952ae2e">

## Weekly Review Emails

LakeView will send you weekly review emails to help you stay on top of important activity in your Hudi deployment. These emails include trends, new tables, and potential issues.

No action is required to set up weekly review emails – you will receive them automatically when you create or join a LakeView project.

<img width="552" alt="Screenshot 2024-06-17 at 6 00 42 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/30f15f8c-1795-4568-ae87-26ece172327e">

# Setup Guide

## Sign Up

1. Contact gtm@onehouse.ai to get allowlisted, then visit https://cloud.onehouse.ai/signup/community-edition to create your account.
1. Upon signing up, you will create a new LakeView project.
1. In the sidebar, open Settings > API Settings, then generate a new token. You will use this token in the next step.
1. Return to the homepage, where you will be prompted to push your Hudi metadata to the project. LakeView works with just your .hoodie metadata. Base data never leaves your cloud. 

<img width="1207" alt="Screenshot 2024-06-17 at 6 04 02 PM" src="https://github.com/onehouseinc/hudi-metadata-extractor/assets/30377815/9005db67-e96f-439b-b223-81e0150d40d5">

## Push Metadata to LakeView

Onehouse provides a metadata extractor tool that you can run within your AWS or Google Cloud environment to continuously push metadata from specified file storage path(s) to LakeView. 

Key functionality of the metadata extractor tool:
- Data Security: The tool interacts exclusively with metadata. It does not access, move, or control actual data files, ensuring the integrity and confidentiality of your data.
- Operational Modes:
  - `CONTINUOUS` - The tool periodically discovers and uploads metadata for tables found in the configured path. Table discovery happens every 30minutes and new commit instants for the files are discovered and extracted every 5minutes (provided the previous run has completed).
  - `ONCE` - Allows users to trigger the discovery and extraction flow on demand, the tool picks up from where it left off on the last run. This can be useful if you want to run the metadata extractor tool as a recurring job that you manage.
- Efficient Data Processing: Utilizes checkpoints for incremental metadata extraction, enabling frequent and efficient metric updates.


Supported Cloud Platforms:
- Amazon Web Services (AWS)
- Google Cloud Platform (GCP)

Metadata Pushed to LakeView:
- The instant files in the active and archived timeline of the tables
- The last modification timestamp of the instant files (this is used to incrementally process changes)


### Step 1: Install the Metadata Extractor Tool

There are three methods to deploy the metadata extractor tool:
1. **Download and run a JAR package:** Download the latest JAR from the [release's page](https://github.com/onehouseinc/LakeView/releases/) prefer using the latest stable release
1. **Download and install a Helm chart in Kubernetes:** Deploy the tool in your Kubernetes cluster using our Helm chart (present in `/helm-chart` directory).
1. **Pull and run a Docker image:** Pull from [docker hub](https://hub.docker.com/r/onehouse/lake-view) and run in supported environments.**Note:** stable releases start with the prefix `release-v`.
  (image: `onehouse/hudi-metadata-extractor`)

### Step 2: Configure the Metadata Extractor Tool

The configuration file for the metadata extractor tool is a YAML file that specifies various settings required for the tool's operation. Below is a detailed explanation of each section and its respective fields within the configuration file.

#### YAML Configuration File Structure
``` YAML
version: V1

onehouseClientConfig:
    # can be obtained from the Onehouse console
    projectId: <LakeView project id>
    apiKey: <api key>
    apiSecret: <api secret>
    userId: <user id>
    file: <absolute path of json/yaml file containing onehouse client configuration>

fileSystemConfiguration:
    # Provide either s3Config or gcsConfig
    s3Config:
        region: <aws-region>
        accessKey: <optional>
        accessSecret: <optional>
    gcsConfig:
        projectId: <optional projectId>
        gcpServiceAccountKeyPath: <optional path_to_gcp_auth_key>

metadataExtractorConfig:
    jobRunMode: CONTINUOUS | ONCE
    pathExclusionPatterns: [<pattern1>, <pattern2>, ...]
    parserConfig:
        - lake: <lake1>
          databases:
            - name: <database1>
              basePaths: [basepath11, basepath12, ...]
            - name: <database2>
              basePaths: [<path1>, <path2>, ...]
        # Add additional lakes and databases as needed
```

#### Configs

**version**
- **Description:** Specifies the configuration format version.
- **Format:** String
- **Example:** version: V1

Note: Currently, only version V1 is supported.

**onehouseClientConfig**
- Description: Contains credentials for communicating with the Onehouse console. these values can be obtained from the Onehouse console
- **projectId:** Your Onehouse project ID. Get this by clicking on your profile in the top right of the Onehouse console.
- **userUuid:** The user ID for accessing the service. Get this by clicking on your profile in the top right of the Onehouse console.
- **apiKey:** The API key for authentication. Get this by opening Settings > API Settings in the Onehouse console and creating an API key.
- **apiSecret:** The corresponding secret for the API key. Get this by opening Settings > API Settings in the Onehouse console and creating an API key.
- **[Optional] file:** Absolute path of json/yaml file containing onehouseClientConfig details - projectId, userId, apiKey, apiSecret - if you wish to break them out into a separate file.

**fileSystemConfiguration**
- Description: Authentication configuration to access file system, only one of AWS S3 or Google Cloud Storage (GCS) credentials should be passed.
- **s3Config**
  - **region:** AWS region of the S3 bucket.
  - **[Optional] accessKey:** AWS access key (not recommended for production).
  - **[Optional] accessSecret:** AWS secret key (not recommended for production).
  - Note: If access keys are not provided, we use the default AWS credentials chain. For example, you can run the package in an EC2 instance with IAM access to read from S3.
- **gcsConfig**
  - **[Optional] projectId:** GCP project ID.
  - **[Optional] gcpServiceAccountKeyPath:** Path to the GCP service account key.
  - Note: If a service account key is not provided, we use the application default credentials. For example, you can run the package in a Compute Engine instance with IAM access to read from GCS.

**metadataExtractorConfig**
- Description: Configuration for the metadata extraction job.
- **jobRunMode:** Can be CONTINUOUS or ONCE.
- **pathExclusionPatterns:** List of regex patterns to exclude from scanning. (Java regex patterns are supported)
- **parserConfig**
  - Description: List of lakes and databases to be parsed.
  - **lake:** Name of the lake (optional, defaults to community-lake). This can be used to organize tables in the Onehouse console under the format Lake > Database > Table.
    - **databases:** List of databases and their respective base paths. This can be used to organize tables in the Onehouse console under the format Lake > Database > Table.
      - **name:** Database name (optional, defaults to community-db ).
      - **basePaths:** List of paths which the extractor needs to look into to find hudi tables. the paths can be paths to hudi tables or a path to a directory containing hudi tables. The paths should start with `s3://` when using S3 or `gs://` when using GCS.

### Step 3: Deploy the Metadata Extractor Tool

#### Deploy with JAR

Use command line arguments for the JAR file:
```BASH
# Option 1: YAML configuration string
java -jar <jar_path> -c '<yaml_string>'

# Option 2: YAML configuration file (local filepaths only)
java -jar <jar_path> -p '<path_to_config_file>'
```

#### Deploy with Docker

Use command line arguments for the Docker image:
```BASH
# Option 1: YAML configuration string
docker run <image_name> -c '<yaml_string>'

# Option 2: YAML configuration file (local filepaths only)
docker run <image_name> -p '<path_to_config_file>'
```

Note: When testing Docker locally, use `-v` to mount the volume where the configuration is present.

#### Deploy to Kubernetes with Helm

1. Clone the github repo
1. Navigate into the helm-chart folder by running: `cd helm-chart`
1. Create the values.yaml file, refer to the fields in [supported_values](helm-chart/values.yaml)
1. Install using helm: `helm install lake-view . -f <path to values.yaml>`

#### Deploy with AWS Glue using JAR File

1. Upload the jar file to a S3 location, this has to be accessible via an IAM role used by the Glue job.
1. Create a glue job with a script. Please find a sample script to be used in Glue below. Here, the config.yaml is embedded as part of the script itself. This is a minified version of the same config.yaml file. Please update the parameters like `PROJECT_ID`, `API_KEY`, `API_SECRET`, `USER_ID`, `REGION`, `LAKE_NAME`, `DATABASE_NAME`, `BASE_PATH_1`, `BASE_PATH_2` etc. in the config.
```python
import pyspark
import pyspark.sql.types as T
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext

conf = pyspark.SparkConf()

glueContext = GlueContext(SparkContext.getOrCreate(conf=conf))

spark_session = glueContext.spark_session
spark_session.udf.registerJavaFunction(name="glue_wrapper", javaClassName="com.onehouse.GlueWrapperMain", returnType=T.StringType())
spark_session.sql("SELECT glue_wrapper('[\"-c\", \"{version: V1, onehouseClientConfig: {projectId: ${PROJECT_ID}, apiKey: ${API_KEY}, apiSecret: ${API_SECRET}, userId: ${USER_ID}}, fileSystemConfiguration: {s3Config: {region: ${REGION}}}, metadataExtractorConfig: {jobRunMode: ONCE, parserConfig: [{lake: ${LAKE_NAME}, databases: [{name: ${DATABASE_NAME}, basePaths: [${BASE_PATH_1}, ${BASE_PATH_2}]}]}]}}\"]') as answer").show()
```
3. Configure the Glue Job details.
   1. Set up the IAM role to be used by glue. **Note**: This role should be able to access the JAR file from S3 and also to the base paths mentioned in the config.
   1. Under `Advanced properties > Libraries`, specify the JAR's S3 location in `Python library path` & `Dependent JARs path` fields.

# FAQ

## How is LakeView different from Onehouse Cloud?

LakeView is a standalone product offered by Onehouse for free to the Apache Hudi community. These are the core differences between the two products:

| | LakeView | Onehouse Cloud |
|-|-|-|
| **Description** | Observability tools to help you analyze, debug, and optimize Apache Hudi tables | Fully managed ingestion and incremental ETL pipelines, managed table services, and interoperability with Apache Hudi, Apache Iceberg, and Delta Lake. [Learn more.](https://www.onehouse.ai/product) |
| **Cost** | Free | Free for 30 days, then compute-based pricing |
| **Deployment** | You send Onehouse your Hudi metadata. Onehouse processes the metadata and exposes metrics and charts on the Onehouse console. | Onehouse deploys an agent in your Cloud account to manage clusters and jobs without sending data out of your VPC. Only the Hudi metadata is sent to Onehouse to populate the Onehouse console. |
| **Who's it for** | Apache Hudi users with existing data lakes | 1) Anyone looking to build new ingestion or ETL pipelines with Apache Hudi, Apache Iceberg, and/or Delta Lake OR 2) Apache Hudi users who want managed table services |

## Will Onehouse see my column stats if I have enabled the Hudi metadata table?

Onehouse will NOT see your column stats. While column stats are present in the [metadata table](https://hudi.apache.org/docs/metadata) under the .hoodie folder, the metadata extractor tool does not send the metadata table to the Onehouse endpoint. We only copy the commit files in .hoodie and .hoodie/archived. We do not copy the content of any other folders in the .hoodie directory. 

# Known Limitations
 
When using the metadata extractor tool, it's important to be aware of certain limitations that can affect its functionality and the accuracy of the metrics provided.

**Issues with Re-created Tables in the Same Path**
- Scenario: If a Hudi table is deleted and a new table is created in the same path, the tool assigns the same table ID as the previous one.
- Implication: This can lead to inaccuracies in the metrics calculation.
- Resolution: Users should delete such tables from the Onehouse console to avoid metric calculation issues.

**Changing Lake or Database Names After Metric Ingestion**
- Scenario: If the lake or database name for a given base path is changed after metrics have already been ingested, the tool will not use the new names.
- Implication: Continuity in metric tracking may be lost due to the change in identifiers.
- Resolution: Users should delete the table from the Onehouse console before making such changes.

**Changing Table Type After Metric Ingestion**
- Scenario: If the table type is changed after metrics have already been ingested, the tool will not use the new table_type.
- Implication: Some metric calculations could be wrong.
- Resolution: Users should delete the table from the Onehouse console before making such changes.

**Pausing and Resuming Metadata Ingestion**
- Scenario: If metadata ingestion for a table is paused and then resumed after a few days, there's a risk that the metrics displayed may be inaccurate (mainly depends on archival configs).
- Reason: The tool processes the archived timeline only once when the table is first discovered. If new instants are committed and moved to the archived timeline while the tool is not running, these instants will not be uploaded upon resumption, leading to potential data loss.
- Implication: There can be a mismatch between the actual table state and the metrics shown.
- Resolution: Users should delete the table from the Onehouse console before running the tool again in case of long wait times.

# LICENSE

This repository is licensed under the terms of the Apache 2.0 license

