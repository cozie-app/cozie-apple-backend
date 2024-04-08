# cozie-apple-backend
Repository for all cloud infrastructure needed for the Cozie-Apple app

For maintainers:
    - Do not share the AWS SAM file (.yaml) for AWS Lambda functions. These file contain environment variables.

# Lambda functions called by Cozie-Apple app
- [cozie-apple-v3-app-write-queue](./lambda_cozie-apple-v3-app-write-queue/)
    - Is called by Cozie-Apple app
    - Insert data into SQS queue
- [lambda_cozie-apple-v3-app-write-influx-queue](./lambda_cozie-apple-v3-app-write-influx-queue/)
    - Is called by SQS queue
    - Writes data to InfluxDB 
- [cozie-apple-v3-app-read-influx](./lambda_cozie-apple-v3-app-read-influx-cozie/)
    - Is called by Cozie-Apple app
    - Reads InfluxDB
    - Provides information shown on top of the 'Data' tab in the Cozie-Apple app
- [cozie-apple-v3-app-write-influx-cozie/](./lambda_cozie-apple-v3-app-write-influx-cozie/) [Deprecated]
    - Is called by Cozie-Apple app
    - Writes data to InfluxDB
    - Can cause long waiting times at the end of watch survey.
    - Is replaced by [cozie-apple-v3-app-write-queue](./lambda_cozie-apple-v3-app-write-queue/) and [lambda_cozie-apple-v3-app-write-influx-queue](./lambda_cozie-apple-v3-app-write-influx-queue/)
 
# Lambda functions called by researcher
- [cozie-apple-v3-researcher-read-influx](./lambda_cozie-apple-v3-researcher-read-influx/)
    - Is called by researchers via web API to retrieve Cozie data from InfluxDB 
- [cozie-apple-v3-researcher-push-notification](./lambda_cozie-apple-v3-researcher-push-notification/)
    - Is called by researchers via web API to send push notifications 
