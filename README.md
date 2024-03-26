# cozie-apple-backend
Repository for all cloud infrastructure needed for the Cozie-Apple app

For maintainers:
    - Do not share the AWS SAM file (.yaml) for AWS Lambda functions. These file contain environment variables.

# Lambda functions
- [cozie-apple-v3-app-write-queue]()
- [cozie-apple-v3-app-write-influx-queue](./lambda_cozie-apple-v3-app-write-queue/)
- [cozie-apple-v3-app-read-influx]()
- [cozie-apple-v3-researcher-read-influx]()
- [cozie-apple-v3-researcher-push-notification](./cozie-apple-v3-researcher-push-notification/)