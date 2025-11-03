## serverless-time-series-extraction-from-satellite-weather-data

This project was inspired from the AWS blog https://aws.amazon.com/blogs/big-data/extract-time-series-from-satellite-weather-data-with-aws-lambda/, under the guidance of https://www.linkedin.com/in/mentorsachin/
and here, I have:

•	Designed and deployed a highly scalable serverless ETL pipeline on AWS to process ~ 100 GB of multidimensional satellite weather data (netCDF format) for time-series extraction.

•	Orchestrated parallel processing using AWS Step Functions to launch 365 concurrent AWS Lambda functions, significantly reducing the processing time for a full year of data to minutes.

•	Containerized the Lambda functions utilizing netCDF4 library to parse domain-specific netCDF files and Pandas to extract, structure, and save daily time-series data to Parquet format on S3, ensuring any level of complex dependencies or heavy customizations can be configured into Lambda runtime

•	Leveraged AWS Glue to consolidate the 365 files from Lambda, and re-partition the dataset from daily key to a geographical point ID key, generating 100 time-series datasets, for each point, having a size of **ONLY 23.8MB** - a big reduction from the original ~100GB

More details on the project here:

https://www.linkedin.com/posts/rushikesh-palnitkar_aws-awsblogs-s3-activity-7382944275350757376-8wX6?utm_source=share&utm_medium=member_desktop&rcm=ACoAADFLe9UBk8g_DOUq_bKIxryPsi0PZfsMV4Q
