# spark-crawl-transformation
||
This repo contains PySpark jobs that take Common Crawl warc files, transform them and save them into BigQuery tables

### Docker image
1. Run command `docker build --platform linux/x86_64 -t "nameOfYourImage:version" .`
2. Tag your image, you can get the imageId from Docker desktop. 
    `docker tag imageId gcpArtifactoryPath/imageName:version`
3. Push your image to Artifactory Registry
    `docker push gcpArtifactoryPath/imageName:version`


### Submit Spark Batch
1. Give execution permission, run command:
    `chmod +x submit.sh`
2. Modify the paths to yours
3. Change batch name in submit.sh file everytime you submit the batch workload
4. Submit batch by running `./submit.sh`






