# cloud search 

**Design & build application that updates and search documents in ES for any changes in S3** 


**Requirements**
1. Connect to online data storage
2. Use API to fetch the files
3. Index the context to maximum
4. Provide API that takes term/token as input
5. Return list of files and http urls that contain the term in their content
6. Provide command line or web interface that consumes above API and displays the files matching the given query


**Technologies used :**
1. Apache Maven 3.3.9
2. Dropwizard web server 2.1.4
3. AWS S3 & SQS
4. Elasticsearch 8.9.1


**App Endpoints**

To query the elasticsearch
http://localhost:8090/data/search?q=g

To delete the index 
http://localhost:8090/data/delete/indexname

To check the health of app
http://localhost:8091/healthcheck


**Deployment**
Fargate (for uneven load) & CDK

**Logging**
Kibana

**Metrics**
Datadog
