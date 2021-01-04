# Requirements:
1. Assume that user has a dedicated conda package manager and docker engine
2. The project data has to be storedwith this path: /Your_project_name/dataset/ 

# Notes
It is essential to install Mongodb Compass (Mongodb UI). 
Mongodb URI formed as: mongodb://<username>:<password>@<host>:<port>/?authSource=admin
Where username: admin, password:secret, host:localhost, port:27888

# Steps:

1. Create new conda environment
```bash
$ conda create -n mongodb python=3.7 -y
$ conda activate mongodb

```

2. Fetch the project code base
```bash
$ git clone git@github.com:vipvitalia/Anomaly-detection-and-prediction-in-operational-log-data.git
$ cd  /Anomaly-detection-and-prediction-in-operational-log-data/mongodb && pip install -r requirements.txt
```

3. Deploy a mongo container
```bash
$ docker run -d -v <ABSOLUTE PATH TO PROJECT>/Anomaly-detection-and-prediction-in-operational-log-data/dataset:/data/db --name mongodb  -p 27888:27017 -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=secret mongo
For example: docker run -d -v /Users/vitalij/Downloads/stream//Anomaly-detection-and-prediction-in-operational-log-data/dataset:/data/db --name mongodb  -p 27888:27017 -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=secret mongo 
```

# Additional docker commands
- Connect to a running container
```bash
$  docker exec -it mongodb bash
```

