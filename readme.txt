
*****************************************************
*****************************************************
JUST IN CASE SOMETHING GOES TERRIBLY WRONG!!
execute:
sudo bash clean_docker_USE_WISELY.sh
--> this deletes all docker data
*****************************************************
*****************************************************


* to spin up services:
sudo docker-compose up 

* if you added a new python package in requirements.txt:
sudo docker-compose up --build
--> this wont delete all historic data

* you can create connections in three ways:
1) with the UI (nothing to explain here)
2) with a python script:
execute python entering the scheduler/webserver console

- modify upload_connections.py with your connections
- sudo docker-compose exec scheduler /bin/bash
- copy->paste the file contents into a python3 console in bash shell

3) addding them as variables in .env file
--> these variables wont appear in the UI as they don't get loaded in metadata DB