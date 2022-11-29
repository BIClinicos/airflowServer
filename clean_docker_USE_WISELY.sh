#!/bin/sh
# USE IT WISELY AS IT DELETES ALL DOCKER STUFF
sudo docker-compose down -v --rmi all
sudo docker kill $(docker ps -q)
sudo docker rm $(docker ps  -q)
sudo docker rmi $(docker images -a -q)
sudo docker system prune -a -f
sudo docker-compose up
