aws ecr create-repository --repository-name discogs-etl
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 232835081793.dkr.ecr.us-east-1.amazonaws.com

docker buildx build --platform linux/amd64 -t discogs-etl .

docker tag discogs-etl:latest 232835081793.dkr.ecr.us-east-1.amazonaws.com/discogs-etl:latest
docker push 232835081793.dkr.ecr.us-east-1.amazonaws.com/discogs-etl:latest