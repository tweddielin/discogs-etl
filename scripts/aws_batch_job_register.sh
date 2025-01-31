aws batch register-job-definition \
    --job-definition-name discogs-etl-job \
    --type container \
    --container-properties '{
        "image": "232835081793.dkr.ecr.us-east-1.amazonaws.com/discogs-etl:latest",
        "vcpus": 2,
        "memory": 2048,
        "executionRoleArn": "arn:aws:iam::232835081793:role/BatchServiceeRole",
        "jobRoleArn": "arn:aws:iam::232835081793:role/BatchServiceeRole",
        "command": ["python", "run.py"],
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": "/aws/batch/job",
                "awslogs-region": "us-east-1",
                "awslogs-stream-prefix": "discogs-etl-job"
            }
        }
    }'