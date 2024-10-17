import boto3
import json

class LambdaExecutor:
    def __init__(self, region_name='us-east-1'):
        self.lambda_client = boto3.client('lambda', region_name=region_name)

    def execute_function(self, function_name, payload):
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            return json.loads(response['Payload'].read())
        except Exception as e:
            print(f"Error executing Lambda function: {e}")
            return None

# Example usage
if __name__ == "__main__":
    executor = LambdaExecutor()
    result = executor.execute_function('your_lambda_function_name', {'key': 'value'})
    print(result)