import base64
import json
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SecretManager:
    def __init__(self, aws_region="cn-north-1"):
        self.aws_region = aws_region
        self.client = boto3.client('secretsmanager', region_name=aws_region)
    def get_secret(self, secret_name):
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
        except Exception as e:
            logger.error(f"get salesforce credential from AWS Secret Manager - Secret: {secret_name} error")
            logger.error("detail error massages: {e}")
            raise
        if 'SecretString' in response:
            credential = json.loads(response['SecretString'])
            return credential
        else:
            credential = base64.b64decode(response['SecretBinary'])
            return json.loads(credential)
