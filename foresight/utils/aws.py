"""AWS utility functions"""

import os

import boto3


def get_client(service_type: str):
    """Get a service client."""

    endpoint_url = os.getenv("AWS_ENDPOINT_URL", None)

    if endpoint_url is None:
        return boto3.client(
            service_type,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
    else:
        return boto3.client(
            service_type,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
            endpoint_url=endpoint_url,
        )


def get_resource(service_type: str):
    """Get a service resource."""

    endpoint_url = os.getenv("AWS_ENDPOINT_URL", None)

    if endpoint_url is None:
        return boto3.resource(
            service_type,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
    else:
        return boto3.resource(
            service_type,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
            endpoint_url=endpoint_url,
        )
