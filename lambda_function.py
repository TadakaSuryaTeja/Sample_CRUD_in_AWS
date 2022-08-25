import boto3
import json
from custom_encoder import CustomEncoder
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_table_name = "surya_sample_test"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"
healthPath = "/health"
productPath = "/product"
productsPath = "/products"


def lambda_handler(event, context):
    logger.info(f"event {event} context:{context}")
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "Suc")
    elif httpMethod == getMethod and path == productPath:
        response = getProduct(event['queryStringParameters']['id'])
    elif httpMethod == getMethod and path == productsPath:
        response = getProducts()
    elif httpMethod == postMethod and path == productPath:
        response = saveProduct(json.loads(event['body']))
    elif httpMethod == patchMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = modifyProduct(requestBody['id'], requestBody['updateKey'], requestBody[
            'updateValue'])
    elif httpMethod == deleteMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = deleteProduct(requestBody['id'])
    else:
        response = buildResponse(404, "Not Found")

    return response


def getProduct(id):
    try:
        response = table.get_item(Key={
                'id': id
                })
        if 'Item' in response:
            return buildResponse(200, response)
        else:
            return buildResponse(400, {'Message': f'id: {id} not found'})

    except:
        logger.exception('Error Message')


def getProducts():
    try:
        response = table.scan()
        result = response['Items']
        logger.info(f"result: {result}")
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
        body = {
                'products': result
                }
        return buildResponse(200, body)
    except:
        logger.exception("Error Message")


def saveProduct(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
                'Operation': 'SAVE',
                'Message': 'SUCCESS',
                'Item': requestBody
                }
        return buildResponse(200, body)
    except:
        logger.exception("Error Message")


def modifyProduct(id, updateKey, updateValue):
    try:
        response = table.update_item(
                Key={
                        'id': id
                        },
                UpdateExpression=f'set %s =:value' %updateKey,
                ExpressionAttributeValues={
                        ":value": updateValue
                        },
                ReturnValues='UPDATED_NEW'
                )
        body = {
                'Operation': 'UPDATE',
                'Message': 'SUCCESS',
                'UpdatedAttributes': response
                }
        return buildResponse(200, body)
    except:
        logger.exception("Error Message")


def deleteProduct(id):
    try:
        logger.info(id)
        response = table.delete_item(
                Key={'id': id},
                ReturnValues="ALL_OLD"

                )
        body = {
                'Operation': 'DELETE',
                'Message': 'SUCCESS',
                'deletedItem': response
                }
        return buildResponse(200, body)
    except:
        logger.exception("Error Message")


def buildResponse(status_code, body=None):
    response = {
            'statusCode': status_code,
            'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                    }
            }
    if body is None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response
