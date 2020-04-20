import json
import boto3
import decimal
import helpers

def handle(event, context):
    
    # parse the body
    try:
        body = json.loads(event["body"])
    except (TypeError, json.decoder.JSONDecodeError):
        error = "Failed to parse body as JSON"
        statusCode = 400
        return helpers.formatErrorResponse(statusCode, error)
        
    # extract the increment by value
    try:
        incrementBy = body["incrementBy"]
    except KeyError:
        error = "Unable to extract incrementBy value from body"
        statusCode = 400
        return helpers.formatErrorResponse(statusCode, error)
        
    # convert increment by value to a number
    try:
        incrementBy = int(incrementBy)
    except (ValueError, TypeError):
        error = "incrementBy must be a string"
        statusCode = 400
        return helpers.formatErrorResponse(statusCode, error)
        
    # Get the path parameter to retrieve 
    try:
        counterId = event["pathParameters"]["counterId"]
    except KeyError:
        error = "Missing counterId in input path"
        statusCode = 400
        return helpers.formatErrorResponse(statusCode, error)
    
    # access the dynamodb table
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('counters')
    except:
        error = "Failed getting the dynamoDB table"
        statusCode = 500
        return helper.formatErrorResponse(statusCode, error)
        
    # Increment the counter 
    dynamoResponse = table.update_item(
        Key={
            'counterId': counterId
        },
        UpdateExpression="set theCount = theCount + :incrementBy",
        ExpressionAttributeValues={
            ':incrementBy': decimal.Decimal(incrementBy)
        },
        ReturnValues="UPDATED_NEW"
    )
    
    #responseBody = json.dumps(dynamoResponse, indent=4, cls=helpers.DecimalEncoder)
    
    try:
        dynamoResponseStatus = dynamoResponse["ResponseMetadata"]["HTTPStatusCode"]
        updatedCount = dynamoResponse["Attributes"]["theCount"]
    except:
        error = "Response from DynamoDB was in an unexpected format"
        statusCode = 500
        return helper.formatErrorResponse(statusCode, error)
        
    if dynamoResponseStatus != 200:
        error = f"Unexpected response status code {dynamoResponseStatus} from Dynamo"
        statusCode = 500
        return helper.formatErrorResponse(statusCode, error)
        
    response = {
        "statusCode": 201,
        "body": json.dumps({"count":updatedCount}, indent=4, cls=helpers.DecimalEncoder)
    }

    return response
