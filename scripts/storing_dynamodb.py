import json
import boto3 
from boto3.dynamodb.conditions import Key


dynamo_table = "liveflightdata"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamo_table)
def lambda_handler(event, context):

    for record in event['Records']: 
        payload = json.loads(record['kinesis']['data'])
        flight_route = payload['flight_route']
        flight_num_code = payload['flight_num_code']
        
        response = table.get_item(
            Key={ 
                'flight_route': flight_route,
                'flight_num_code': flight_num_code  
            }
        
        )

        item = response.get('Item')

        if item: 
            #update record 

            update_expression = []
            expression_attribute_values = {}
            expression_attribute_names = {}

            for coord in ['latitude' , 'longitude']: 
                new_value = payload.get(coord)
                if new_value is not None:
                    update_expression.append(f"SET #coord_{coord} = list_append(if_not_exists(#coord_{coord}, :empty_list), :{coord})")
                    expression_attribute_names[f"#coord_{coord}"] = coord
                    expression_attribute_values[f":{coord}"] = [new_value]
                    expression_attribute_values[":empty_list"] = []
            
            for key , new_val in payload.items():
                #skip keys that are handled differently 

                if key in ['latitude' , 'longitude' , 'flight_route' , 'flight_num_code']:
                    continue
                old_val = item.get(key)
                if new_val != old_val:
                    update_expression.append(f"SET #{key} = :{key}")
                    expression_attribute_names[f"#{key}"] = key
                    expression_attribute_values[f":{key}"] = new_val 
            if update_expression: # if the values are different 
                table.update_item(
                    Key={
                        'flight_route': flight_route,
                        'flight_num_code': flight_num_code
                    },
                    UpdateExpression=" ".join(update_expression),
                    ExpressionAttributeValues=expression_attribute_values,
                    ExpressionAttributeNames=expression_attribute_names
                )
            
            else: 
                payload['latitude'] = [payload['latitude']] if "latitude" in payload else []
                payload['longitude'] = [payload['longitude']] if "longitude" in payload else []
                table.put_item(Item=payload)


    # TODO implement
    return {
        'statusCode': 200,
        'body': 'Processed Kinesis records into DynamoDB'
    }
