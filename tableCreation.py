import requests
import json
import boto3



def createPlayerTable(dynamodb):

  table = dynamodb.create_table(
    TableName='Players',
    KeySchema=[
      {
        'AttributeName': 'sport',
        'KeyType': 'HASH'  # Partition key
      },
      {
        'AttributeName': 'id',
        'KeyType': 'RANGE'  # Sort key
      }
    ],
    AttributeDefinitions=[
      {
        'AttributeName': 'sport',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'id',
        'AttributeType': 'S'
      },

    ],
    ProvisionedThroughput={
      'ReadCapacityUnits': 10,
      'WriteCapacityUnits': 10
    }
  )
  print("Table status:", table.table_status)

def main():
  r = requests.get('http://api.cbssports.com/fantasy/players/list?version=3.0&SPORT=baseball&response_format=JSON')
  baseballPlayersDict = json.loads(r.text)

  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  createPlayerTable(dynamodb)

if __name__ == '__main__':
  main()