import requests
import json
import boto3



def createPlayerTable(dynamodb):

  table = dynamodb.create_table(
    TableName='Movies',
    KeySchema=[
      {
        'AttributeName': 'id',
        'KeyType': 'HASH'  # Partition key
      },
      {
        'AttributeName': 'sport',
        'KeyType': 'RANGE'  # Sort key
      }
    ],
    AttributeDefinitions=[
      {
        'AttributeName': 'id',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'sport',
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