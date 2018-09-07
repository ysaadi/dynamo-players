import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

def getPlayer(table, sport, id):
  result=table.query(KeyConditionExpression=Key('sport').eq(sport) & Key('id').eq(id))
  return result

def getSportsPlayers(table, sport):
  response=table.query(ProjectionExpression="id, fullname",
                             KeyConditionExpression=Key('sport').eq('baseball'))
  sportsPlayers=response['Items']
  while 'LastEvaluatedKey' in response:
    print(response['Count'])
    for key in response:
      print(key)
    response=table.query(ProjectionExpression="id, fullname",
                               KeyConditionExpression=Key('sport').eq('baseball'),
                                ExclusiveStartKey=response['LastEvaluatedKey'])
    sportsPlayers=sportsPlayers+response['Items']
  return sportsPlayers

def main():
  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  playerTable = dynamodb.Table('Players')
  players=getSportsPlayers(playerTable, 'baseball')
  #print(players)

if __name__ == '__main__':
  main()
