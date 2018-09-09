import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging.config
import os

#dir_path=os.path.dirname(os.path.realpath(__file__))
#logConfigPath=os.path.join(dir_path, 'config', 'logconfig.json')
#
#logger=logging.getLogger(__name__)
##logger.setLevel(logging.INFO)
#setupLogging(logConfigPath)

def getPlayer(table, sport, id):
  result=table.query(KeyConditionExpression=Key('sport').eq(sport) & Key('id').eq(id))
  if result['Count'] > 0:
    return result['Items']
  else:
    logger.info('could not find user with sport %s and id %s' % (sport, id))
    return None

def getSportsPlayers(table, sport):
  response=table.query(ProjectionExpression="id, fullname",
                             KeyConditionExpression=Key('sport').eq('baseball'))
  sportsPlayers=response['Items']
  playerCount=0
  while 'LastEvaluatedKey' in response:
    playerCount+=response['Count']
    for key in response:
      print(key)
    response=table.query(ProjectionExpression="id, fullname",
                               KeyConditionExpression=Key('sport').eq('baseball'),
                                ExclusiveStartKey=response['LastEvaluatedKey'])
    sportsPlayers=sportsPlayers+response['Items']
  playerCount+=response['Count']
  return sportsPlayers, playerCount

def createPlayer(table, playerDict):
  if 'id' not in playerDict or 'sport' not in playerDict:
    logger.error("could not create player.  id or sport not in playerDictionary for object: \n %d" % playerDict)
  if(getPlayer(table, 

def setupLogging(logConfigPath, defaultLevel=logging.INFO):
  if(not os.path.isfile(logConfigPath)):
    logging.basicConfig(level=defaultLevel)
  else:
    with open(logConfigPath) as logConfigFile:
      logging.config.dictConfig(json.load(logConfigFile))

def main():
  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  playerTable = dynamodb.Table('Players')
  global logger
  dir_path=os.path.dirname(os.path.realpath(__file__))
  logConfigPath=os.path.join(dir_path, 'config', 'logconfig.json')
  logger=logging.getLogger(__name__)
  #logger.setLevel(logging.INFO)
  setupLogging(logConfigPath)

 
#  players, count=getSportsPlayers(playerTable, 'baseball'
#  print(players)
  player=getPlayer(playerTable, 'baseball', '5856262312')
  print(player)

if __name__ == '__main__':
  main()
