import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging.config
import os


def getPlayer(table, sport, id):
  result=table.query(KeyConditionExpression=Key('sport').eq(sport) & Key('id').eq(id))
  if result['Count'] > 0:
    return result['Items'][0]
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

def createPlayer(playerTable, sportTable, playerDict):
  dynamoClient=boto3.client('dynamodb')
  if (getSport(table=sportTable, sport=playerDict['sport'])==None):
    logger.error('user {userid} has an invalid sport: {sport}.  Please correct
                 the sport name or add the sport the sports
                 table.'.format(userid=playerDict['id'],
                                sport=playerDict['sport']))
    return
  try:
    result=playerTable.put_item(Item=playerDict,
                    ConditionExpression='attribute_not_exists(id)')
    logger.info('creation result is %s' % result)
    #todo: need to log if a modification occured.
  except dynamoClient.exceptions.ConditionalCheckFailedException as err:
    logger.error("Creation Failed.  User already exists: {0}".format(err))

def deletePlayer(table, sport, id):
  result=table.delete_item(Key={'id': id, 'sport': sport})
  logger.info('result of deletion is : {0}'.format(result))

def updatePlayer(table,playerDict):
  dynamoClient=boto3.client('dynamodb')
  try:
    result=table.put_item(Item=playerDict,
                    ConditionExpression='attribute_exists(id)')
    logger.info('creation result is %s' % result)
    #todo: need to log if a modification occured.
  except dynamoClient.exceptions.ConditionalCheckFailedException as err:
    logger.error("Update Failed.  User does not exist: {0}".format(err))

def createSport(table,sportDict):
  dynamoClient=boto3.client('dynamodb')
  try:
    result=table.put_item(Item=sportDict,
                    ConditionExpression='attribute_not_exists(sport)')
    logger.info('creation result is %s' % result)
    #todo: need to log if a modification occured.
  except dynamoClient.exceptions.ConditionalCheckFailedException as err:
    logger.error("Creation Failed.  User already exists: {0}".format(err))

def getSport(table, sport):
  result=table.query(KeyConditionExpression=Key('sport').eq(sport))
  if result['Count'] > 0:
    return result['Items'][0]
  else:
    logger.info('could not find sport %s' % (sport))
    return None

def deleteSport(sport, id):
  result=table.delete_item(Key={ 'sport': sport})
  logger.info('result of deletion is : {0}'.format(result))

def setupLogging(logConfigPath, defaultLevel=logging.INFO):
  if(not os.path.isfile(logConfigPath)):
    logging.basicConfig(level=defaultLevel)
  else:
    with open(logConfigPath) as logConfigFile:
      logging.config.dictConfig(json.load(logConfigFile))


def main():
  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  dynamoClient=boto3.client('dynamodb')
  playerTable = dynamodb.Table('Players')
  global logger
  dir_path=os.path.dirname(os.path.realpath(__file__))
  logConfigPath=os.path.join(dir_path, 'config', 'logconfig.json')
  logger=logging.getLogger(__name__)
  #logger.setLevel(logging.INFO)
  setupLogging(logConfigPath)
  
  sportTable=dynamodb.Table('Sports')
  baseball={'sport':'baseball'}
  football={'sport':'football'}
  basketball={'sport':'basketball'}
  createSport(table=sportTable, sportDict=football)
  createSport(table=sportTable, sportDict=basketball)
#  players, count=getSportsPlayers(playerTable, 'baseball'
#  print(players)
#  player=getPlayer(playerTable, 'baseball', '585626')
#  player['firstname']='Rick'
#  player['id']='5856262548'
#  deletePlayer(playerTable, player['sport'], player['id'])
#  player=getPlayer(playerTable, 'baseball', '5856262548')
#  print(player)
if __name__ == '__main__':
  main()
