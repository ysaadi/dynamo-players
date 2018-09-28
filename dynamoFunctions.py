import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging.config
import os
import logging

logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
dynamoClient=boto3.client('dynamodb')
sportTable=dynamodb.Table('Sports')

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
    response=table.query(ProjectionExpression="id, fullname",
       KeyConditionExpression=Key('sport').eq('baseball'),
       ExclusiveStartKey=response['LastEvaluatedKey'])
    sportsPlayers=sportsPlayers+response['Items']
  playerCount+=response['Count']
  return sportsPlayers

def createPlayer(playerTable, sportTable, playerDict):
  dynamoClient=boto3.client('dynamodb')
  if (getSport(table=sportTable, sport=playerDict['sport'])==None):
    logger.error('user {userid} has an invalid sport: {sport}.  Please correct the sport name or add the sport the sports table.'.format(userid=playerDict['id'], sport=playerDict['sport']))
    return
  try:
    result=playerTable.put_item(Item=playerDict,
                    ConditionExpression='attribute_not_exists(id)')
    logger.info('creation result is %s' % result)
    return 0
    #todo: need to log if a modification occured.
  except dynamoClient.exceptions.ConditionalCheckFailedException as err:
    logger.error("Creation Failed.  User already exists: {0}".format(err))

def deletePlayer(table, sport, id):
  #todo check if deletion is successful
  result=table.delete_item(Key={'id': id, 'sport': sport})
  logger.info('result of deletion is : {0}'.format(result))

def updatePlayer(table, id, sport, playerDict):
  dynamoClient=boto3.client('dynamodb')
  #todo, this does not handle modifying a player's sport.
  if not validatePlayer(playerDict):
    logger.error('player validation failed')
    abort(400)
  Keys={'sport': playerDict.pop('sport'), 'id': playerDict.pop('id')}
  updateExpression, updateAttributeDict, updateAttributeNames=generateUpdateExpression(playerDict)
  try:
    result=table.update_item(Key=Keys,
                             UpdateExpression=updateExpression,
                             ExpressionAttributeValues=updateAttributeDict,
                             ExpressionAttributeNames=updateAttributeNames,
                    ConditionExpression='attribute_exists(id)',
                            ReturnValues='UPDATED_OLD')
    logger.info('creation result is %s' % result)
    print(result)
    return result['Attributes']
    #todo: need to log if a modification occured.
  except dynamoClient.exceptions.ConditionalCheckFailedException as err:
    logger.error("Update Failed.  User does not exist: {0}".format(err))

def generateUpdateExpression(playerDict):
  updateValueDict={}
  updateKeyDict={}
  deleteAttributes=[]
  updateExpression='set'
  attributeCount=1
  firstIter=True
  for key in playerDict.keys():
    val = playerDict[key]
    if val=='':
      deleteAttributes.append(key)
      continue
    valueSub= ':A' + str(attributeCount)
    attributeCount+=1
    keySub='#'+key
    updateValueDict[valueSub]=val
    updateKeyDict[keySub]=key
    if firstIter:
      updateExpression+=" {0} = {1}".format(keySub, valueSub)
      firstIter=False
    else:
      updateExpression+=", {0} = {1}".format(keySub, valueSub)
  if deleteAttributes:
    delString=' REMOVE'
    firstIter=True
    for deleteAttribute in deleteAttributes:
      if firstIter:
        delString+=' {}'.format(deleteAttribute)
        firstIter=False
      else:
        delString+=', {}'.format(deleteAttribute)
    updateExpression+=delString
  return updateExpression, updateValueDict, updateKeyDict


def createSport(table,sportDict):
  dynamoClient=boto3.client('dynamodb')
  try:
    result=table.put_item(Item=sportDict,
                    ConditionExpression='attribute_not_exists(sport)')
    logger.info('creation result is %s' % result)
    #todo: need to log if a modification occured.
  except dynamoClient.exceptions.ConditionalCheckFailedException as err:
    logger.error("Creation Failed.  Sport already exists: {0}".format(err))

def getSports(table):
  result=table.scan()
  return result['Items']

def getSport(table, sport):
  result=table.query(KeyConditionExpression=Key('sport').eq(sport))
  if result['Count'] > 0:
    return result['Items'][0]
  else:
    logger.info('could not find sport %s' % (sport))
    return None

def deleteSport(table, sport):
  result=table.delete_item(Key={ 'sport': sport}, ReturnValues='ALL_OLD')
  if 'Attributes' not in result.keys():
    return None
  return result['Attributes']

def setupLogging(logConfigPath, defaultLevel=logging.INFO):
  if(not os.path.isfile(logConfigPath)):
    logging.basicConfig(level=defaultLevel)
  else:
    with open(logConfigPath) as logConfigFile:
      logging.config.dictConfig(json.load(logConfigFile))

def validatePlayer(playerDict):
  if 'id' not in playerDict.keys() or 'sport' not in playerDict.keys():
    logger.error('Validation failed.  id or sport not found in playerDict' \
                 '{0}'.format(json.dumps(playerDict)))
    return False
  elif (getSport(table=sportTable, sport=playerDict['sport'])==None):
    logger.error('user {userid} has an invalid sport: {sport}.  Please correct the sport name or add the sport the sports table.'.format(userid=playerDict['id'],
                                                                                                                                        sport=playerDict['sport']))
    return False
  return True

def main():
  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  dynamoClient=boto3.client('dynamodb')
  playerTable = dynamodb.Table('Players')
#  global logger
#  dir_path=os.path.dirname(os.path.realpath(__file__))
#  logConfigPath=os.path.join(dir_path, 'config', 'logconfig.json')
#  logger=logging.getLogger(__name__)
#  #logger.setLevel(logging.INFO)
  sportTable=dynamodb.Table('Sports')
  deleteReturn=deleteSport(sportTable,'soccer')
#  players=getSportsPlayers(playerTable, 'baseball'
#  print(players)
  player=getPlayer(playerTable, 'baseball', '585626')
  print(str(player))
  player['randomAttr']=''
  result=updatePlayer(playerTable, player)
#  player['id']='5856262548'
#  deletePlayer(playerTable, player['sport'], player['id'])
#  player=getPlayer(playerTable, 'baseball', '5856262548')
#  print(player)
if __name__ == '__main__':
  main()
