import requests
import json
import boto3


def loadPlayers(sport):
  url = "http://api.cbssports.com/fantasy/players/list?version=3.0&SPORT=%s&response_format=JSON" %sport
  r = requests.get(url)
  playerDict = json.loads(r.text)
  players = playerDict['body']['players']
  for player in players:
    for key in player.keys(): #I am not able to iterate over player.items()
      value = player[key]
      if value == '':
        del player[key]
    player.update({'sport': sport})
  return players



def main():
  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  playerTable = dynamodb.Table('Players')

  baseballPlayers = loadPlayers('baseball')
  footballPlayers = loadPlayers('football')
  basketballPlayers = loadPlayers('basketball')
  playerJsons = [baseballPlayers, footballPlayers, basketballPlayers]
  with playerTable.batch_writer() as batch:
    for playerJson in playerJsons:
      for player in playerJson:
        batch.put_item(player)

if __name__ == '__main__':
  main()