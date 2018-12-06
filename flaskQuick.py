from flask import Flask
from flask import request
from flask import abort
from flask import render_template
import boto3
from . import dynamoFunctions
import json

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
dynamoClient=boto3.client('dynamodb')
playerTable = dynamodb.Table('Players')
sportTable=dynamodb.Table('Sports')
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/sports', methods=['GET'])
def getSports():
  sports=dynamoFunctions.getSports(sportTable)
  sportsString=''
  for sport in sports:
    sportsString+=str(sport)+'\n'
  return sportsString

@app.route('/sports/<sport>', methods=['GET'])
def getSport(sport):
  sport=dynamoFunctions.getSport(sportTable, sport)
  return str(sport)+'\n'

@app.route('/sports/<sport>', methods=['POST'])
def createSport(sport):
  response=dynamoFunctions.createSport(sportTable, {'sport': sport})
  if response==None:
    abort(409)    #ToDo, print a custom error for this HTTP code.
  return str(response)

@app.route('/sports/<sport>', methods=['DELETE'])
def deleteSport(sport):
  response=dynamoFunctions.deleteSport(sportTable, sport)
  if response==None:
    abort(404)    #ToDo, print a custom error for this HTTP code.
  return str(response)

@app.route('/sports/<sport>/users', methods=['GET'])
def getSportsPlayers(sport):
  response=dynamoFunctions.getSportsPlayers(playerTable, sport)
  response=list(map(str, response))
  return '\n\n'.join(response)

@app.route('/sports/<sport>/users/<userid>', methods=['GET'])
def getPlayer(sport, userid):
  response=dynamoFunctions.getPlayer(playerTable, sport, userid)
  if response==None:
    abort(404)
  return str(response)

@app.route('/sports/<sport>/users/user', methods=['POST'])
def createPlayer(sport):
  #todo handle error if user exists
  if request.json==None:
    abort(400)
  response=dynamoFunctions.createPlayer(playerTable, request.json)
  if response==None:
    abort(400)
  return('Creation Successful for user with id {0}'.format(request.json['id']))

@app.route('/sports/<sport>/users/<userid>', methods=['DELETE'])
def deletePlayer(sport, userid):
  response=dynamoFunctions(playerTable, sport, userid)
  return response

@app.route('/sports/<sport>/users/<userid>', methods=['PUT'])
def updatePlayer(sport, userid):
  if request.json==None:
    abort(400)
  response=dynamoFunctions.updatePlayer(playerTable, request.json)
  if response == None:
    abort(400)
  return(str(response))
