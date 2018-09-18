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

@app.route('/sport/<sport>', methods=['DELETE'])
def deleteSport(sport):
  response=dynamoFunctions.deleteSport(sportTable, {'sport': sport})
  if response==None:
    abort(404)    #ToDo, print a custom error for this HTTP code.
  return str(response)


