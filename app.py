from flask import Flask, request
from uuid import uuid4
import boto3
import json
from decimal import Decimal

app = Flask(__name__)


def post_dynamo(data):
    data.pop('id')
    data['id'] = str(uuid4())
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')
    table.put_item(Item=data)
    return f"Compra com id {data['id']} cadastrada com sucesso!"


def consulta_lista():
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return json.dumps(data, default=str)


def consulta_item(id):
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')
    data = table.scan()
    return json.dumps(data, default=str)


@app.route('/health')
def health_check():
    return 'Ok', 200


@app.route('/incluir', methods=['POST'])
def incluir_compra():
    try:
        data = json.loads(request.data, parse_float=Decimal)
        response = post_dynamo(data)
        return response, 200
    except Exception as err:
        return err.args[-1], 400


@app.route('/consultar', methods=['GET'])
def consultar_compras():
    try:
        data = json.loads(request.data, parse_float=Decimal)
        response = post_dynamo(data)
        return response, 200
    except Exception as err:
        return err.args[-1], 400


if __name__ == '__main__':
    app.run()
