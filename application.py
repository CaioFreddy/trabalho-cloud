import json
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import boto3
from flask import Flask, request

application = Flask(__name__)


def post_dynamo(data):
    data.pop('id')
    data['id'] = str(uuid4())
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')
    table.put_item(Item=data)
    return {
        "id": data['id'],
        "message": f"Compra com id {data['id']} cadastrada com sucesso",
        "datetime": datetime.now().isoformat()
    }


def consulta_lista():
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return {
        "datetime": datetime.now().isoformat(),
        "items": json.loads(json.dumps(data, default=str))
    }


@application.route('/')
def health_check():
    return {
        "message": "Health Ok",
        "datetime": datetime.now().isoformat()
    }


@application.route('/incluir', methods=['POST'])
def incluir_compra():
    data = json.loads(request.data, parse_float=Decimal)
    response = post_dynamo(data)
    return response


@application.route('/consultar', methods=['GET'])
def consultar_compras():
    response = consulta_lista()
    return response


if __name__ == '__main__':
    application.run(host='0.0.0.0', debug=True)
