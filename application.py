import datetime as dt
import json
import os
import time
from decimal import Decimal
from uuid import uuid4

import boto3
from flask import Flask, request

application = Flask(__name__)
os.environ['TZ'] = 'America/Sao_Paulo'
time.tzset()


def post_dynamo(data):
    data.pop('id')
    data['id'] = str(uuid4())
    data['dataCompra'] = dt.datetime.now().strftime("%d/%m/%Y")
    data['horaCompra'] = dt.datetime.now().strftime("%H:%M:%S")
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')
    table.put_item(Item=data)
    return {
        "id": data['id'],
        "message": f"Compra com id {data['id']} cadastrada com sucesso",
        "datetime": dt.datetime.now().isoformat()
    }


def consulta_lista(args):
    rsc = boto3.resource('dynamodb', region_name='us-east-1')
    table = rsc.Table('lista_supermercado')

    if not args:
        args = {}

    if args:
        items = table.scan(
            FilterExpression=f' and '.join([f'{k} = :{k}'
                                            for k in args.keys()]),
            ExpressionAttributeValues={
                f':{k}': v for k, v in args.items()
            }
        ).get('Items')
    else:
        items = table.scan()['Items']

    return {
        "datetime": dt.datetime.now().isoformat(),
        "items": json.loads(json.dumps(items, default=str))
    }


@application.route('/')
def health_check():
    return {
        "message": "Health Ok",
        "datetime": dt.datetime.now().isoformat()
    }


@application.route('/incluir', methods=['POST'])
def incluir_compra():
    data = json.loads(request.data, parse_float=Decimal)
    response = post_dynamo(data)
    return response


@application.route('/consultar', methods=['GET'])
def consultar_compras():
    args = dict(request.args)
    response = consulta_lista(args)
    return response


if __name__ == '__main__':
    application.run(host='0.0.0.0', debug=False)
