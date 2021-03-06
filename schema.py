from graphene import ObjectType, Field, String, Float, DateTime, List, Int, Boolean
from collections import namedtuple
import json
from elasticsearch import Elasticsearch
from dateutil.parser import parse
import datetime
import os

elastic_host = os.environ.get('ELASTIC_HOST')
if not elastic_host:
    elastic_host = 'localhost'
elastic_port = os.environ.get('ELASTIC_PORT')
if not elastic_port:
    elastic_port = 9200
es = Elasticsearch([{'host': elastic_host, 'port': elastic_port}])


class CustomGrapheneDateTime(DateTime):
    @staticmethod
    def serialize(date):
        if isinstance(date, str):
            date = parse(date)
        return DateTime.serialize(date)


class Measurement(ObjectType):
    n = String(description='Sensor name')
    u = String(description='Measurement unit e.g., %RH')
    v = Float(description='Integer value')
    vs = String(description='String value')
    vb = Boolean(description='Boolean value')
    ut = Float(description='Update time')
    sum = Float(description='Sum')
    uuid = String(description='Unique measurement ID')
    timestamp = CustomGrapheneDateTime(description='Timestamp for when measurement was received')


class Aggregate(ObjectType):
    avg = Float(description='Average value of selected sensor type')
    unit = String(description='Unit of measurement')


def _json_object_hook(d):
    return namedtuple('X', d.keys())(*d.values())


def _json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)


def _parse_date(date_string):
    try:
        date = datetime.datetime.strptime(date_string, '%Y-%m-%dt%H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect date format, should be yyyy-MM-dd'T'HH:mm:ss")
    return date


class Query(ObjectType):
    measurements = List(
        Measurement,
        description='Retrieve measurements based on name and type.',
        sensor_name=String(description='name of the sensor e.g., urn:dev:mac:fcc23d000000050f'),
        amount=Int(description='Number of measurements to retrieve. 1-100, default is 10.'),
        sensor_type=String(
            description='Choose sensor type from: temp, humidity, pressure, pm1, pm2_5, pm10, no2'
        ),
        from_date=String(description="UTC Timestamp: yyyy-MM-dd'T'HH:mm:ss, e.g. 2019-01-01T10:00:00"),
        to_date=String(description="UTC Timestamp: yyyy-MM-dd'T'HH:mm:ss, e.g. 2019-01-07T10:00:00"),
    )
    avgbydate = Field(
        Aggregate,
        description='Calculate average value of measurement type, e.g., average temp.',
        sensor_type=String(
            description='Choose sensor type from: temp, humidity, pressure, pm1, pm2_5, pm10, no2'
        ),
        from_date=String(description="UTC Timestamp: yyyy-MM-dd'T'HH:mm:ss, e.g. 2019-01-01T10:00:00"),
        to_date=String(description="UTC Timestamp: yyyy-MM-dd'T'HH:mm:ss, e.g. 2019-01-07T10:00:00"),
    )

    def resolve_measurements(
            _self, info, sensor_name=None, amount=10, sensor_type=None, from_date=None, to_date=None, **kwargs
    ):
        allowed_types = ['temp', 'humidity', 'pressure', 'pm1', 'pm2_5', 'pm10', 'no2', 'SoC:temp', 'WiFi:ESSID',
                         'WiFi:ch', 'hostname', 'uptime']
        if sensor_type not in allowed_types:
            sensor_type = None
        if (amount < 0) or (amount > 100):
            amount = 10
        if not from_date:
            # Default to 1 week back in time
            from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
        if not to_date:
            to_date = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        if _parse_date(from_date).date() == _parse_date(to_date).date():  # Same day
            index_name = f"measurements-{from_date.split('T')[0]}"
        else:
            index_name = 'measurements-*'
        if not sensor_name:
            sensor_name = 'urn:dev'

        if sensor_type:
            n_query = f'{sensor_name}*{sensor_type}'
        else:
            n_query = f'{sensor_name}*'
        query = {
            'query': {
                'bool': {
                    'filter': [
                        {'wildcard': {'n.keyword': n_query}},
                        {
                            'range': {
                                'timestamp': {
                                    'from': from_date,
                                    'to': to_date,
                                }
                            }
                        }
                    ]
                }
            },
            'size': amount,
            "sort": [
                {"timestamp": {"order": "asc"}}
            ]
        }
        res = es.search(index=index_name, body=query)['hits']['hits']
        return [_json2obj(json.dumps(x['_source'])) for x in res]

    def resolve_avgbydate(_self, info, sensor_type=None, from_date=None, to_date=None, **kwargs):

        allowed_types = ['temp', 'humidity', 'pressure', 'pm1', 'pm2_5', 'pm10', 'no2']
        if sensor_type not in allowed_types:
            print(f'Not allowed type: {sensor_type}, using temp instead!')  # Change to logging.info
            sensor_type = 'temp'
        if not from_date:
            # Default to 1 week back in time
            from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
        if not to_date:
            to_date = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

        _parse_date(from_date)
        _parse_date(to_date)

        query = {
            'query': {
                'bool': {
                    'filter': [
                        {'wildcard': {'n': sensor_type}},
                        {'range': {'timestamp': {'from': from_date, 'to': to_date}}}
                    ]
                }
            },
            '_source': 'false',
            'aggs': {
                'avg': {'avg': {'field': 'v'}},
                'units': {'terms': {'field': 'u.keyword', 'size': '2'}}
            }
        }
        res = es.search(index='measurements-*', body=query, filter_path='aggregations')
        data = dict()
        data['avg'] = res['aggregations']['avg']['value']
        buckets = len(res['aggregations']['units']['buckets'])
        if buckets > 1:
            raise Exception("Multiple different units in aggregation!")
        elif buckets == 1:
            data['unit'] = res['aggregations']['units']['buckets'][0]['key']
        return _json2obj(json.dumps(data))
