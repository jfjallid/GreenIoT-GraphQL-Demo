from graphene import ObjectType, Field, String, Float, DateTime, List, Int
from collections import namedtuple
import json
from elasticsearch import Elasticsearch
from dateutil.parser import parse
import datetime

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


class CustomGrapheneDateTime(DateTime):
    @staticmethod
    def serialize(date):
        if isinstance(date, str):
            date = parse(date)
        return DateTime.serialize(date)


class Measurement(ObjectType):
    u = String()
    v = Float()
    n = String()
    uuid = String()
    timestamp = CustomGrapheneDateTime()


class Aggregate(ObjectType):
    avg = Float(description='Average value of selected sensor type')
    unit = String(description='Unit of measurement')


def _json_object_hook(d):
    return namedtuple('X', d.keys())(*d.values())


def json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)


class Query(ObjectType):
    measurements = List(
        Measurement,
        sensor_name=String(description='name of the sensor e.g., urn:dev:mac:fcc23d000000050f'),
        amount=Int(),
        sensor_type=String(
            description='Choose sensor type from: temp, humidity, pressure, pm1, pm2_5, pm10, no2'
        ),
    )
    avgbydate = Field(
        Aggregate,
        sensor_type=String(
            description='Choose sensor type from: temp, humidity, pressure, pm1, pm2_5, pm10, no2'
        ),
        from_date=String(description="yyyy-MM-dd'T'hh:mm:ss, e.g. 2019-01-01T10:00:00"),
        to_date=String(description="yyyy-MM-dd'T'hh:mm:ss, e.g. 2019-01-07T10:00:00"),
    )

    @staticmethod
    def resolve_measurements(
            info, sensor_name=None, amount=10, sensor_type=None, **kwargs
    ):
        allowed_types = ['temp', 'humidity', 'pressure', 'pm1', 'pm2_5', 'pm10', 'no2']

        if sensor_type not in allowed_types:
            sensor_type = None

        if (amount < 0) or (amount > 100):
            amount = 10

        if sensor_name:
            if sensor_type:
                sensor_name = sensor_name + '*' + sensor_type
            else:
                sensor_name = sensor_name + '*'
            query = {
                'query': {
                    'bool': {
                        'filter': [
                            {'wildcard': {'n.keyword': sensor_name}},
                            {
                                'range': {
                                    'timestamp': {
                                        'from': '2019-03-28T10:00:00.000',
                                        'to': '2019-03-28T10:10:00.000',
                                    }
                                }
                            },
                        ]
                    }
                },
                'size': amount,
            }
            res = es.search(
                index='measurements-*', body=query, filter_path='hits.hits._source'
            )['hits']['hits']
        else:
            res = es.search(index='measurements-*', filter_path='hits.hits._source')['hits'][
                'hits'
            ]
        measurements = []

        for x in res:
            measurements.append(json2obj(json.dumps(x['_source'])))
        return measurements

    @staticmethod
    def resolve_avgbydate(info, sensor_type=None, from_date=None, to_date=None, **kwargs):

        allowed_types = ['temp', 'humidity', 'pressure', 'pm1', 'pm2_5', 'pm10', 'no2']

        if not from_date:
            # Default to 1 week back in time
            from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
        if not to_date:
            to_date = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        if sensor_type not in allowed_types:
            print(f'Not allowed type: {sensor_type}, using temp instead!')
            sensor_type = 'temp'

        query = {
            'query': {
                'bool': {
                    'filter': [
                        {'wildcard': {'n': sensor_type}},
                        {'range': {'timestamp': {'from': from_date, 'to': to_date}}},
                    ]
                }
            },
            '_source': 'false',
            'aggs': {
                'avg': {'avg': {'field': 'v'}},
                'units': {'terms': {'field': 'u.keyword', 'size': '1'}},
            },
        }
        res = es.search(index='measurements-*', body=query, filter_path='aggregations')
        data = dict()
        data['avg'] = res['aggregations']['avg']['value']
        data['unit'] = res['aggregations']['units']['buckets'][0]['key']
        return json2obj(json.dumps(data))
