import requests

from hades import hades_api


@hades_api
def get(url, params=None, **kwargs):
    return requests.get(url, params, **kwargs).content.decode('utf-8')


@hades_api
def post(url, data=None, json=None, **kwargs):
    return requests.post(url, data, json, **kwargs).content.decode('utf-8')

