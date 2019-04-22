#coding=utf-8

import requests


api_table = {}


def hades_api(func):
    name = func.__name__
    api_table[name] = func

    return func


class Hades(object):

    def __init__(self, server='127.0.0.1:8102'):
        self.server = server

    def __getattr__(self, attr):

        def call_server(*args, **kwargs):
            params = {
                'name': attr,
                'args': args,
                'kwargs': kwargs
            }

            rs = requests.post('http://%s/api/' % (self.server), json=params)
            if rs.status_code == 200:
                return rs.json()['result']
            elif rs.status_code == 500:
                raise Exception(rs.json()['result'])
            else:
                raise Exception(rs.status_code)

        return call_server

    def __setitem__(self, name, value):
        params = {
            'name': name,
            'py_file': value,
        }

        rs = requests.post('http://%s/module/' % (self.server), json=params)
        if rs.status_code == 200:
            return rs.json()
        elif rs.status_code == 500:
            raise Exception(rs.text)
        else:
            raise Exception(rs.status_code)
