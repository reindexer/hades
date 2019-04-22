#coding=utf-8

from tornado import gen
from tornado.locks import Event
from tornado.tcpclient import TCPClient
from tornado.ioloop  import IOLoop

from utils import gzip_string
from importlib import import_module

import json
import sys
import traceback
import base64
import os

import hades
import settings
import argparse


disconnected_event = Event()
disconnected_event.set()

@gen.coroutine
def connect(server, port, identity):
    client = TCPClient()
    stream = yield client.connect(server, port)

    print ('[Connected]: ', server, ':', port)
    disconnected_event.clear()

    def disconnected():
        print ('[Disconnected]')
        disconnected_event.set()


    def client_read(future):
        data = future.result()
        try:
            message = json.loads(data.decode('utf-8')[:-len(settings.MESSAGE_SEPARATOR)])
            recv(message)
        except Exception as ex:
            print ('[Error]: ', ex)
        finally:
            stream.read_until(settings.MESSAGE_SEPARATOR.encode('utf-8'), 100000).add_done_callback(client_read)


    def send(message):
        msg_text = json.dumps(message)
        text = gzip_string(msg_text)
        stream.write(text)
        stream.write(settings.MESSAGE_SEPARATOR.encode('utf-8'))
        print ('[Send]: (%d / %d)' % (len(text), len(msg_text)))


    def invoke(job_id, params, stage):
        try:
            name = params['name']
            args = params['args']
            kwargs = params['kwargs']

            code = 0
            result = hades.api_table[name](*args, **kwargs)
        except Exception as ex:
            traceback.print_exc()
            code = 1
            result = {
                'exception': str(ex),
                'stack': '',
            }
        send({'code': code, 'type': 'result', 'job_id': job_id, 'result': result, 'stage':stage})


    def recv(message):
        print ('[Receive]: ', json.dumps(message, indent=2))

        msg_type = message['type']

        if msg_type == 'invoke':
            job_id = message['job_id']
            params = message['params']
            stage = message['stage']
            invoke(job_id, params, stage)
            
        elif msg_type == 'module':
            py_file = base64.b64decode(message['file'].encode('utf-8'))
            module_name = message['name']
            if not os.path.exists('src'):
                os.mkdir('src')

            with open('src/%s.py' % module_name, 'wb') as f:
                f.write(py_file)

            import_module('src.%s' % module_name)
            send({
                'type': 'module',
                'status': 'loaded'
            })

        elif msg_type == 'status':
            msg = {
                'type': 'ready',
                'identity': identity,
            }
            send(msg)
        elif msg_type == 'rejected':
            print ('[Connection Rejected] Shutting down')
            client.close()
            IOLoop.current().stop()

        elif msg_type == 'sleep':
            global RECONNECT_TIME
            print ('Sleep... Good Night')
            RECONNECT_TIME=180

    stream.set_close_callback(disconnected)
    stream.read_until(settings.MESSAGE_SEPARATOR.encode('utf-8'), 100000).add_done_callback(client_read)


@gen.coroutine
def try_connect(server, port, identity):
    while True:
        if disconnected_event.is_set():
            print ('[Connecting]: ...')
            connect(server, port, identity)

        global RECONNECT_TIME
        yield gen.sleep(RECONNECT_TIME)


if __name__ == '__main__':    
    parser= argparse.ArgumentParser()
    parser.add_argument('-ip', type=str, default='127.0.0.1')
    parser.add_argument('-id', type=str, default='')
    parser.add_argument('-port', type=int, default=8101)
    #server = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1' #test server: 139.196.22.122
    args = parser.parse_args()

    __import__('src.requests')

    RECONNECT_TIME=3
    try_connect(args.ip, args.port, args.id)
    IOLoop.current().start()




