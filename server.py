# coding=utf-8


from tornado.ioloop import IOLoop

from tornado import gen
from tornado.locks import Condition
from tornado.tcpserver import TCPServer
from tornado.web import RequestHandler, Application
from tornado import template

from functools import partial
from utils import gunzip_string

import json
import random
import random
import uuid
import time
import base64

import settings
from collections import OrderedDict
import logging
import os
import signal
import sys


class Client(object):

    def __init__(self, address, stream):
        self.stream = stream
        self.name = ''
        self.address = address
        self.ready = False
        self.online_time = time.time()
        self.work_list = []
        self.work_done = 0


    def __str__(self):
        return self.name + ':' + self.address.__str__()

    def add_work_history(self, job, stage):
        # j = {k: v for k, v in job.items() if k not in [
        #     'timeout', 'condition', 'client', 'started']}
        # j['history'] = j['history'][stage]
        # self.work_list.append(j)
        self.work_done += 1


class MessageServer(TCPServer):

    def __init__(self):
        self.clients = []
        self.jobs = {}
        self.modules = {}
        self.finished = OrderedDict()

        TCPServer.__init__(self)

    def handle_stream(self, stream, address):
        client = Client(address, stream)
        print ('[Connected]: ', address)

        self.add_client(client)
        stream.set_close_callback(partial(self.remove_client, client))

    def dispatch_jobs(self):

        def invoke_timeout(job):
            print ('[Timeout]', job['job_id'], job['client'].address)

            client = job['client']
            job['client'] = None
            job['timeout'] = None
            job['started'] = 0
            job['history'][-1]['timeout'] = True

            client.add_work_history(job, -1)

            self.dispatch_jobs()

        for job in [_ for _ in self.jobs.values() if not _['client']]:

            ready_clients = [_ for _ in self.clients if _.ready]

            if not ready_clients:
                continue

            client = random.choice(ready_clients)
            client.ready = False

            job['client'] = client
            job['timeout'] = IOLoop.current().call_later(
                settings.JOB_TIMEOUT, invoke_timeout, job)
            job['started'] = time.time()
            job['history'].append({
                'client_address': client.__str__(),
                'start_time': time.time(),
                'end_time': None,
                'timeout': False,
            })

            self.send(client, {
                'type': 'invoke',
                'job_id': job['job_id'],
                'params': job['params'],
                'stage': len(job['history']) - 1,
            })

    @gen.coroutine
    def recv(self, client, message):
        msg_type = message['type']

        if msg_type == 'result':
            job_id = message['job_id']
            client.ready = True

            if job_id in self.jobs:
                job = self.jobs[job_id]

                stage = message['stage']
                job['history'][stage]['end_time'] = time.time()

                timeout = job['timeout']

                if client == job['client']:
                    IOLoop.current().remove_timeout(timeout)
                    job['result'] = message
                    job['timeout'] = None
                    job['condition'].notify()
                    job['finish_time'] = time.time()
                else:
                    print ('[Throw]', job['job_id'], client.address)

            client.add_work_history(job, stage)
            self.dispatch_jobs()

        elif msg_type == 'ready':

            identity = message['identity']

            client.ready = True
            client.name = identity

            for module in self.modules:
                result = yield self.module(module, self.modules[module])

            self.dispatch_jobs()

    def send(self, client, message):
        print (client.address, '<==', json.dumps(message, indent=2))
        text = json.dumps(message) + settings.MESSAGE_SEPARATOR

        client.stream.write(text.encode('utf-8'))

    def add_client(self, client):
        self.clients.append(client)

        def client_read(future):
            data = future.result()

            text = data[:-len(settings.MESSAGE_SEPARATOR)]
            msg_text = gunzip_string(text)
            print (client.address, '==>', '(%d/%d)' % (len(text), len(msg_text)))

            message = json.loads(msg_text)
            print (message)

            self.recv(client, message)
            client.stream.read_until(settings.MESSAGE_SEPARATOR.encode('utf-8'), 100000).add_done_callback(client_read)

        client.stream.read_until(settings.MESSAGE_SEPARATOR.encode('utf-8'), 100000).add_done_callback(client_read)
        self.send(client, {'type': 'status'})

    def remove_client(self, client):
        self.clients.remove(client)
        to_log = {
            'address': client.address,
            'online_time': time.ctime(int(client.online_time)),
            'offline_time': time.ctime(int(time.time())),
            'finished_works_num': client.work_done,
            'work_history': client.work_list,
        }
        logging.info('==%s Disconnected==\n%s', client.name, to_log)
        print ('[Disconnected]: ', client.address)

    @gen.coroutine
    def invoke(self, params):
        job_id = uuid.uuid4().hex
        condition = Condition()
        self.jobs[job_id] = {
            'job_id': job_id,
            'params': params,
            'result': None,
            'condition': condition,
            'client': None,
            'timeout': None,
            'started': 0,
            'history': [],
            'recv_time': time.time(),
            'finish_time': None
        }

        self.dispatch_jobs()
        yield condition.wait()

        result = self.jobs[job_id]['result']
        del self.jobs[job_id]
        # self.finished[job_id] = self.jobs.pop(job_id)
        # self.finished[job_id]['client'] = self.finished[
        #     job_id]['client'].address

        # if len(self.finished) > settings.MAX_LENGTH_HISTORY_IN_MEMO:
        #     last = self.finished.popitem(last=False)
        #     to_log = {last[0]:  {k: v for k, v in last[
        #         1].items() if k not in ['timeout', 'condition']}}
        #     logging.info('===Finished Job===\n%s', to_log)
        #     assert len(self.finished) <= settings.MAX_LENGTH_HISTORY_IN_MEMO

        raise gen.Return(result)

    @gen.coroutine
    def module(self, name, py_file):
        self.modules[name] = py_file

        for client in self.clients:
            self.send(client, {
                'type': 'module',
                'name': name,
                'file': base64.b64encode(py_file.encode('utf-8')).decode('utf-8'),
            })

        raise gen.Return({ 'status': 'ok' })

    @gen.coroutine
    def print_status(self):
        while True:
            print ('routine check')
            print ('current jobs')
            for k, v in self.jobs.items():
                print (v['job_id'])
                print (v['history'])
            print ('finished jobs')
            for k, v in self.finished.items():
                print (v['job_id'])
                print (v['history'])

            yield gen.sleep(30)


class ApiHandler(RequestHandler):

    def initialize(self, server):
        self.server = server

    def get(self):
        res = json.dumps({
            'clients': [{
                'name': _.name,
                'address': _.address,
                'ready': _.ready,
                'online_time': _.online_time,
                'work_done': _.work_done,
            } for _ in self.server.clients],
            'jobs': self.server.jobs,
        })

        self.write(res)

    @gen.coroutine
    def post(self):
        params = json.loads(self.request.body.decode('utf-8'))
        result = yield self.server.invoke(params)

        self.set_status(200)
        self.write(json.dumps(result))


class ModuleHandler(RequestHandler):

    def initialize(self, server):
        self.server = server

    def get(self):
        res = json.dumps({
            'modules': self.server.modules,
        })

        self.write(res)

    @gen.coroutine
    def post(self):
        params = json.loads(self.request.body.decode('utf-8'))
        result = yield self.server.module(**params)

        self.set_status(200)
        self.write(json.dumps(result))


def config_logging():
    path = settings.LOGPATH
    if not os.path.exists(path):
        os.mkdir(path)
    stamp = '[%s].log' % time.ctime(int(time.time()))
    path = os.path.join(path, stamp)
    logging.basicConfig(level=logging.DEBUG, filename=path,
                        format='%(levelname)s:%(asctime)s~ %(message)s')


def sig_handler(sig, frame):
    print ('Caught signal: %s'%sig)
    IOLoop.current().add_callback(shutdown)


def shutdown():
    logging.info('Stopping http server')
    IOLoop.current().stop()

    # global server
    # server.stop()

    # print 'Will shutdown in %s seconds ...'% settings.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
    # io_loop = IOLoop.current()

    # deadline = time.time() + settings.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

    # def stop_loop():
    #     now = time.time()
    #     if now < deadline and (io_loop._callbacks or io_loop._timeouts):
    #         io_loop.add_timeout(now + 1, stop_loop)
    #     else:
    #         logging.info('----------------Saving Finished Jobs-----------')
    #         for _ in reversed(server.finished):
    #             to_log = { _: {k: v for k,v in server.finished[_].items() if k not in ['timeout', 'condition']}}
    #             logging.info('===Finished Job===\n%s', to_log)

    #         logging.info('----------------Saving Ongoing Jobs-----------')
    #         for _ in server.jobs:
    #             to_log = {_:  {k: v for k, v in server.jobs[_].items() if k not in ['timeout', 'condition']}}
    #             logging.info('===Ongoing Job===\n%s', to_log)

    #         logging.info('----------------Saving Online Workers----------')
    #         print 'Putting Workers to sleep'
    #         for c in server.clients:
    #             to_log = {
    #                 'address':c.address,
    #                 'online_time': time.ctime(int(c.online_time)),
    #                 'offline_time': time.ctime(int(time.time())),
    #                 'finished_works_num': len(c.work_list),
    #                 'work_history' : c.work_list,
    #                 }
    #             logging.info('==%s Disconnected==\n%s', client.name, to_log)
    #             server.send(c,{'type':'sleep'})

    #         io_loop.stop()
    #         print 'Shutdown'
    # stop_loop()


if __name__ == '__main__':

    # sys.path.append("/root/hades/hades/src")
    # sys.path.append("/Users/mac/Desktop/hades/src")

    server = MessageServer()
    server.listen(8101)

    config_logging()

    app = Application([
        (r"/api/", ApiHandler, {'server': server}),
        (r"/module/", ModuleHandler, {'server': server}),
    ], debug = True)
    app.listen(8102)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)


    IOLoop.current().start()
