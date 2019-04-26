# python 3.5+
from aiohttp import web, ClientSession

import asyncio
import time
import json
import requests


async def get(*args, **kwargs):
    async with ClientSession() as session:
        async with session.get(*args, **kwargs) as response:
            return await response.text()


async def post(*args, **kwargs):
    async with ClientSession() as session:
        async with session.post(*args, **kwargs) as response:
            return await response.text()

apis = {
    'get': get,
    'post': post,
}


class Hades:

    def __init__(self, ip='0.0.0.0', port='8000', master=None):
        self.ip = ip
        self.port = port
        self.peers = {}
        self.last_called = 0
        self.master = master

    async def heartbeat(self, queue):
        if self.master:
            try:
                await post('http://%s/connect' % self.master, json={ 'ip': '%s:%s' % (self.ip, self.port), 'last_called': self.last_called })
            except:
                print ('master lost.')
            finally:
                await asyncio.sleep(5)
                asyncio.create_task(self.heartbeat(queue))

    async def startup(self, ctx):
        queue = asyncio.Queue()
        asyncio.create_task(self.heartbeat(queue))

    async def shutdown(self, ctx):
        if self.master:
            await post('http://%s/disconnect' % self.master, json={ 'ip': '%s:%s' % (self.ip, self.port) })

    async def api(self, request):
        name = request.match_info.get('name')

        params = await request.json()
        args = params['args']
        kwargs = params['kwargs']

        node = None
        for peer, last_called in self.peers.items():
            if last_called < self.last_called:
                node = peer
                break

        print ('peer select: ', node if node else 'self')
        if not node:
            self.last_called = time.time()
            result = await apis[name](*args, **kwargs)
        else:
            self.peers[node] = time.time()
            result = await post('http://%s/api/%s' % (node, name), json={ 'args': args, 'kwargs': kwargs })

        return web.Response(text=json.dumps(result))

    async def connect(self, request):
        params = await request.json()
        ip = params['ip']
        self.peers[ip] = params['last_called']

        print ('connect - peers', self.peers)

        return web.Response(text='ok')

    async def disconnect(self, request):
        params = await request.json()
        ip = params['ip']
        if ip in self.peers:
            del self.peers[ip]

        print ('disconnect - peers', self.peers)

        return web.Response(text='ok')

    def run(self):
        app = web.Application()
        app.add_routes([
            web.post('/api/{name}', self.api),
            web.post('/connect', self.connect),
            web.post('/disconnect', self.disconnect),
        ])
        app.on_startup.append(self.startup)
        app.on_shutdown.append(self.shutdown)

        web.run_app(app, host=self.ip, port=self.port)

    def __getattr__(self, attr):

        def caller(*args, **kwargs):
            return requests.post('http://%s:%s/api/%s' % (self.ip, self.port, attr), json={ 'args': args, 'kwargs': kwargs}).json()

        return caller

