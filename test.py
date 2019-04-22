from hades import Hades

hd = Hades()

hd['test'] = """
from hades import hades_api


@hades_api
def ping():
    return 'pong'

"""

print (hd.ping())