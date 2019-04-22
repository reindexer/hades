
import gzip
import time


class Timer(object):

    def __init__(self, text='%.2fms'):
        self._text = text


    def __enter__(self):
        self._start = time.time()


    def __exit__(self, type, value, trace):
        end = time.time()
        print (self._text % (end - self._start))



def gzip_string(text):
    gzip_string = gzip.compress(text.encode('utf-8'))
    return gzip_string


def gunzip_string(zip_data):
    return gzip.decompress(zip_data).decode('utf-8')

