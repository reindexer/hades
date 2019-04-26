from hades import Hades

import sys


if __name__ == '__main__':
    server = Hades(*sys.argv[1:])
    server.run()