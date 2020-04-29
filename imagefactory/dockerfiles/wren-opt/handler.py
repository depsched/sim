from urllib.parse import urlparse

import cloudpickle as pickle
import sys
import time

from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

"""This server should run in the container in charge of accepting
tasks and run with its python runtime."""

SERVER_PORT = 7777

def make_handler():
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, request, client_address, server):
            SimpleHTTPRequestHandler.__init__(
                self, request, client_address, server)
            data = pickle.dumps({"func": None})
            data = pickle.loads(data)

        def do_GET(self):
            path, input = self.load()
            response = {}
            if path in {"/"}:
                response = self.execute(input)
            self.respond(response)

        def execute(self, data):
            func = data["func"]
            args = data["args"]
            return func(*args)

        def load(self):
            content_len = int(str(self.headers.get("Content-Length", 0)))
            raw_body = self.rfile.read(content_len)
            path = urlparse(self.path).path
            try:
                # start = time.time()
                data = pickle.loads(raw_body)
                # print(time.time() - start, sys.getsizeof(data), data)
                return path, data
            except:
                return path, raw_body

        def respond(self, response, status_code=200):
            self.send_response(status_code)
            self.end_headers()
            self.wfile.write(pickle.dumps(response))
    return Handler


class TaskServer(ThreadingMixIn, HTTPServer):
    def __init__(self, port):
        handler = make_handler()
        HTTPServer.__init__(self, ("127.0.0.1", port), handler)


def main():
    server = TaskServer(SERVER_PORT)
    print("Starting server on port {}".format(SERVER_PORT))
    server.handle_request()
    # server.serve_forever()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit('\nInterrupted\n')
