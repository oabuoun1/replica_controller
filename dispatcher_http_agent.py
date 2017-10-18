import http.client, urllib.parse, urllib.request, time, json, argparse, sys, random, datetime
from pprint import pprint
from threading import Lock

class Dispatcher_HTTP_Agent:
    self.lock = Lock()

    def __init__(self, args):
        self.server = args.server if (args.server != None) else "127.0.0.1" 
        self.port = args.port if (args.port != None) else 8777
        self.url =  self.server + ":" + str(self.port)
        self.conn = None
        self.finished = False
        self.conn_status = 0 # disconnected

    def parse_response(self, data):
        try:
            data_as_json = json.loads(str(data, "utf-8"))
            print("data_as_json : " + str(data_as_json))
            task_id = data_as_json["task_id"]
            print("TaskID:" + task_id)
            task_data = data_as_json["data"]
            print("task_data : " + str(task_data))
            json_object = json.loads(task_data)
            print("I got a new task:")
            print(json_object)
            print("-----------------------------------------------------")
            return True, json_object
            #do the task

        except ValueError:
            task_data_str = str(data, "utf-8")
            print("It is a direct command :" + task_data_str  + ":")
            print("-----------------------------------------------------")
            return False, task_data_str


    def http_get(self, url, request_data = {}):
        #request_data = {'replica_id' : replica_id}
        self.lock.acquire()
        while True:
            try:
                conn.request("GET",url, urllib.parse.urlencode(request_data))
                response = self.conn.getresponse()
                print(response.status, response.reason)
                data = response.read()  # This will return entire content.
                self.lock.release() #release lock
                print("data:" + str(data))
                return parse_response(data)
            except:
                self.lock.release() #release lock
                self.connect()

    def add_task(self, task):
        return 

    def get_task_count(self):
        return http_get("/query/tc")

    def get_undispatched_count(self):
        return http_get("/query/utc")

    def get_dispatched_count(self):
        return http_get("/query/dtc")

    def get_finished_count(self):
        return http_get("/query/ftc")

    def disconnected(self):
        try:
            self.conn.disconnect()
        except:
            print("Problem in disconnecting from server, exiting anyway")

    def connect(self):
        print("Trying to connect to " + self.url)
        index = 0
        while True:
            try:
                self.conn = http.client.HTTPConnection(url)
                self.conn.connect()
                print(self.conn)
                self.conn_status = 1 # Connected
                break
            except:
                print("Try " + str(index) + ": Connection failed to the server's URL " + url)
                index += 1
                self.conn_status = 0 # disconnected
                time.sleep(5)
        print("Connected to " + url + " successfully !")