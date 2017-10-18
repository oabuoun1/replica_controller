#!/usr/bin/env python
import math, time, datetime, random, shutil, threading, subprocess, docker, sys
from threading import Lock, Thread
from docker import types
from pprint import pprint
from pathlib import Path
import argparse, time, sys

class Replica_Manager:
    SERVICE_NAME = ""
    SERVICE = None
    lock = Lock()
    replicas = {}
    index = 0

    def __init__(self, args, dispatcher_http_agent):
        self.dispatcher_http_agent = dispatcher_http_agent
        self.IMAGE = args.image
        self.SERVER = args.server
        self.PORT = args.port
        self.JOB_DEADLINE = 0
        self.TASK_DURATION = args.td
        try:
            self.MIN_REPLICAS_ALLOWED = args.min
        except:
            self.MIN_REPLICAS_ALLOWED = 1
        try:
            self.MAX_REPLICAS_ALLOWED = args.max
        except:
            self.MAX_REPLICAS_ALLOWED = 10
        self.SIMULTANEOUS_REPLICAS_NEEDED = 0
        self.JOB_DEADLINE_SET(args.jdl)
        self.STARTING_TIME = self.TIME_NOW_GET()

        for ch in self.IMAGE:
            self.SERVICE_NAME += "_" if ch == "/" else ch
        self.SERVICE_NAME += "_"
        self.SERVICE_NAME += str(random.randrange(1000,9999))
        self.start_service(self.SIMULTANEOUS_REPLICAS_NEEDED)
        #subprocess.run(["docker", "service", "create" , "--name", self.SERVICE_NAME, image, "--server", server, "--port", str(port)], stdout=subprocess.PIPE)

    def JOB_DEADLINE_SET(self, jdl):
        assert isinstance(jdl, int)
        self.JOB_DEADLINE = jdl
        self.SIMULTANEOUS_REPLICAS_NEEDED = math.ceil((self.dispatcher_http_agent.get_task_count() * self.TASK_DURATION) / self.JOB_DEADLINE)
        print(" /////////////// SRN = " + str(self.SIMULTANEOUS_REPLICAS_NEEDED) + " ////////////////////")
        if (self.SIMULTANEOUS_REPLICAS_NEEDED < self.MIN_REPLICAS_ALLOWED):
            self.SIMULTANEOUS_REPLICAS_NEEDED = self.MIN_REPLICAS_ALLOWED
        if (self.SIMULTANEOUS_REPLICAS_NEEDED > self.MAX_REPLICAS_ALLOWED):
            self.SIMULTANEOUS_REPLICAS_NEEDED = self.MAX_REPLICAS_ALLOWED

    def TIME_NOW_GET(self):
        return round(time.time())

    def STARTING_TIME_GET(self):
        return self.STARTING_TIME

    def SIMULTANEOUS_REPLICAS_NEEDED_INCREASE(self, count = 1):
        if ((self.SIMULTANEOUS_REPLICAS_NEEDED + count) <= self.MAX_REPLICAS_ALLOWED):
            self.SIMULTANEOUS_REPLICAS_NEEDED += count
        else:
            self.SIMULTANEOUS_REPLICAS_NEEDED = self.MAX_REPLICAS_ALLOWED

    def SIMULTANEOUS_REPLICAS_NEEDED_DECREASE(self, count = 1):
        if ((self.SIMULTANEOUS_REPLICAS_NEEDED - count) > self.MIN_REPLICAS_ALLOWED):
            self.SIMULTANEOUS_REPLICAS_NEEDED -= count
        else:
            self.SIMULTANEOUS_REPLICAS_NEEDED = self.MIN_REPLICAS_ALLOWED

    def ELAPSED_TIME(self):
        return self.TIME_NOW_GET() - self.STARTING_TIME_GET()

    def REMAINING_TIME(self):
        return self.JOB_DEADLINE - self.ELAPSED_TIME()

    def start_service(self, SRN):
        print("--------------------------------------------------------------")
        print("Starting the service ")
        self.docker_client = docker.from_env()
        env = ["SERVER=" + self.SERVER, "PORT=" + str(self.PORT)]
        mode = types.ServiceMode(mode='replicated', replicas= SRN)
        self.SERVICE = self.docker_client.services.create(self.IMAGE, command=None, env= env, mode= mode, name=self.SERVICE_NAME)

    def stop_service(self):
        self.SERVICE.remove()

    def __str__(self):
        text  = "JDL:" + str(self.JOB_DEADLINE) + " | "
        text += "TD :" + str(self.TASK_DURATION) + " | "
        text += "JDL " + str(self.JOB_DEADLINE) + " | "
        text += "SRN " + str(self.SIMULTANEOUS_REPLICAS_NEEDED) + " | "
        text += "MIN " + str(self.MIN_REPLICAS_ALLOWED) + " | "
        text += "MAX " + str(self.MAX_REPLICAS_ALLOWED) + " | "
        text += "ST  " + str(self.STARTING_TIME_GET()) + " | "
        text += "RT  " + str(self.REMAINING_TIME()) + "\n"
        text += str(self.dispatcher_http_agent)
        now = time.time()
        text += "Replicas : " + str([{key, self.replicas[key]["last_still_alive_at"], now - self.replicas[key]["last_still_alive_at"]} for key in self.replicas.keys()]) 
        return text

    def register(self, client_address):
        self.lock.acquire()
        replica_id = -1
        try:
            # only one thread can execute code there
            replica_id = "REPLICA_" + str(self.index)
            self.index += 1
            now = time.time()
            replica = {'ip': client_address[0], 'port': client_address[1], 'registered_at':now, 'last_still_alive_at': now}
            self.replicas[replica_id] = replica
        finally:
            self.lock.release() #release lock
        #print(self.replicas)
        return replica_id

    def still_alive(self,replica_id, data):
        # only one thread can execute code there
        now = time.time()
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        print("Replica before : " + str (replica_id) + " = " + str(self.replicas[replica_id]))
        self.replicas[replica_id]['last_still_alive_at'] = now
        print("Now = " + str(now))
        print("Replica after : " + str (replica_id) + " = " + str(self.replicas[replica_id]))
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        print(self.replicas)
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        return 

    def get_replicas(self):
        return self.replicas

    def is_replica_alive(self, replica_detail):
        now = time.time()
        if (now - float(replica_detail['last_still_alive_at']) > 12):
            return False
        else:
            return True

    def replica_count(self):
        count = 0
        for replica_id, replica_detail in self.replicas.items():
            if self.is_replica_alive(replica_detail):
                count += 1
        return count

    def get_service_data(self):
        return self.docker_client.services.get(self.SERVICE_NAME)

    def docker_replica_count(self):
        return int(self.get_service_data().attrs['Spec']['Mode']['Replicated']['Replicas'])

    def add_replicas(self, count):
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print(self.SERVICE.attrs['Spec']['Mode'])
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        subprocess.run(["docker", "service", "scale" , self.SERVICE_NAME + "=" + str(self.replica_count() + count)], stdout=subprocess.PIPE)

    def start(self):
        while True:
            if (self.dispatcher_http_agent.get_undispatched_count() > 0):
                print("UTC > 0")
                if (self.replica_count() < self.SIMULTANEOUS_REPLICAS_NEEDED):
                    print("RC < SRN")
                    print("Adding a replica")
                    self.add_replicas(1)
                    # RC++
                    pass
                else:
                    print("RC >= SRN")
                    if (self.SIMULTANEOUS_REPLICAS_NEEDED > self.dispatcher_http_agent.get_dispatched_count()):
                        pass
                    else:                    
                        if((self.dispatcher_http_agent.get_undispatched_count() * self.TASK_DURATION) > self.REMAINING_TIME()):
                            print("UTC*TD > RT")
                            if (self.SIMULTANEOUS_REPLICAS_NEEDED < self.MAX_REPLICAS_ALLOWED):
                                print("SRN < MAX")
                                self.SIMULTANEOUS_REPLICAS_NEEDED_INCREASE()
                                continue
                            else:
                                print("SRN >= MAX")
                                pass
                        else:
                            print("UTC*TD <= RT")
                            if (self.SIMULTANEOUS_REPLICAS_NEEDED >= self.MAX_REPLICAS_ALLOWED):
                                print("SRN >= MAX")
                                self.SIMULTANEOUS_REPLICAS_NEEDED_DECREASE()
                            else:
                                pass
            else:
                print("UTC <= 0")
                if (self.dispatcher_http_agent.get_finished_count() == self.dispatcher_http_agent.get_task_count()):
                    print("TC == FTC")
                    break 
                else:
                    print("TC != FTC")
                    pass
            print(self)
            time.sleep(3)

        print("Job Accomplished")
        self.stop_service()
        sys.exit()
