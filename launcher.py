#!/usr/bin/env python

import argparse, time, sys
from pprint import pprint
from threading import Thread
from pathlib import Path
import dispatcher 

import dispatcher_http_agent, replica_controller

def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
         raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def check_directory(ivalue):
    path = Path(ivalue)
    if ((not path.exists()) | (not path.is_dir())):    
         raise argparse.ArgumentTypeError("%s doesn't exist or it isn't a directory" % ivalue)
    return ivalue

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server"  , help="HTTP Server Address (Accessible from all VMs)", required=True)
    parser.add_argument("--port"        , help="HTTP Server port, default = 8777", type=check_positive)
    parser.add_argument("--image"       , help="Client Docker Image ", required=True)
    parser.add_argument("--min"         , help="Min Allowed Replicas, default = 1" , type=check_positive)
    parser.add_argument("--max"         , help="Max Allowed Replicas, default = 10", type=check_positive)
    parser.add_argument("--td"          , help="Single Task Duration (in seconds)", type=check_positive, required=True)
    parser.add_argument("--jdl"         , help="Job (all tasks) deadline (in seconds)", type=check_positive, required=True)
    parser.add_argument("--dir"         , help="Jobs directory, default = ./jobs")
    parser.add_argument("--tc"          , help="Task Count", type=int)
    parser.add_argument("--tasks"       , help="Extended Tasks directory (JSON Files and Tasks' files), it overrides --tc", type=check_directory)
    return parser.parse_args()

if __name__ == "__main__":
    args = getArgs()
    print(args)
    dispatcher_http_agent_instance = dispatcher_http_agent.Dispatcher_HTTP_Agent()

    dispatcher_http_agent_thread = Thread(target = dispatcher_http_agent_instance.start)
    dispatcher_http_agent_thread.start()

    replica_controller_instance = replica_controller.Replica_controller(args, dispatcher_http_agent_instance)

    replica_controller_thread = Thread(target = replica_controller.start)
    replica_controller_thread.start()