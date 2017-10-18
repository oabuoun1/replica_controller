import math, time, datetime, random, shutil, json, ast
from pathlib import Path
#from threading import Lock

class Task_Manager:
    #lock_dispatched_tasks = Lock()
    #lock_undispatched_tasks = Lock()

    def __init__(self, task_duration, tasks, task_count=0):
        self.TASK_DURATION = task_duration
        self.MIN_REAL_TASK_DURATION = 0
        self.MAX_REAL_TASK_DURATION = 0
        self.AVG_REAL_TASK_DURATION = 0

        self.TASKS = []
        print(tasks)
        print(task_count)
        self.init_directory()
        if (tasks == None):
            self.process_list(task_count)
        else:
            self.process_tasks(tasks)

    def __str__(self):
        text = "\n"
        text += "================================================================" + "\n"
        text += "UTC :" + str(self.get_undispatched_count()) + " | "
        tasks = [{task['id']} for task in self.get_undispatched_tasks()]
        text += "Undispatched : " + str(tasks) + "\n"
        text += "---------------------------------------------------------" + "\n"
        text += "DTC :" + str(self.get_dispatched_count()) + " | "
        tasks = [{task['id'],time.time() - task['dispached_at'], str(task['replica_id'])} for task in self.get_dispatched_tasks()]
        text += "Dispatched : " + str(tasks) + "\n"       
        text += "---------------------------------------------------------" + "\n"
        text += "FTC :" + str(self.get_finished_count()) + " | "
        tasks = [{task['id'],time.time() - task['finished_at']} for task in self.get_finished_tasks()]
        text += "Finished : " + str(tasks) + "\n"
        text += "================================================================" + "\n"
        #text += str(self.TASKS)
        return text

    def init_directory(self, dir = "./jobs"):
        path = Path(dir)
        if (path.exists()):
            print("Path = " + str(path))
        else:
            print("Path doesn't exist")
            path.mkdir()

        dir += "/" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(random.randrange(1000,9999)) 
        self.JOB_DIR = Path(dir)
        self.JOB_DIR.mkdir()
        input_dir = dir + "/" + "input" 
        self.INPUT_DIR = Path(input_dir)
        self.INPUT_DIR.mkdir()
        self.OUTPUT_DIR = Path(dir + "/" + "output")
        self.OUTPUT_DIR.mkdir()

    def process_tasks(self, tasks):
        path = Path(tasks)
        if (path.exists()):
            for x in sorted(list(path.glob('*.json'))):
                if not x.is_dir():
                    file_name = str(x.resolve().stem)
                    print("File_name : " + file_name)
                    task = {'id': file_name, \
                        'status': 'undispatched', \
                        'replica_id': None, \
                        'dispached_at':None, \
                        'finished_at':None, \
                        'results': [], \
                    }
                    shutil.copy(str(x),str(self.INPUT_DIR))
                    task['json'] = str(self.INPUT_DIR) + "/" + file_name + ".json"
                    directory_name = str(x.parent) + "/" + file_name + "_files"
                    directory = Path(directory_name)
                    if (directory.exists()):
                        task['data'] = directory_name
                    self.TASKS.append(task)
            print(self.TASKS)
            print(len(self.TASKS))

        else:
            print("Tasks Directory doesn't exist")

    def process_list(self, task_count):
        for x in range(0,task_count):
            task = {"task_" + str(x) : { \
                'status': 'undispatched', \
                'replica_id': None, \
                'dispached_at':None, \
                'finished_at':None, \
                'json': None, \
                'data': None, \
                'results': [], \
            }}
            self.TASKS.append(task)
        print(self.TASKS)
        print(len(self.TASKS))

    def count(self, key, value):
        return len([i for i in self.TASKS if i[key] == value])

    def findAll(self, key, value):
        return [i for i in self.TASKS if i[key] == value]

    def findFirst(self, key, value):
        for i, dic in enumerate(self.TASKS):
            if dic[key] == value:
                return self.TASKS[i]
        return None
    def get_delayed_dispatched_task(self, data):
        #self.lock_dispatched_tasks.acquire()
        task_to_return = None
        try:        
            tasks = self.findAll("status", "dispatched")
            for task in tasks:
                elapsed_time = time.time() - task["dispached_at"]
                if (elapsed_time > self.MAX_REAL_TASK_DURATION):
                    task["status"] = "dispatched"
                    task["replica_id"] = data['replica_id']
                    task["dispached_at"] = time.time()
                    task_to_return = task
                    break
        finally:
            #self.lock_dispatched_tasks.release() #release lock
            pass
        return task_to_return

    def get_undispatched_task(self, data):
        #self.lock_undispatched_tasks.acquire()
        task_to_return = None
        try:
            task = self.findFirst("status", "undispatched")
            if (task != None):
                task["status"] = "dispatched"
                task["replica_id"] = data['replica_id']
                task["dispached_at"] = time.time()
                task_to_return = task
        finally:
            #self.lock_undispatched_tasks.release() #release lock
            pass
        return task_to_return

    def get_task_count(self):
        return len(self.TASKS)

    def get_undispatched_tasks(self):
        return self.findAll("status", "undispatched")

    def get_undispatched_count(self):
        return self.count("status", "undispatched")

    def get_dispatched_tasks(self):
        return self.findAll("status", "dispatched")

    def get_dispatched_count(self):
        return self.count("status", "dispatched")

    def get_finished_tasks(self):
        return self.findAll("status", "finished")

    def get_finished_count(self):
        return self.count("status", "finished")

    def save_result(self,replica_id, data):
        #print("data : " + str(type(data)) + " - " + data)
        #json_object = json.loads(data["result"])
        data_as_json = ast.literal_eval(data)
        result_id = data_as_json["result_id"]
        task_id   = data_as_json["task_id"]
        file_name = str(self.OUTPUT_DIR) + "/" + task_id + "_" + replica_id + "_" + result_id  
        file_name += ".json"

        file = open(file_name, 'a')
        file.write(str(data_as_json["result"]))
        file.close()
        task = self.findFirst("id", task_id)
        task['results'].append(file_name)

    def task_finished(self,replica_id, data):
        #print("data : " + str(type(data)) + " - " + data)
        data_as_json = ast.literal_eval(data)
        task = self.findFirst("id",data_as_json["task_id"])
        #print(task)
        task["status"] = "finished"
        task['finished_at'] = data_as_json["finished_at"]
        self.update_durations(float(task['finished_at']) - task['dispached_at'])
        return 

    def update_durations(self, duration):
        if (self.MIN_REAL_TASK_DURATION > duration):
            self.MIN_REAL_TASK_DURATION = duration
        if (self.MAX_REAL_TASK_DURATION < duration):
            self.MAX_REAL_TASK_DURATION = duration
