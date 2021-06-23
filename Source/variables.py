from flask import Flask,request, url_for, flash,redirect
from flask import abort,send_from_directory
from flask import render_template

import threading
import queue
import os
import time
import requests
from requests.exceptions import HTTPError
from asyncio.tasks import all_tasks

from werkzeug.utils import secure_filename
from markupsafe import escape

import asyncio

from flask import jsonify
import logging
from inspect import currentframe
from logging import info as pinfo
from logging import error as perror
import shutil
import errno
import pkgutil
import json
import copy
class Object:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

class InvalidUsage(Exception):
    status_code = 400
    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload
    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv
class NoFreeNodes(Exception):
    __name__='NoFreeNodes'
    def __init__(self, args):
        Exception.__init__(self)
        self.args = args


class my_queue:
    def __repr__(self):
        return str(self.maxsize)
    def __str__(self):
        return str(self.maxsize)
    def __dict__(self):
        return str(self.maxsize)
    def val(self):
        return self.maxsize
    def __init__(self, maxsize=0):
        self.maxsize=int(maxsize)
        self.cur=int(maxsize)
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0
    def _qsize(self):
        return self.cur
    def _add(self,val=1):
        temp =self.cur
        self.cur=self.cur+val
        return temp
    def empty(self):
        return self.cur==0
    def full(self):
        return self.cur==self.maxsize
    def put(self, item=1, block=True, timeout=None):
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() >= self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time.time() + timeout
                    while self._qsize() >= self.maxsize:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise queue.Full
                        self.not_full.wait(remaining)
            self._add(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
    def get(self, block=True, timeout=None):
        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time.time() + timeout
                print(endtime)
                while not self._qsize():
                    print(endtime)
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise queue.Empty
                    self.not_empty.wait(remaining)
            item = self._add(-1)
            self.not_full.notify()
            return item
class node_count(dict):
    def __init__(self, dictionary={}, timeout=0):
        print('Tout {}'.format(timeout))
        self.timeout=timeout
        self.node_update = threading.Condition(threading.Lock())
    def set_node(self,node,val):
        with self.node_update:
            self[node]=val
            pinfo(val)
            pinfo(self)
            self.node_update.notify_all()
    def put_to(self,node,val=1):
        with self.node_update:
            self[node].put(val)
            self.node_update.notify_all()
    def get(self):
        try:
            endtime = time.time() + self.timeout
            with self.node_update:
                while True:
                    for node in self:
                        pinfo(node)
                        try:
                            if self[node].empty():
                                continue
                            self[node].get(timeout=0.01)
                            pinfo("got_node")
                            return node
                        except queue.Full:
                                pinfo('{} node is full, procceding to next node'.format(node))
                        except Exception as ex:
                            print("Node skipped:",self)
                            template = "An exception of type {0} occurred on node. Arguments:\n{1!r}"
                            message = template.format(type(ex).__name__, ex.args)
                            print(message)
                    if self.timeout != 0:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise NoFreeNodes
                        self.node_update.wait(remaining)
                    else:
                        self.node_update.wait()
        except Exception as ex:
            perror("Error occured on get() in node_count")
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
        return None
class safe_int(dict):
    def __repr__(self):
        return ''+self.val
    def __str__(self):
        return ''+self.val
    def __init__(self, val):
        self.lock=threading.Lock()
        self.val=val
    def value(self):
        return self.val
    def add(self,val):
        with self.lock:
            temp = self.val
            self.val=self.val+val
            return temp


MAX_FAILS=3

tasks_to_be_done=safe_int(0)
jobs_nums={'0':10,'1':5,'2':0}
MAX_QUEUE_SIZE=1
WORKERS_NUM=10
CENTRAL_NODE='127.0.0.1:5000'
IP='127.0.0.1'
STD_JOB_DIR_LIST=['inputs','results','modules']
SOCKET=5000
#CENTRAL_URL='http:\\'+CENTRAL_IP+':'+CENTRAL_SID
UPLOAD_FOLDER = '/mnt/d/Dipl/Source/Upload'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'ini','data'}
JOB_TYPES=['0','1','2']

jobQueue=queue.Queue()
faulty_nodes=queue.Queue()
collectData=[]

node_num=safe_int(0)
cur_node=-1
node_dict={}#'127.0.0.1:5000':0}
#словарь очередей
MAX_QUEUE_SIZE_WORK0=10
MAX_QUEUE_SIZE_WORK1=5
MAX_QUEUE_SIZE_WORK2=5

recv_mutex=threading.Lock()
update=threading.Condition(recv_mutex)
faulty_update=threading.Condition(threading.Lock())
tasks_done_lock=threading.Lock()
#написать класс который на get возвращает один из узлов 

#connect должен кидать размеры своих работников, которые закидываются в dict_nodes0(работники 0 типа), dict_nodes1(работники 1 типа) в данном количестве соответсвенно
dict_nodes0=node_count()#'127.0.0.1:5000':queue.Queue(MAX_QUEUE_SIZE),'127.0.0.1:5001':queue.Queue(MAX_QUEUE_SIZE)}
dict_nodes1=node_count()#'127.0.0.1:5000':my_queue(MAX_QUEUE_SIZE_WORK1),'127.0.0.1:5001':queue.Queue(MAX_QUEUE_SIZE_WORK1)})
dict_nodes2=node_count()#'127.0.0.1:5000':queue.Queue(MAX_QUEUE_SIZE_WORK2),'127.0.0.1:5001':queue.Queue(MAX_QUEUE_SIZE_WORK2)}
#словарь в котором храняться очереди для всех работ
jobs_types={'0':dict_nodes0,'1':dict_nodes1,'2':dict_nodes2}
tasktype_queues={}

node_fail_count={}
tasks_done={}#'0':{2:'job0_2.txt',1:'job0_1.txt'}
job_num=safe_int(0)
#job_class=JobType({0:all_nodes0,1:all_nodes1,2:all_nodes2})

t_begin=0

close_workers=False
down_workers=safe_int(0)
job_input={}
close_workers=False