from itertools import chain
from datetime import datetime
from time import sleep
import threading


class LogTrap(object):
    class LogThread(threading.Thread):
        def __init__(self, proc_pipe):
            super(LogTrap.LogThread, self).__init__()

            self.proc_pipe = proc_pipe
            self.die = threading.Event()
            self.log = []
            self.latest_time = datetime.now()

        def run(self):
            while not self.die.is_set():
                self.latest_time = datetime.now()

                line = self.proc_pipe.readline()
                while line and line.strip() != '':
                    self.log += [line]
                    line = self.proc_pipe.readline()
                    self.latest_time = datetime.now()
                self.die.wait(1)

    class Reader(object):
        def __init__(self, log_thread):
            self.thread = log_thread
            self.last_read = datetime.now()
            self.latest_read_index = 0

        def read(self):
            if self.thread.latest_time > self.last_read:
                self.last_read = datetime.now()

                result = self.thread.log[self.latest_read_index:]
                self.latest_read_index = len(self.thread.log) - 1
                return tuple(result)
            else:
                return []

        def iter(self): pass

    def __init__(self, proc_pipe):
        self.thread = self.LogThread(proc_pipe)
        self.thread.start()

    def get_complete_log(self):
        log = (''.join(self.thread.log)).split('\n')
        return log


    def get_reader(self):
        return self.Reader(log_thread=self.thread)

    def destroy(self):
        self.thread.die.set()
