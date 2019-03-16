import logging
import psutil
import subprocess



_LOGGER = 'procession'


class Session(object):
    def __init__(self, proc=(), logger=logging.getLogger(_LOGGER)):
        self.log = logger
        self.proc = proc
        self.processes = None

    def launch(self):
        self.log.info("Session has started!")
        self.log.info("==============================")
        self.processes = dict(map(lambda (name, args): (name, self.spawn(name=name, args=args)), self.proc))
        self._log_proc_tree()

    def spawn(self, name, args):
        self.log.info("Launching %s: %s", name, ' '.join(args))
        return psutil.Popen(args=args, bufsize=0, stdout=subprocess.PIPE,
            stdin=subprocess.PIPE, stderr=subprocess.PIPE)

    def poll(self):
        #todo:
        result = {}
        for name, proc in self.processes.items():
            self.log.debug("Trying to read STDERR from `%s`", name)
            # result[name] = proc.stderr.readline()
        return result

    def kill(self):
        self.log.info("Terminating...")
        self._reap_children()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_val, trace):
        self.kill()
        return False

    def _log_proc_tree(self):
        self.log.info("")
        self.log.info("Process tree")
        self.log.info("==============================")

        max_width = max(map(lambda k: len(k + " PID: "), self.processes.keys()))
        self.log.info("%s%s", " PID: ".rjust(max_width), psutil.Process().pid)

        for name, proc in self.processes.items():
            left  = "{name} PID: ".format(name=name.capitalize()).rjust(max_width)
            right = "|--- {pid}".format(pid=proc.pid)
            self.log.info("%s%s", left, right)

        self.log.info("==============================")


    def _reap_children(self, timeout=3):
        """Tries hard to terminate and ultimately kill all the children of this process.
        See: https://psutil.readthedocs.io/en/latest/#terminate-my-children
        """
        pids = dict(map(lambda (name, p): (p.pid, name), self.processes.items()))

        def on_terminate(proc):
            self.log.info("process `%s` PID %s terminated with exit code %s", pids[proc.pid], proc.pid, proc.returncode)

        # send SIGTERM
        for name, p in self.processes.items():
            try:
                p.terminate()
            except psutil.NoSuchProcess:
                pass
        gone, alive = psutil.wait_procs(self.processes.values(), timeout=timeout, callback=on_terminate)
        if alive:
            # send SIGKILL
            for p in alive:
                self.log.debug("process %s survived SIGTERM; trying SIGKILL", p)
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
            gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
            if alive:
                # give up
                for p in alive:
                    self.log.error("zombie process eliminated! Process %s survived SIGKILL.", p)
