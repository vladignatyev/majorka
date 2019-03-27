import logging
import psutil
import subprocess
import time
import socket

from proc.pipelog import LogTrap


class AwaitingException(Exception): pass
#
# def await_remote_socket(host, port, max_retries=5):
#     num_retries = 0
#
#     while num_retries < max_retries:
#         try:
#             s = socket.socket()  # todo: use socket_ex instead
#             s.connect((host, port))
#             s.close()
#             return
#         except socket.error:
#             self.log.debug("Awaiting %s:%s to start listening...", host, port)
#             num_retries += 1
#             time.sleep(1)
#
#     raise AwaitingException("Num retries ({retries}) exceed, " \
#                             "but socket is still not alive for {host}:{port}" \
#                             .format(host=host,
#                                     port=port,
#                                     retries=num_retries))


class MultiprocessException(Exception): pass

class Multiprocess(object):
    LOGGER = 'procession'

    def __init__(self, proc, logger=None):
        self.log = logger or logging.getLogger(Multiprocess.LOGGER)
        self.proc = proc

        self.processes = {}
        self.proc_loggers = {}

    def launch(self, proc=None):
        proc = proc or self.proc

        self._log_info_header("Starting...")

        for name, args in proc:
            if self.spawn(name=name, args=args) is None:
                self.log.error("Unable to spawn process `%s` with args `%s`. The process with such name is already exist", name, ' '.join(args))
                self.kill()

        self._log_proc_tree()

    def spawn(self, name, args):
        if name in self.processes.keys():
            return None

        self.log.info("Launching %s: %s", name, ' '.join(args))

        self.processes[name] = proc = psutil.Popen(args=args, bufsize=0,
                                                   stdout=subprocess.PIPE,
                                                   stdin=subprocess.PIPE,
                                                   stderr=subprocess.PIPE)
        self.proc_loggers[name] = LogTrap(proc.stdout)

        return proc


    def poll(self):
        return_codes = map(lambda name, proc: name, proc, proc.poll(), self.processes.items())

        died = filter(lambda name, proc, code: code is not None, return_codes)

        postmortem = [None] * len(died)
        for i, dead in enumerate(died):
            name, proc, code = dead
            self.log.warn("process `%s` PID %s exited unexpectedly with exit code %s", name, proc.pid, code)
            postmortem[i] = (name, proc, self.proc_loggers[name].get_complete_log())

        if len(postmortem) == 0:
            return None
        else:
            return postmortem

    def kill(self):
        self._log_info_header("Terminating...")

        self._reap_children()
        self._destroy_logs()

    def await_socket(self, proc, port, max_retries=5, ips=('127.0.0.1', '0.0.0.0',)):
        num_retries = 0
        socket_ready = False
        while not socket_ready and num_retries < max_retries:
            conns = self.processes[proc].connections('inet')
            for c in conns:
                for ip in ips:
                    status_ok = c.status == 'ESTABLISHED' or c.status == 'LISTEN'
                    ip_ok = c.laddr.ip == ip
                    port_ok = c.laddr.port == int(port)
                    socket_ready = socket_ready or (status_ok and ip_ok and port_ok)
            if socket_ready:
                self.log.debug("`%s` started.", proc)
                return
            else:
                self.log.debug("Awaiting `%s` process to start listening on port `%s`...", proc, port)
                num_retries += 1
                time.sleep(1)

        raise MultiprocessException("Num retries ({retries}) exceed, " \
                                    "but socket port {port} still not alive " \
                                    "for process {proc}" \
                                    .format(proc=proc,
                                            port=port,
                                            retries=num_retries))


    def _log_info_header(self, msg):
        self.log.info("")
        self.log.info(msg)
        self.log.info("==============================")

    def _destroy_logs(self):
        map(lambda (name, logger): logger.destroy(), self.proc_loggers.items())

    def _log_proc_tree(self):
        self._log_info_header("Process tree")

        max_width = max(map(lambda k: len(k + " PID: "), self.processes.keys()))
        self.log.info("%s%s", " PID: ".rjust(max_width), psutil.Process().pid)

        for name, proc in self.processes.items():
            left  = "{name} PID: ".format(name=name.capitalize()).rjust(max_width)
            right = "|--- {pid}".format(pid=proc.pid)
            self.log.info("%s%s", left, right)

    def _get_pids(self):
        return dict(map(lambda (name, p): (p.pid, name), self.processes.items()))

    def _reap_children(self, timeout=3):
        """Tries hard to terminate and ultimately kill all the children of this process.
        See: https://psutil.readthedocs.io/en/latest/#terminate-my-children
        """
        pids = self._get_pids()

        def on_terminate(proc):
            self.log.info("\tprocess `%s` PID %s terminated with exit code %s", pids[proc.pid], proc.pid, proc.returncode)

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
                self.log.debug("\tprocess %s survived SIGTERM; trying SIGKILL", p)
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
            gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
            if alive:
                # give up
                for p in alive:
                    self.log.error("Zombie process eliminated! Process %s survived SIGKILL.", p)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_val, trace):
        self.kill()
        return False # propagate exceptions
