import sys
import time
import subprocess
import threading    
import os
import os.path
import signal
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

_DEBUG = True

class KillableProcess(object):
    def __init__(self, cmd):
        self.process = subprocess.Popen(cmd, creationflags=(0 if _DEBUG else 0x08000000), shell=True)

    def kill(self):
        if hasattr(os, 'getpgid'): # UNIX
            os.killpg(os.getpgid(self.process.pid), signal.SIGINT)
        else: # Windows
            subprocess.Popen('TASKKILL /F /PID {pid} /T'.format(pid=self.process.pid))

def runCommand(notebook_path, results_dir):
    cmd = '''jupyter nbconvert --to=html --output-dir="%s" --execute --allow-errors --ExecutePreprocessor.timeout=-1 "%s"'''
    cmd = cmd % (results_dir, notebook_path)
    print('Running command: %s' % cmd)
    return KillableProcess(cmd)

class ThrottledPool(object):

    def __init__(self, throttle_time=5):
        self.throttle_time = throttle_time
        self.lock = threading.Lock()
        self.counters = {}
        self.processes = {}

    def run(self, notebook_path, results_dir):
        def maybeRunJob():
            with self.lock:
                if notebook_path in self.processes:
                    success = self.processes[notebook_path].kill()
                    print('Terminating %s' % notebook_path)
                    del self.processes[notebook_path]
                self.counters[notebook_path] = self.counters.get(notebook_path, 0) + 1
            time.sleep(self.throttle_time)
            with self.lock:
                self.counters[notebook_path] -= 1
                if self.counters[notebook_path] <= 0:
                    self.counters[notebook_path] = 0
                    if os.path.isfile(notebook_path):
                        self.processes[notebook_path] = runCommand(notebook_path, results_dir)
        threading.Thread(target=maybeRunJob).start()

class RunNotebookEventHandler(PatternMatchingEventHandler):

    def __init__(self, notebook_dir, results_dir):
        super(RunNotebookEventHandler, self).__init__(patterns=["*.ipynb"], ignore_directories=True)
        self.notebook_dir = notebook_dir
        self.results_dir = results_dir
        self.pool = ThrottledPool()

    def run(self, event):
        self.pool.run(os.path.join(self.notebook_dir, event.src_path), self.results_dir)

    def on_created(self, event):
        print(event.src_path + ' created')
        self.run(event)

    def on_modified(self, event):
        print(event.src_path + ' modified')
        self.run(event)

    def on_deleted(self, event):
        print(event.src_path + ' deleted')
        self.run(event)

def main(notebook_dir, results_dir):
    notebook_dir = os.path.abspath(notebook_dir)
    results_dir = os.path.abspath(results_dir)

    print('Watching notebooks in "%s", and saving the results to "%s"' % (notebook_dir, results_dir))

    event_handler = RunNotebookEventHandler(notebook_dir, results_dir)
    observer = Observer()
    observer.schedule(event_handler, notebook_dir, recursive=False)
    observer.start()
    print('Press Control-C to stop')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Welcome to nbconvert_watch! Please run "python %s <notebook folder> <results folder>"' % __file__)
        print('Press Enter to continue...')
        input()
        exit()

    notebook_dir, results_dir = sys.argv[1:3]
    main(notebook_dir, results_dir)
