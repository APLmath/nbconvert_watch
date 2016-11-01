import multiprocessing
import threading
import psutil
import time

def process_apply_and_signal(func, args, kwargs, completed_value, process_event):
    func(*args, **kwargs)
    completed_value.value = True
    process_event.set()

class KillableProcess(object):
    def __init__(self, target, args=(), kwargs={}, completion_func=None):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.completion_func = completion_func

        self.completed_value = multiprocessing.Value('b', False)
        self.process_event = multiprocessing.Event()

    def start(self):
        def thread_func(func, args, kwargs, completed_value, process_event):
            process = multiprocessing.Process(target=process_apply_and_signal, args=(func, args, kwargs, completed_value, process_event))
            process.start()
            process_event.wait()

            if not completed_value.value:
                psutil_process = psutil.Process(pid=process.pid)
                psutil_process.kill()

            self.completion_func()

        thread = threading.Thread(target=thread_func, args=(self.target, self.args, self.kwargs, self.completed_value, self.process_event))
        thread.start()

        self.start_time = time.time()

    def kill(self):
        self.process_event.set()

class KeyedProcessPool(object):
    def __init__(self, throttle_sec=5):
        self.lock = threading.Lock()
        self.throttle_sec = throttle_sec
        self.upcoming_invocations = {}
        self.active_processes = {}

    def apply_async(self, key, func, args=(), kwargs={}):
        if key in self.active_processes:
            self.active_processes[key].kill()

        def delete_key_from_processes():
            del self.active_processes[key]

        def delayed_invocation():
            with self.lock:
                del self.upcoming_invocations[key]
            process = KillableProcess(func, args=args, kwargs=kwargs, completion_func=delete_key_from_processes)
            self.active_processes[key] = process
            process.start()

        with self.lock:
            if key in self.upcoming_invocations:
                self.upcoming_invocations[key].cancel()
            self.upcoming_invocations[key] = threading.Timer(self.throttle_sec, delayed_invocation)
            self.upcoming_invocations[key].start()
    
    def close(self):
        with self.lock:
            for key, upcoming_invocation in self.upcoming_invocations.items():
                upcoming_invocation.cancel()
                del self.upcoming_invocations[key]
        
        for key, active_process in self.active_processes.items():
            active_process.kill()
