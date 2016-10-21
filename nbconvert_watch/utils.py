import multiprocessing
import threading
import psutil
import time

def coalesce(sec=5):
    def coalesce_decorator(func):
        upcoming_invocation = [None]
        lock = threading.Lock()

        def delayed_func(*args, **kwargs):
            def delayed_call():
                with lock:
                    upcoming_invocation[0] = None
                func(*args, **kwargs)

            with lock:
                if upcoming_invocation[0]:
                    upcoming_invocation[0].cancel()
                upcoming_invocation[0] = threading.Timer(sec, delayed_call)
                upcoming_invocation[0].start()

        return delayed_func
    return coalesce_decorator

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

            if completed_value.value:
                if self.completion_func:
                    self.completion_func()
            else:
                psutil_process = psutil.Process(pid=process.pid)
                psutil_process.kill()
        
        thread = threading.Thread(target=thread_func, args=(self.target, self.args, self.kwargs, self.completed_value, self.process_event))
        thread.start()

    def kill(self):
        self.process_event.set()

class KeyedProcessPool(object):
    def __init__(self):
        self.processes = {}

    def apply_async(self, key, func, args=(), kwargs={}):
        if self.processes.has_key(key):
            self.processes[key].kill()
            del self.processes[key]

        self.__apply_async_lazy(key, func, args, kwargs)

    @coalesce(sec=5)
    def __apply_async_lazy(self, key, func, args=(), kwargs={}):
        def delete_key_from_processes():
            del self.processes[key]

        process = KillableProcess(func, args=args, kwargs=kwargs, completion_func=delete_key_from_processes)
        self.processes[key] = process
        process.start()
