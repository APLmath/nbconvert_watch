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
    

@coalesce(5)
def test():
    print 'Hello'

class Test(object):
    @coalesce(5)
    def go(self, text):
        print text

def process_apply_and_signal(func, args, completed_value, process_event):
    func(*args)
    completed_value.value = True
    process_event.set()

class KillableProcess(object):
    def __init__(self, func, args):
        def thread_func(func, args, thread_event):
            time.sleep(5)
            completed_value = multiprocessing.Value('b', False)
            process_event = multiprocessing.Event()
            process = multiprocessing.Process(target=process_apply_and_signal, args=(func, args, completed_value, process_event))
            process.start()
            process_event.wait()
            if not completed_value.value:
                process.kill()
        self.func = func
        self.args = args
        self.completed = False
        self.event = multiprocessing.Event()

    def start(self):
        def wrapped_func(func, args, event):
            process = multiprocessing.Process(target=func, args=self.args)
            process.start()
            process.wait()
            self.completed = True
            event.set()
        

    def kill(self):
        self.event.set()

class KeyedProcessPool(object):
    def __init__(self):
        self.processes = {}
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.last_call

    def apply_async(key, func, args):
        if self.processes.has_key(key):
            self.processes[key].kill()
            del self.processes[key]
