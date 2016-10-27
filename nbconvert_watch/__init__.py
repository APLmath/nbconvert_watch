import time
import threading
import os.path
import nbformat
import nbconvert
import utils
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

def runAndConvertNotebook(notebook_path, results_dir):
    if not os.path.isfile(notebook_path):
        return

    with open(notebook_path) as f:
        notebook = nbformat.read(f, as_version=4)

    execute_preprocessor = nbconvert.preprocessors.ExecutePreprocessor(
        timeout=None,
        allow_errors=True)
    execute_preprocessor.preprocess(notebook, {
        'metadata': {
            'path': os.path.dirname(notebook_path)
        }
    })

    html_exporter = nbconvert.HTMLExporter()

    body, resources = html_exporter.from_notebook_node(notebook)

    result_basename = os.path.splitext(os.path.basename(notebook_path))[0]
    result_path = os.path.join(results_dir, result_basename + '.html')

    with open(result_path, 'w') as f:
        f.write(body)

class RunNotebookEventHandler(PatternMatchingEventHandler):

    def __init__(self, notebook_dir, results_dir):
        super(RunNotebookEventHandler, self).__init__(patterns=["*.ipynb"], ignore_directories=True)
        self.notebook_dir = notebook_dir
        self.results_dir = results_dir
        self.pool = utils.KeyedProcessPool()

    def run(self, event):
        notebook_path = os.path.join(self.notebook_dir, event.src_path)
        self.pool.apply_async(notebook_path, runAndConvertNotebook, args=(notebook_path, self.results_dir))

    def on_created(self, event):
        self.run(event)

    def on_modified(self, event):
        self.run(event)

    def on_deleted(self, event):
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
