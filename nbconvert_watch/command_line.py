import nbconvert_watch
import sys

def main():
    if len(sys.argv) != 3:
        print('Welcome to nbconvert-watch! Please run "nbconvert-watch <notebook folder> <results folder>"')
        print('Press Enter to continue...')
        input()
        exit()

    notebook_dir, results_dir = sys.argv[1:3]
    nbconvert_watch.main(notebook_dir, results_dir)
