import nbconvert_watch
import argparse

def main():
    parser = argparse.ArgumentParser(description='Automatically run and convert notebooks as you edit.')
    parser.add_argument('notebook_dir', help='The directory to watch for .ipynb notebooks')
    parser.add_argument('results_dir', help='The directory to save the results')
    args = parser.parse_args()

    nbconvert_watch.main(args.notebook_dir, args.results_dir)
