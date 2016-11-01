import nbconvert_watch
import argparse
import logging

def main():
    parser = argparse.ArgumentParser(description='Automatically run and convert notebooks as you edit.')
    parser.add_argument('notebook_dir', help='The directory to watch for .ipynb notebooks')
    parser.add_argument('results_dir', help='The directory to save the results')
    parser.add_argument('-v', '--verbose', help='Show verbose output', action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    nbconvert_watch.main(args.notebook_dir, args.results_dir)
