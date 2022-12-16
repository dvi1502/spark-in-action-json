import runpy
import sys
from arguments import args_reader


if __name__ == '__main__':
    args = args_reader()
    runpy.run_path(args.module, run_name='__main__')
