import unittest
import contextlib
import glob
import io
import os
import tempfile
from rebabel_format import load_processes, load_readers, load_writers
from rebabel_format.process import ALL_PROCESSES
from rebabel_format.config import read_config

load_processes(False)
load_readers(False)
load_writers(False)

class StaticTests(unittest.TestCase):
    '''Run commands and compare the outputs to the files in static/

    To add a new test to this runner:
    - Create static/NAME.toml with desired parameters
    - Put any input data in static/data
    - For each command you want to run:
      - put the expected output in a file named static/NAME.ORDER.COMMAND.txt
      - files will be sorted lexicographically, so zero-pad ORDER
      - outputs will be compared with leading and trailing whitespace trimmed
    '''
    def single_command(self, db, config, fname):
        command = fname.split('.')[-2]
        with self.subTest(fname):
            if command == 'export':
                with tempfile.NamedTemporaryFile() as fout:
                    proc = ALL_PROCESSES[command](config, db=db, outfile=fout.name)
                    proc.run()
                    text = fout.read().decode('utf-8')
            else:
                stream = io.StringIO()
                with contextlib.redirect_stdout(stream):
                    proc = ALL_PROCESSES[command](config, db=db)
                    proc.run()
                text = stream.getvalue()
            with open(fname) as fin:
                expected = fin.read()
                self.assertEqual(expected.strip(), text.strip())

    def single_test(self, name):
        with self.subTest(name):
            config = read_config(name + '.toml')
            with tempfile.TemporaryDirectory() as db_dir:
                db = os.path.join(db_dir, 'data.db')
                with self.subTest('import'):
                    proc = ALL_PROCESSES['import'](config, db=db)
                    proc.run()
                for path in sorted(glob.glob(name+'.*')):
                    if path.endswith('.toml') or path.endswith('~'):
                        continue
                    self.single_command(db, config, path)

    def runTest(self):
        cwd_was = os.getcwd()
        dir_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'static')
        os.chdir(dir_name)
        for fname in glob.glob('*.toml'):
            self.single_test(fname[:-5])
        os.chdir(cwd_was)
