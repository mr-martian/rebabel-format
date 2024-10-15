import unittest
import contextlib
import glob
import io
import os
import tempfile
from rebabel_format import load_processes, load_readers, load_writers, run_command
from rebabel_format.config import read_config

load_processes(False)
load_readers(False)
load_writers(False)

@contextlib.contextmanager
def data_dir(name: str):
    cwd_was = os.getcwd()
    dir_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
    os.chdir(dir_name)
    try:
        yield
    finally:
        os.chdir(cwd_was)

class StaticTests(unittest.TestCase):
    '''
    Run commands and compare the outputs to the files in static/

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
                run_command(command, config, db=db, outfile=fname+'.out')
                with open(fname+'.out') as fin:
                    text = fin.read()
            else:
                stream = io.StringIO()
                with contextlib.redirect_stdout(stream):
                    run_command(command, config, db=db)
                text = stream.getvalue()
            with open(fname) as fin:
                expected = fin.read()
                self.assertEqual(expected.strip()+'\n', text.strip()+'\n')

    def single_test(self, name):
        with self.subTest(name):
            config = read_config(name + '.toml')
            db = name + '.db'
            if os.path.isfile(db):
                os.remove(db)
            with self.subTest('import'):
                run_command('import', config, db=db)
            for path in sorted(glob.glob(name+'.[0123456789]*.txt')):
                self.single_command(db, config, path)

    def runTest(self):
        with data_dir('static'):
            for fname in glob.glob('*.toml'):
                self.single_test(fname[:-5])

class SimpleTest:
    def runTest(self):
        db_name = self.__class__.__name__ + '.db'
        with data_dir(''):
            if os.path.isfile(db_name):
                os.remove(db_name)
            self.commands(db_name)
            from rebabel_format.db import RBBLFile
            db = RBBLFile(db_name)
            self.checks(db)

class FlexTextMergeTest(SimpleTest, unittest.TestCase):
    def commands(self, db_name):
        run_command('import', {}, infiles=['data/merge_text.flextext'],
                    mode='flextext', db=db_name)
        run_command('import', {}, infiles=['data/merge_pos.flextext'],
                    mode='flextext', db=db_name,
                    merge_on={
                        'interlinear-text': 'meta:index',
                        'paragraph': 'meta:index',
                        'phrase': 'meta:index',
                        'word': 'meta:index',
                    })

    def checks(self, db):
        from rebabel_format.query import ResultTable
        table = ResultTable(db,
                            {
                                'phrase': {'type': 'phrase'},
                                'word': {'type': 'word', 'parent': 'phrase'},
                            },
                            order=['phrase', 'word'])
        table.add_features('phrase', ['FlexText:en:segnum', 'meta:index'])
        table.add_features('word', ['FlexText:en:txt', 'FlexText:en:pos',
                                    'meta:index'])
        results = list(table.results())
        self.assertEqual(8, len(results))
        first = set(x[0]['phrase'] for x in results[:4])
        self.assertEqual(1, len(first))
        second = set(x[0]['phrase'] for x in results[:4])
        self.assertEqual(1, len(second))
        expected = [
            ('The', 'DET'), ('man', 'NOUN'), ('snores', 'VERB'), ('.', 'PUNCT'),
            ('The', 'DET'), ('woman', 'NOUN'), ('sings', 'VERB'), ('.', 'PUNCT'),
        ]
        for exp, (nodes, features) in zip(expected, results):
            self.assertEqual(exp[0], features[nodes['word']]['FlexText:en:txt'])
            self.assertEqual(exp[1], features[nodes['word']]['FlexText:en:pos'])

class ConlluNLPMergeTest(SimpleTest, unittest.TestCase):
    def commands(self, db_name):
        run_command('import', {}, infiles=['data/basic.conllu'], mode='conllu',
                    db=db_name)
        run_command('import', {}, infiles=['data/basic.conllu.nlp_apertium.txt'],
                    mode='nlp_pos', db=db_name,
                    merge_on={
                        'sentence': 'meta:index',
                        'word': 'meta:index',
                    })

    def checks(self, db):
        from rebabel_format.query import ResultTable
        table = ResultTable(db,
                            {
                                'sentence': {'type': 'sentence'},
                                'word': {'type': 'word', 'parent': 'sentence'},
                            },
                            order=['sentence', 'word'])
        table.add_features('sentence', ['UD:sent_id', 'meta:index'])
        table.add_features('word', ['UD:form', 'UD:upos', 'nlp:form', 'nlp:pos',
                                    'meta:index'])

        results = list(table.results())
        self.assertEqual(8, len(results))
        self.assertEqual(1, len(set(x[0]['sentence'] for x in results[:4])))
        self.assertEqual(1, len(set(x[0]['sentence'] for x in results[4:])))

        expected = [
            {'meta:index': 1, 'UD:form': 'The', 'UD:upos': 'DET', 'nlp:form': 'The', 'nlp:pos': 'det'},
            {'meta:index': 2, 'UD:form': 'man', 'UD:upos': 'NOUN', 'nlp:form': 'man', 'nlp:pos': 'n'},
            {'meta:index': 3, 'UD:form': 'snores', 'UD:upos': 'VERB', 'nlp:form': 'snores', 'nlp:pos': 'vblex'},
            {'meta:index': 4, 'UD:form': '.', 'UD:upos': 'PUNCT', 'nlp:form': '.', 'nlp:pos': 'sent'},
            {'meta:index': 1, 'UD:form': 'The', 'UD:upos': 'DET', 'nlp:form': 'The', 'nlp:pos': 'det'},
            {'meta:index': 2, 'UD:form': 'woman', 'UD:upos': 'NOUN', 'nlp:form': 'woman', 'nlp:pos': 'n'},
            {'meta:index': 3, 'UD:form': 'sings', 'UD:upos': 'VERB', 'nlp:form': 'sings', 'nlp:pos': 'vblex'},
            {'meta:index': 4, 'UD:form': '.', 'UD:upos': 'PUNCT', 'nlp:form': '.', 'nlp:pos': 'sent'},
        ]
        for (units, features), exp in zip(results, expected):
            fdict = features[units['word']]
            self.assertEqual(len(exp), len(fdict))
            for k in exp:
                self.assertIn(k, fdict)
                self.assertEqual(exp[k], fdict[k])
