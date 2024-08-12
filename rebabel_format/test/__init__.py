import unittest
import contextlib
import io
import os
import tempfile
from ..processes.process import ALL_PROCESSES

class CommandRunner:
    file_contents = ''

    def setUp(self):
        self.db_dir = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.db_dir.name, 'data.db')

    def run_command(self, name, **kwargs):
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            proc = ALL_PROCESSES[name]({}, db=self.db, **kwargs)
            proc.run()
        return stream.getvalue()

    def import_string(self, mode, **kwargs):
        with tempfile.NamedTemporaryFile() as f:
            f.write(self.file_contents.encode('utf-8'))
            f.flush()
            return self.run_command('import', mode=mode, infiles=[f.name],
                                    **kwargs)

class ConlluImport(CommandRunner, unittest.TestCase):
    file_contents = '''
# sent_id = 1
1	The	the	DET	dt	Definiteness=Def	2	det	_	_
2	man	man	NOUN	nn	Number=Sing	3	nsubj	_	_
3	snores	snore	VERB	vb	Number=Sing|Person=3	0	root	_	SpaceAfter=No
4	.	.	PUNCT	pc	_	3	punct	_	_
'''

    def runTest(self):
        self.import_string('conllu')

        schema = self.run_command('inspect', schema=True)
        expected_schema = '''
sentence
	UD
		sent_id: str

word
	UD
		deprel: str
		form: str
		head: ref
		id: str
		lemma: str
		null: bool
		upos: str
		xpos: str
	UD/FEATS
		Definiteness: str
		Number: str
		Person: str
	UD/MISC
		SpaceAfter: bool
	meta
		index: int
'''
        self.assertEqual(expected_schema.strip(), schema.strip())
