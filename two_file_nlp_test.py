from rebabel_format import get_process_parameters, load_processes, load_readers, load_writers, run_command
load_processes(True)
load_readers(True)
load_writers(True)

get_process_parameters('export')


run_command('import', infiles=["rebabel_format/test/data/nlp_language_file.txt"],
            mode='nlp_pos', db='merge.db', nlpFileType='language')

run_command('import', infiles=["rebabel_format/test/data/nlp_pos_file.txt"],
            mode='nlp_pos', db='merge.db', nlpFileType='pos',
            merge_on={
                'sentence': 'meta:index',
                'word': 'meta:index'
            })

run_command(
    'export', mode='flextext', db='merge.db', outfile='out.flextext',
    mappings=[
        {'in_type': 'sentence', 'out_type': 'phrase'},
        {'in_feature': 'nlp:form', 'out_feature': 'FlexText:en:txt'},
        {'in_feature': 'nlp:pos', 'out_feature': 'FlexText:en:pos'}
    ],
    root='phrase',
    skip=['morph'],
)
