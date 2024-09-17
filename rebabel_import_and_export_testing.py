import rebabel_format

rebabel_format.load_processes(True)
rebabel_format.load_readers(True)
rebabel_format.load_writers(True)

rebabel_format.get_process_parameters("export")

rebabel_format.run_command(
    "import",
    mode="nlp_pos",
    db="temp.db",
    infiles=["nlp_pos.txt"],
    delimiter="/"
)

rebabel_format.run_command(
    # export out.flextext from in.db
    "export",
    mode="flextext",
    db="temp.db",
    outfile="out.flextext",
    # making some adjustments to account for differences between
    # CoNLL-U and FlexText
    mappings=[
        # use CoNLL-U sentence nodes where FlexText expects phrases
        {"in_type": "sentence", "out_type": "phrase"},
        {"in_feature": "UD:upos", "out_feature": "FlexText/en:pos"},
        # use UD:lemma where FlexText wants FlexText/en:txt
        {"in_feature": "UD:form", "out_feature": "FlexText/en:txt"},
    ],
    # settings specific to the FlexText writer:
    # the highest non-empty node will be the phrase
    # (the CoNLL-U importer currently doesn't create paragraph and document nodes)
    root="phrase",
    # the morpheme layer will also be empty
    skip=["morph"],
)
