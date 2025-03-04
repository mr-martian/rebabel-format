# CoNLL-U

The CoNLL-U converters read and write files in the [Universal Dependencies format](https://universaldependencies.org/format).

```conllu
# sent_id = 1
1	Teh	the	DET	dt	Definiteness=Def	2	det	2:det	CorrectForm=The
2	man	man	NOUN	nn	Number=Sing	3	nsubj	3:nsubj|5:nsubj	_
3	snores	snore	VERB	vb	Number=Sing|Person=3	0	root	0:root	_
4	and	and	CCONJ	cj	_	5	cc	5:cc	_
5	dreams	dream	VERB	vb	Number=Sing|Person=3	3	conj	0:root	SpaceAfter=No
6	.	.	PUNCT	pc	_	3	punct	3:punct	_
```

The first two lines of this example produce the following units:

* type: `sentence`
  * `meta:index`: 1 (integer)
  * `UD:sent_id`: `1` (string)
* type: `word`
  * `meta:index`: 1 (integer)
  * `UD:id`: `1` (string)
  * `UD:form`: `Teh` (string)
  * `UD:lemma`: `the` (string)
  * `UD:upos`: `DET` (string)
  * `UD:xpos`: `dt` (string)
  * `UD:FEATS:Definiteness`: `Def` (string)
  * `UD:head`: word 2 (reference)
  * `UD:deprel`: `det` (string)
  * `UD:MISC:CorrectForm`: `The` (string)
* type: `UD-edep`
  * `UD:parent`: word 2 (reference)
  * `UD:child`: word 1 (reference)
  * `UD:deprel`: `det` (string)

Multi-word tokens have the same structure as words, but the unit type is `token` rather than `word`.

Currently, everything is imported as strings except for `SpaceAfter`, which is converted to a boolean.
When exporting, boolean features become `Yes` and `No`.
