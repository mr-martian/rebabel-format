test:
	python3 -m unittest discover

install:
	pip3 install -e .

coverage:
	coverage run --source 'rebabel_format' -m unittest discover
	coverage report
	coverage html
