.PHONY: release build test cov

flake:
	flake8 aiosqlite test
release: test
	python setup.py bdist_wheel upload

build: flake test
	python setup.py bdist_wheel

test:
	pytest -s -v --cov-report term --cov=aiosqlite3

cov cover coverage:
	py.test -s -v --cov-report term --cov-report html --cov aiosqlite3 ./tests
	@echo "open file://`pwd`/htmlcov/index.html"