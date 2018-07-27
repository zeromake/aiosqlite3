.PHONY: release build test cov

flake:
	flake8 aiosqlite3 tests

release: test
	python setup.py bdist_wheel
	twine upload dist/*.whl

build:
	python setup.py bdist_wheel

test: flake
	pytest -s -v --cov-report term --cov=aiosqlite3

cov cover coverage: flake
	py.test -s -v --cov-report term --cov-report html --cov aiosqlite3 ./tests
	@echo "open file://`pwd`/htmlcov/index.html"