.PHONY: release build test cov

release: test
	python setup.py bdist_wheel
	twine upload dist/*.whl

build: test
	python setup.py bdist_wheel

test:
	pytest -s -v --cov-report term --cov=aiosqlite3

cov cover coverage:
	py.test -s -v --cov-report term --cov-report html --cov aiosqlite3 ./tests
	@echo "open file://`pwd`/htmlcov/index.html"