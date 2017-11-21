.PHONY: release build test

release: test
	python setup.py sdist upload

build: test
	python setup.py sdist

test:
	echo 'not test'
	#pytest ./tests/ 