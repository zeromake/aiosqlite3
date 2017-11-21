.PHONY: release build test

release: test
	python setup.py sdist_wheel upload

build: test
	python setup.py sdist_wheel

test:
	echo 'not test'
	#pytest ./tests/ 