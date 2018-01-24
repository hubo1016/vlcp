#!/bin/bash -xe

# This is a hack. We are running in sudo, must start the virtualenv manually

if [ "${TRAVIS_PYTHON_VERSION:0:4}" == "pypy" ]; then
	venv=pypy-5.4
else
	venv=python${TRAVIS_PYTHON_VERSION}
fi

[ -f "/home/travis/virtualenv/${venv}/bin/activate" ] && source /home/travis/virtualenv/${venv}/bin/activate

python --version
coverage --version

# run in pypy , never run coverage 
if [ "${TRAVIS_PYTHON_VERSION:0:4}" == "pypy" ]; then
	python -m unittest discover
else
	coverage run -m unittest discover
fi

if [ "${TRAVIS_EVENT_TYPE}" == "cron" -o "${TRAVIS_EVENT_TYPE}" == "pull_request" -o "${TRAVIS_TAG:-}" != "" ]; then
	python setup.py bdist_wheel
	wget https://github.com/hubo1016/vlcp-controller-test/archive/master.tar.gz -O ./vlcp-controller-test.tar.gz
	tar -xzvf vlcp-controller-test.tar.gz
	cp dist/*.whl vlcp-controller-test-master/
	pushd vlcp-controller-test-master/
    
    if [ "${TRAVIS_PYTHON_VERSION:0:4}" == "pypy" ]; then
        bash -xe starttest.sh $venv ${KV_DB}
    else
        bash -xe starttest.sh $venv ${KV_DB} "coverage"
    fi
fi
