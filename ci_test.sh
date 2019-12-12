#!/bin/bash -xe

# This is a hack. We are running in sudo, must start the virtualenv manually


if [ "${TRAVIS_PYTHON_VERSION:0:4}" == "pypy" ]; then
	venv=${TRAVIS_PYTHON_VERSION}
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

cache_dir=`pwd`/cache

if [ "${TRAVIS_EVENT_TYPE}" == "cron" -o "${TRAVIS_EVENT_TYPE}" == "pull_request" -o "${TRAVIS_TAG:-}" != "" ]; then
	python setup.py bdist_wheel
	wget https://github.com/hubo1016/vlcp-controller-test/archive/master.tar.gz -O ./vlcp-controller-test.tar.gz
	tar -xzvf vlcp-controller-test.tar.gz
	cp dist/*.whl vlcp-controller-test-master/
	pushd vlcp-controller-test-master/
    
    if [ "${TRAVIS_PYTHON_VERSION:0:4}" == "pypy" ]; then
        bash -xe starttest.sh "$venv" "${KV_DB}" "" "" "$ovs_version"
    else
        # only all integration test we will upload coverage file
        # otherwise coverage line maybe zigzag because common commit 
        # will only run unittest
        bash <(curl -s https://codecov.io/bash) -F unittests -e DB=${KV_DB}
        bash -xe starttest.sh "$venv" "${KV_DB}" "coverage" "$cache_dir" "$ovs_version"
        bash <(curl -s https://codecov.io/bash) -F integration -e DB=${KV_DB}
    fi
fi
