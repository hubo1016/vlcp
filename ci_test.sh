#!/bin/bash -xe

# This is a hack. We are running in sudo, must start the virtualenv manually

if [ "${TRAVIS_PYTHON_VERSION}" == "pypy" ]; then
	venv=pypy
else
	venv=python${TRAVIS_PYTHON_VERSION}
fi

[ -f "/home/travis/virtualenv/${venv}/bin/activate" ] && source /home/travis/virtualenv/${venv}/bin/activate

python --version
python -m unittest discover
if [ "${TRAVIS_EVENT_TYPE}" == "cron" -o "${TRAVIS_EVENT_TYPE}" == "pull_request" -o "${TRAVIS_TAG:-}" != "" ]; then
	python setup.py bdist_wheel
	wget https://github.com/hubo1016/vlcp-controller-test/archive/master.tar.gz -O ./vlcp-controller-test.tar.gz
	tar -xzvf vlcp-controller-test.tar.gz
	cp dist/*.whl vlcp-controller-test-master/
	pushd vlcp-controller-test-master/
	bash -xe starttest.sh $venv ${KV_DB}
fi
