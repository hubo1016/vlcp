#!/bin/bash -xe
sudo python -m unittest discover
if [ "${TRAVIS_EVENT_TYPE}" == "cron" -o "${TRAVIS_EVENT_TYPE}" == "pull_request" ]; then
	sudo apt-get install -y openvswitch-switch
	python setup.py bdist_wheel
	wget https://github.com/hubo1016/vlcp-controller-test/archive/master.tar.gz -O ./vlcp-controller-test.tar.gz
	tar -xzvf vlcp-controller-test.tar.gz
	cp dist/*.whl vlcp-controller-test-master/
	pushd vlcp-controller-test-master/
	sudo bash -xe starttest.sh
fi
