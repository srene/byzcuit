
# How to create a Chainspace testbed using AWS EC2 instances

## Create AWS EC2 instances and install chainspace 

Clone github repository
```shell
$ git clone git@github.com:srene/byzcuit.git
```

Install python libraries
```shell
$ pip install -e chainspacemeasurements
```

Create EC2 instances. Replace X with the number of validators instances and clients.
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.launch(X,0); n.launch(X,1);’  
```
Note that is necessary to generate a keypair pem file to access the EC2 instances. The pem file can be generated first logging in AWS-cli and using the [keypair.py](https://github.com/srene/byzcuit/blob/master/chainspacemeasurements/chainspacemeasurements/keypair.py) script. The file should be named `ec2-keypair.pem`.

Initiate SSH on EC2 instances
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.ssh_connect(0); n.ssh_connect(1);’
```

Install depencencies on EC2 instances
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.install_deps(0); n.install_deps(1);’
```

Install chainspace
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.install_core(0); n.install_core(1);’
```
Optional:

Start instances
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.start(0); n.start(1);’
```

Stop instances
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.stop(0); n.stop(1);’
```
