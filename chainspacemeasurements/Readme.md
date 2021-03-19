
# How to create a Chainspace testbed using AWS EC2 instances

## Creating AWS EC2 instances and installing Chainspace 

Clone github repository
```shell
$ git clone git@github.com:srene/byzcuit.git
```

Install python libraries
```shell
$ cd byzcuit
$ pip install -e chainspacemeasurements
```

Create EC2 instances. Replace X with the number of validators instances and clients required. In n.launch(X,t) the t value is for the type of node. 1 is for clients and 0 for validators.
```shell
$ cd chainspacemeasurements/chainspacemeasurements/
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.launch(X,0); n.launch(X,1);’  
```
Note that is necessary to generate a keypair pem file to access the EC2 instances. The pem file can be generated first logging in AWS-cli and using the [keypair.py](https://github.com/srene/byzcuit/blob/master/chainspacemeasurements/chainspacemeasurements/keypair.py) script. The file should be named `ec2-keypair.pem`.


Install chainspace on EC2 instances
```shell
$ python -c 'from chainspacemeasurements.instances import ChainspaceNetwork; n = ChainspaceNetwork(0); n.ssh_connect(0); n.ssh_connect(1); n.install_deps(0); n.install_deps(1); n.install_core(0); n.install_core(1);’
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

## Running transactions on the testbed

To run transactions on the running tesbed the following command should be used. This command will generate transactions with objects from the shards indicated in the file 

```shell
$ python tester.py sharding_measurements arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 
```
where the arguments should be:

* arg1: Minimum number of validators per shard.
* arg2: Maximum number of validators per shard.
* arg3: Number of transactions generated.
* arg4: Number of shards in the testbed.
* arg5: Number of runs of the evaluation.
* arg6: Shard list input file. This file should indicate to which shard each of the object of the transactions belongs separated by ','. This is an [example](https://github.com/srene/byzcuit/blob/master/chainspacemeasurements/chainspacemeasurements/shards.txt) for a test with 12000 transactions and two shards. By default there are two input objects per transaction. This can be modified [here](https://github.com/srene/byzcuit/blob/79dc906b79c4b371b342760d6dc6a9ee540fc673/chainspacemeasurements/chainspacemeasurements/tester.py#L330).
* arg7: This is the output file where the TPS will be logged.
* arg8: This is the output file where the latencies measured in the clients will be logged.

Note: A minimum number of 3 validators and 8 clients per shard is required
