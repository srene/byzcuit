
# How to create a Chainspace testbed using AWS EC2 instances

## Configure your terminal for aws 

1) create/sign in your aws account
2) configure your local machine to be able to communicate with your aws account
with
```shell
$ aws configure
``` 
you should be able to fill in your credentials (here)
- AWS Access Key ID[None] = here
- AWS Secret Access Key[None] = here
- Default region name [None] = here
- Default output format [None] = None
--> if you have values instead of None, no worries you can modify those

You will find your keys in the section Identity and Access Management (IAM) under the Users section.
There you either already have a user to which you should already know your keys. If not or you dont remember those, simply create a new user.
There you will have your keys.

WARNING: The region you enter in the configuration should be the same than you see on your ec2 dahsboard (in the top right corner).

## Install

Clone github repository
```shell
$ git clone https://github.com/alxd112/byzcuit.git
```

Install python libraries
```shell
$ cd byzcuit
$ pip install -e chainspacemeasurements
```


## FixMe
There are multiple lines where you'll have to do some modifications yourself before moving on:
- in the file instances.py :
  * line 23: change the region if needed
  * line 338: directory = put your own path
  * line 307, 321 : verify that this is the right path on your ec2 instance (this can be done connecting to one of your instance via putty and using the command "pwd")
- tester.py:
  * line 23: change your path
- in the file generate_transactions.py:
  * line 26: change your path
- in the file generate_objects.py:
  * line 21: change your path
- keypair.py
  * line 5 and 8: change the name of the keypair if you want to
  * Execute : "python keypair.py"
  * This should have created a .pem file
  * (optional/if error) specify your region line 2 with "ec2 = boto3.resource('ec2', region_name = 'eu-west-2')"



Then in file instance.py:
- line 129: change the ImageId according to the one in your region (be careful the type of your machine is crucial, to help you, see the end of the file)
- line 133: change the keyName to the one you created earlier
- line 134: If your run the code now, you might get an error saying that your groupId is invalid. This is because you have no group created in your aws account. For this go to your dashboard, click on the section security group and create a new group called chainspace.
WARNING: add inbound and outbound rules such that everything is allowed!
- line 70 : you might get an error saying that it cannot connect to your instances because of an RSA key, add this line of code between client.set_missing... and client.connect(...) :
```
k = paramiko.RSAKey.from_private_key_file('./ec2-keypair.pem')
```
and change the argument key_filename = ... to
```
pkey = k
```
this will solve the problem


## Creating AWS EC2 instances and installing Chainspace 



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
