"""EC2 instance management."""
import time
import os
import sys
from multiprocessing.dummy import Pool
import random
import math
import functools
import operator

import boto3
import paramiko
import numpy

SHARD = 0
CLIENT = 1


class ChainspaceNetwork(object):
    threads = 1000
    aws_api_threads = 5

    def __init__(self, network_id, aws_region='eu-west-2'):
        self.network_id = str(network_id)

        self.aws_region = aws_region
        self.ec2 = boto3.resource('ec2', region_name=aws_region)

        self.ssh_connections = {}
        self.shards = {}
        self.clients = []

        self.logging = True

    def _get_running_instances(self, type):
        return self.ec2.instances.filter(Filters=[
            {'Name': 'tag:type', 'Values': ['chainspace']},
            {'Name': 'tag:network_id', 'Values': [self.network_id]},
            {'Name': 'tag:node_type', 'Values': [str(type)]},
            {'Name': 'instance-state-name', 'Values': ['running']}
        ])

    def _get_stopped_instances(self, type):
        return self.ec2.instances.filter(Filters=[
            {'Name': 'tag:type', 'Values': ['chainspace']},
            {'Name': 'tag:network_id', 'Values': [self.network_id]},
            {'Name': 'tag:node_type', 'Values': [str(type)]},
            {'Name': 'instance-state-name', 'Values': ['stopped']}
        ])

    def _get_all_instances(self, type):
        return self.ec2.instances.filter(Filters=[
            {'Name': 'tag:type', 'Values': ['chainspace']},
            {'Name': 'tag:network_id', 'Values': [self.network_id]},
            {'Name': 'tag:node_type', 'Values': [str(type)]},
        ])

    def _log(self, message):
        if self.logging:
            _safe_print(message)

    def _log_instance(self, instance, message):
        message = '[{}] {}'.format(instance.public_ip_address, message)
        self._log(message)

    def _single_ssh_connect(self, instance):
        self._log_instance(instance, "Initiating SSH connection...")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=instance.public_ip_address, username='admin',key_filename='./ec2-keypair.pem')
        self.ssh_connections[instance] = client
        self._log_instance(instance, "Initiated SSH connection.")

    def _single_ssh_exec(self, instance, command):
        self._log_instance(instance, "Executing command: {}".format(command))
        client = self.ssh_connections[instance]
        stdin, stdout, stderr = client.exec_command(command)
        output = ''
        for message in iter(stdout.readline, ''):
            output += message
            try:
                self._log_instance(instance, message.rstrip())
            except Exception:
                pass
        for message in stderr.readlines():
            try:
                self._log_instance(instance, message.rstrip())
            except Exception:
                pass
        self._log_instance(instance, "Executed command: {}".format(command))

        return (instance, output)

    def _single_ssh_close(self, instance):
        self._log_instance(instance, "Closing SSH connection...")
        client = self.ssh_connections[instance]
        client.close()
        self._log_instance(instance, "Closed SSH connection.")

    def _config_shards_command(self, directory):
        command = ''
        command += 'cd {0};'.format(directory)
        command += 'printf "" > shardConfig.txt;'
        for i, instances in enumerate(self.shards.values()):
            command += 'printf "{0} {1}/shards/s{0}\n" >> shardConfig.txt;'.format(i, directory)
            command += 'cp -r shards/config0 shards/s{0};'.format(i)

            # config hosts.config
            command += 'printf "" > shards/s{0}/hosts.config;'.format(i)
            for j, instance in enumerate(instances):
                command += 'printf "{1} {2} 3001\n" >> shards/s{0}/hosts.config;'.format(i, j, instance.private_ip_address)

            # config system.config
            initial_view = ','.join((str(x) for x in range(len(instances))))
            faulty_replicas = (len(instances)-1)/3
            faulty_replicas = int(math.floor(faulty_replicas))
            command += 'cp shards/config0/system.config.forscript shards/s{0}/system.config.forscript;'.format(i)
            command += 'printf "system.servers.num = {1}\n" >> shards/s{0}/system.config.forscript;'.format(i, len(instances))
            command += 'printf "system.servers.f = {1}\n" >> shards/s{0}/system.config.forscript;'.format(i, faulty_replicas)
            command += 'printf "system.initial.view = {1}\n" >> shards/s{0}/system.config.forscript;'.format(i, initial_view)
            command += 'cp shards/s{0}/system.config.forscript shards/s{0}/system.config;'.format(i)

        #print command
        return command

    def launch(self, count, type):
        self._log("Launching {0} instances of type {1}...".format(count, type))
        self.ec2.create_instances(
            ImageId='ami-e5a35782', # Debian 8.7
            InstanceType='t2.medium',
            MinCount=count,
            MaxCount=count,
            KeyName='ec2-keypair',
            SecurityGroups=['chainspace'],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'type', 'Value': 'chainspace'},
                        {'Key': 'network_id', 'Value': self.network_id},
                        {'Key': 'Name', 'Value': 'Chainspace node (network: {})'.format(self.network_id)},
                        {'Key': 'node_type', 'Value': str(type)},
                    ]
                }
            ]
        )
        self._log("Launched {0} instances of type {1}...".format(count, type))

    def install_deps(self, type):
        self._log("Installing Chainspace dependencies on all nodes...")
        command = 'export DEBIAN_FRONTEND=noninteractive;'
        command += 'export DEBIAN_PRIORITY=critical;'
        command += 'echo "deb http://ftp.debian.org/debian stretch-backports main" | sudo tee -a /etc/apt/sources.list;'
        command += 'until '
        command += 'sudo -E apt update'
        command += '&& sudo -E apt --yes --force-yes -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install -t stretch-backports openjdk-8-jdk'
        command += '&& sudo -E apt --yes --force-yes -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install git python-pip maven screen psmisc'
        command += '; do :; done'
        self.ssh_exec(command, type)
        self._log("Installed Chainspace dependencies on all nodes.")

    def install_core(self, type):
        self._log("Installing Chainspace core on all nodes...")
        command = 'git clone https://github.com/sheharbano/byzcuit chainspace;'
        command += 'sudo pip install chainspace/chainspacecontract;'
        command += 'sudo pip install chainspace/chainspaceapi;'
        command += 'sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java;'
        command += 'cd ~/chainspace/chainspacecore; export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; mvn package assembly:single;'
        command += 'cd ~; mkdir contracts;'
        command += 'cp ~/chainspace/chainspacemeasurements/chainspacemeasurements/contracts/simulator.py contracts'
        self.ssh_exec(command, type)
        self._log("Installed Chainspace core on all nodes.")

    def ssh_connect(self, type):
        self._log("Initiating SSH connection on all nodes...")
        args = [(self._single_ssh_connect, instance) for instance in self._get_running_instances(type)]
        pool = Pool(ChainspaceNetwork.threads)

        pool.map(_multi_args_wrapper, args)
        pool.close()
        pool.join()
        self._log("Initiated SSH connection on all nodes.")

    def ssh_exec(self, command, type):
        self._log("Executing command on all nodes: {}".format(command))
        args = [(self._single_ssh_exec, instance, command) for instance in self._get_running_instances(type)]
        pool = Pool(ChainspaceNetwork.threads)
        result = pool.map(_multi_args_wrapper, args)
        pool.close()
        pool.join()
        self._log("Executed command on all nodes: {}".format(command))

        return result

    def ssh_exec_in_shards(self, command):
        self._log("Executing command on all nodes in shards: {}".format(command))
        args = [(self._single_ssh_exec, instance, command) for instance in functools.reduce(operator.iconcat, self.shards.itervalues(), [])]
        pool = Pool(ChainspaceNetwork.threads)
        result = pool.map(_multi_args_wrapper, args)
        pool.close()
        pool.join()
        self._log("Executed command on all nodes in shards: {}".format(command))

        return result

    def ssh_exec_in_clients(self, command):
        self._log("Executing command on all nodes in clients: {}".format(command))
        args = [(self._single_ssh_exec, instance, command) for instance in self.clients]
        pool = Pool(ChainspaceNetwork.threads)
        result = pool.map(_multi_args_wrapper, args)
        pool.close()
        pool.join()
        self._log("Executed command on all nodes in clients: {}".format(command))

        return result

    def ssh_close(self, type):
        self._log("Closing SSH connection on all nodes...")
        args = [(self._single_ssh_close, instance) for instance in self._get_running_instances(type)]
        pool = Pool(ChainspaceNetwork.threads)
        pool.map(_multi_args_wrapper, args)
        pool.close()
        pool.join()
        self._log("Closed SSH connection on all nodes.")

    def terminate(self, type):
        self._log("Terminating all nodes...")
        self._get_all_instances(type).terminate()
        self._log("All nodes terminated.")

    def start(self, type):
        self._log("Starting all nodes...")
        self._get_stopped_instances(type).start()
        self._log("Started all nodes.")

    def stop(self, type):
        self._log("Stopping all nodes...")
        self._get_running_instances(type).stop()
        self._log("Stopped all nodes.")

    def start_core(self):
        self._log("Starting Chainspace core on all shards...")
        args = [(self._start_shard, shard) for shard in self.shards.values()]
        pool = Pool(ChainspaceNetwork.threads)
        pool.map(_multi_args_wrapper, args)
        pool.close()
        pool.join()
        self._log("Started Chainspace core on all shards.")

    def _start_shard(self, shard):
        command = 'rm screenlog.0;'
        command += 'rm simplelog;'
        #command += 'java -cp chainspace/chainspacecore/lib/BFT-SMaRt.jar:chainspace/chainspacecore/target/chainspace-1.0-SNAPSHOT-jar-with-dependencies.jar uk.ac.ucl.cs.sec.chainspace.bft.TreeMapServer chainspace/chainspacecore/ChainSpaceConfig/config.txt &> log;'
        command = 'screen -dmS chainspacecore java -cp chainspace/chainspacecore/lib/BFT-SMaRt.jar:chainspace/chainspacecore/target/chainspace-1.0-SNAPSHOT-jar-with-dependencies.jar uk.ac.ucl.cs.sec.chainspace.bft.TreeMapServer chainspace/chainspacecore/ChainSpaceConfig/config.txt'

        for instance in shard:
                self._single_ssh_exec(instance, command)
                time.sleep(0.5)

    def stop_core(self):
        self._log("Stopping Chainspace core on all shards...")
        command = 'killall java' # hacky; should use pid file
        self.ssh_exec(command, SHARD)
        self._log("Stopping Chainspace core on all shards.")

    def uninstall_core(self, type):
        self._log("Uninstalling Chainspace core on all nodes...")
        command = 'rm -rf chainspace;'
        command += 'sudo pip uninstall -y chainspacecontract;'
        command += 'sudo pip uninstall -y chainspaceapi;'
        command += 'rm -rf contracts;'
        command += 'rm -rf config;';
        self.ssh_exec(command, type)
        self._log("Uninstalled Chainspace core on all nodes.")

    def clean_state_core(self, type):
        self._log("Resetting Chainspace core state...")
        command = ''
        command += 'rm database.sqlite;'
        command += 'rm chainspace/chainspacecore/ChainSpaceConfig/test_objects*.txt;'
        self.ssh_exec(command, type)
        self._log("Reset Chainspace core state.")

    def config_local_client(self, directory):
        os.system(self._config_shards_command(directory))

    def config_core(self, shards, nodes_per_shard):
        instances = [instance for instance in self._get_running_instances(SHARD)]
        shuffled_instances = random.sample(instances, shards * nodes_per_shard)

        if shards * nodes_per_shard > len(instances):
            raise ValueError("Number of total nodes exceeds the number of running instances.")

        self.shards = {}
        for shard in range(shards):
            self.shards[shard] = shuffled_instances[shard*nodes_per_shard:(shard+1)*nodes_per_shard]

        for i, instances in enumerate(self.shards.values()):
            for j, instance in enumerate(instances):
                command = self._config_shards_command('chainspace/chainspacecore/ChainSpaceConfig')
                command += 'printf "shardConfigFile chainspace/chainspacecore/ChainSpaceConfig/shardConfig.txt\nthisShard {0}\nthisReplica {1}\n" > config.txt;'.format(i, j)
                command += 'cd ../../../;'
                command += 'rm -rf config;'
                command += 'cp -r chainspace/chainspacecore/ChainSpaceConfig/shards/s{0} config;'.format(i)
                self._single_ssh_exec(instance, command)

    def config_me(self, directory='/home/admin/chainspace/chainspacecore/ChainSpaceClientConfig'):
        return os.system(self._config_shards_command(directory))

    def config_clients(self, clients):
        instances = [instance for instance in self._get_running_instances(CLIENT)]
        if clients > len(instances):
            raise ValueError("Number of total nodes exceeds the number of running instances.")

        shuffled_instances = random.sample(instances, clients)

        self.clients = []
        for client in range(clients):
            self.clients.append(shuffled_instances[client])

        self.ssh_exec_in_clients(self._config_shards_command('/home/admin/chainspace/chainspacecore/ChainSpaceClientConfig'))

    def start_clients(self):
        command = 'cd ~/chainspace/chainspacecore;'
        command += 'rm screenlog.0;'
        command += 'rm latencylog;'
        command += 'screen -dmS clientservice ./runclientservice.sh;'
        #command += 'sh runclientservice.sh &> log;'
        command += 'cd;'
        self.ssh_exec_in_clients(command)

    def stop_clients(self):
        self._log("Stopping all Chainspace clients...")
        command = 'killall java' # hacky; should use pid file
        self.ssh_exec(command, CLIENT)
        self._log("Stopping all Chainspace clients.")

    def prepare_transactions(self, num_transactions, shardListPath, directory='/Users/srene/workspace/byzcuit'):
        print "Prepare transactions "+str(num_transactions)+" "+directory
        num_shards = str(len(self.shards))
        num_transactions = str(int(num_transactions))
        os.system('python ' + directory + '/contrib/core-tools/generate_transactions.py' + ' ' + num_shards + ' ' + directory + '/chainspacecore/ChainSpaceClientConfig/' + ' ' +shardListPath)

        transactions = open(directory + '/chainspacecore/ChainSpaceClientConfig/test_transactions.txt').read().splitlines()
        transactions_per_client = len(transactions) / len(self.clients)

        for client in self.clients:
            data = '\\n'.join([transactions.pop() for i in range(transactions_per_client)])

            #command = 'printf \'' + data + '\' > ' + directory + '/chainspacecore/ChainSpaceClientConfig/test_transactions.txt;'
            command = 'printf \'' + data + '\' > ' + '/home/admin/chainspace/chainspacecore/ChainSpaceClientConfig/test_transactions.txt;'
            self._single_ssh_exec(client, command)

    def send_transactions(self, batch_size, batch_sleep):
        command = 'python -c \'from chainspaceapi import ChainspaceClient; client = ChainspaceClient(); client.send_transactions_from_file({0}, {1})\''.format(batch_size, batch_sleep)
        self.ssh_exec_in_clients(command)

    def generate_objects(self, num_objects):
        num_objects = str(int(num_objects))
        num_shards = str(len(self.shards))
        self.ssh_exec_in_shards('python chainspace/contrib/core-tools/generate_objects.py ' + num_objects + ' ' + num_shards + ' chainspace/chainspacecore/ChainSpaceConfig/')

    def load_objects(self):
        instance = self.clients[0]
        command = 'python -c \'from chainspaceapi import ChainspaceClient; client = ChainspaceClient(); client.load_objects_from_file()\''
        self._single_ssh_exec(instance, command)

    def get_tps_set(self):
        tps_set = []
        for shard in self.shards.itervalues():
            instance = shard[0]
            tps = self._single_ssh_exec(instance, 'python chainspace/chainspacemeasurements/chainspacemeasurements/tps.py')[1]
            tps = float(tps.strip())
            tps_set.append(tps)

        return tps_set

    def get_tpsm_set(self):
        tps_set = []
        self._log("tps")
        for shard in self.shards.itervalues():
            self._log("shard")
            instance = shard[0]
            tps = self._single_ssh_exec(instance, 'python chainspace/chainspacemeasurements/chainspacemeasurements/tpsm.py')[1]
            #print "Get TPS "+tps
            tps = float(tps.strip())
            #print "Get TPS "+str(tps)
            tps_set.append(tps)

        return tps_set

    def get_r0_logs(self):
        logs = []
        for shard in self.shards.itervalues():
            instance = shard[0]
            log = self._single_ssh_exec(instance, 'cat simplelog')[1]
            logs.append(log)

        return logs

    def get_latency(self):
        latencies = []
        for instance in self.clients:
            log = self._single_ssh_exec(instance, 'cat ~/chainspace/chainspacecore/latencylog')[1]
            for line in log.splitlines():
                if line:
                    latencies.append(int(line))

        return latencies


def _multi_args_wrapper(args):
    return args[0](*args[1:])


def _safe_print(message):
    sys.stdout.write('{}\n'.format(message))


_jessie_mapping = {
    'ap-northeast-1': 'ami-dbc0bcbc',
    'ap-northeast-2': 'ami-6d8b5a03',
    'ap-south-1': 'ami-9a83f5f5',
    'ap-southeast-1': 'ami-0842e96b',
    'ap-southeast-2': 'ami-881317eb',
    'ca-central-1': 'ami-a1fe43c5',
    'eu-central-1': 'ami-5900cc36',
    'eu-west-1': 'ami-402f1a33',
    'eu-west-2': 'ami-87848ee3',
    'sa-east-1': 'ami-b256ccde',
    'us-east-1': 'ami-b14ba7a7',
    'us-east-2': 'ami-b2795cd7',
    'us-west-1': 'ami-94bdeef4',
    'us-west-2': 'ami-221ea342',
}
