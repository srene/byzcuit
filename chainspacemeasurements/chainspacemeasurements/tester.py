import os
import operator
import time
import sys
import traceback
import json

from chainspacemeasurements import dumper
from chainspacemeasurements.instances import ChainspaceNetwork, SHARD
from chainspacemeasurements.dumpparser import parse_tcpdump
from os import walk

def parse_client_simplelog(filename):
    data = open(filename).readlines()[2:]
    txes = {}
    for line in data:
        record = line.split()
        txes[record[1]] = int(record[0])

    return txes


class Tester(object):
    def __init__(self, network, core_directory='/home/sergi/workspace/byzcuit/chainspacecore', tpsfile='tps',latencyfile='lat'):
        self.tpsfh = open(tpsfile, 'w')
        self.latfh = open(latencyfile, 'w')
        self.core_directory = core_directory
        self.network = network

        network.logging = True

        network.ssh_connect(0)
        network.ssh_connect(1)

        # freshen state
        self.stop_tcpdump()
        self.stop_clients()
        network.stop_core()
        time.sleep(2)
        network.clean_state_core(SHARD)

    def start_clients(self):
        self.network.config_clients(len(self.network.shards)*8)
        n.start_clients()

    def stop_clients(self):
        n.stop_clients()

    def start_tcpdump(self):
        os.system('sudo rm ' + self.core_directory + '/tcpdump_log')
        os.system('screen -dmS tcpdump bash -c "sudo tcpdump -i eth0 -A -tt | grep \'\' > ' + self.core_directory + '/tcpdump_log"')

    def stop_tcpdump(self):
        os.system('sudo killall tcpdump')

    def measure_client_latency(self, shards, inputs, outputs, min_batch, max_batch, batch_step, runs, defences=False):
        if defences:
            create_dummy_objects = 1
        else:
            create_dummy_objects = 0
        latency_times_set_set = []

        for batch_size in range(min_batch, max_batch+1, batch_step):
            latency_times_set = []
            for i in range(runs):
                print "Running client latency measurements for batch size {0} (run {1}).".format(batch_size, i)

                num_transactions = max_batch*3

                self.network.config_core(shards, 4)
                self.network.config_me(self.core_directory + '/ChainSpaceClientConfig')
                self.network.start_core()
                time.sleep(10)
                self.start_clients()
                time.sleep(10)
                dumper.simulation_batched(self.network, inputs_per_tx=inputs, outputs_per_tx=outputs, batch_size=batch_size, batch_sleep=1, num_transactions=num_transactions, create_dummy_objects=create_dummy_objects)
                time.sleep(20)
                self.stop_clients()
                self.network.stop_core()
                time.sleep(2)
                self.network.clean_state_core(SHARD)

                latency_times = self.network.get_latency()

                latency_times_set.append(latency_times)
                print latency_times

            latency_times_set_set.append(latency_times_set)

        self.tpsfh.write(json.dumps(latency_times_set_set))
        return latency_times_set_set

    def measure_shard_scaling(self, min_shards, max_shards, runs, inputs_per_tx=1, outputs_per_tx=0, defences=False):
        if defences:
            create_dummy_objects = 1
        else:
            create_dummy_objects = 0
        tps_sets_sets = []
        for num_shards in range(min_shards, max_shards+1):
            tps_sets = []

            for i in range(runs):
                try:
                    print "Running measurements for {0} shards (run {1}).".format(num_shards, i)
                    self.network.config_core(num_shards, 4)
                    self.network.config_me(self.core_directory + '/ChainSpaceClientConfig')
                    self.network.start_core()

                    time.sleep(10)
                    self.start_clients()
                    time.sleep(10)
                    dumper.simulation_batched(self.network, inputs_per_tx, outputs_per_tx, create_dummy_objects=create_dummy_objects)
                    time.sleep(20)
                    self.stop_clients()

                    tps_set = self.network.get_tpsm_set()
                    tps_sets.append(tps_set)
                    print "Result for {0} shards (run {1}): {2}".format(num_shards, i, tps_set)
                except Exception:
                    traceback.print_exc()
                finally:
                    try:
                        self.network.stop_core()
                        time.sleep(2)
                        self.network.clean_state_core(SHARD)
                    except:
                        # reset connection
                        for i in range(5):
                            try:
                                self.network.ssh_close()
                                self.network.ssh_connect()
                                self.network.stop_core()
                                time.sleep(2)
                                self.network.clean_state_core(SHARD)
                                break
                            except:
                                time.sleep(5)

            tps_sets_sets.append(tps_sets)

        self.tpsfh.write(json.dumps(tps_sets_sets))
        return tps_sets_sets

    def measure_node_scaling(self, num_shards, min_nodes, max_nodes, runs, step=1):
        tps_sets_sets = []
        for num_nodes in range(min_nodes, max_nodes+1, step):
            tps_sets = []

            for i in range(runs):
                try:
                    print "Running measurements for {2} nodes in {0} shards (run {1}).".format(num_shards, i, num_nodes)
                    self.network.config_core(num_shards, num_nodes)
                    self.network.config_me(self.core_directory + '/ChainSpaceClientConfig')
                    self.network.start_core()

                    time.sleep(10)
                    self.start_clients()
                    time.sleep(10)
                    dumper.simulation_batched(self.network, 1, 0)
                    time.sleep(20)
                    self.stop_clients()

                    tps_set = self.network.get_tps_set()
                    tps_sets.append(tps_set)
                    print "Result for {3} nodes in {0} shards (run {1}): {2}".format(num_shards, i, tps_set, num_nodes)
                except Exception:
                    traceback.print_exc()
                finally:
                    try:
                        self.network.stop_core()
                        time.sleep(2)
                        self.network.clean_state_core(SHARD)
                    except:
                        # reset connection
                        for i in range(5):
                            try:
                                self.network.ssh_close()
                                self.network.ssh_connect()
                                self.network.stop_core()
                                time.sleep(2)
                                self.network.clean_state_core(SHARD)
                                break
                            except:
                                time.sleep(5)

            tps_sets_sets.append(tps_sets)

        self.tpsfh.write(json.dumps(tps_sets_sets))
        return tps_sets_sets

    def measure_input_scaling(self, num_shards, min_inputs, max_inputs, num_outputs, runs, case=None, defences=False):
        if defences:
            create_dummy_objects = 1
        else:
            create_dummy_objects = 0
        tps_sets_sets = []
        for num_inputs in range(min_inputs, max_inputs+1):
            tps_sets = []

            if case is None:
                shards_per_tx = None
            elif case == 'best':
                shards_per_tx = 1
            elif case == 'worst':
                shards_per_tx = num_shards

            for i in range(runs):
                try:
                    print "Running measurements for {2} inputs across {0} shards (run {1}).".format(num_shards, i, num_inputs)
                    self.network.config_core(num_shards, 4)
                    self.network.config_me(self.core_directory + '/ChainSpaceClientConfig')
                    self.network.start_core()

                    time.sleep(10)
                    self.start_clients()
                    time.sleep(10)
                    dumper.simulation_batched(self.network, num_inputs, num_outputs, create_dummy_objects=create_dummy_objects)
                    time.sleep(20)
                    self.stop_clients()

                    tps_set = self.network.get_tpsm_set()
                    tps_sets.append(tps_set)
                    print "Result for {0} shards (run {1}): {2}".format(num_shards, i, tps_set)
                except Exception:
                    traceback.print_exc()
                finally:
                    try:
                        self.network.stop_core()
                        time.sleep(2)
                        self.network.clean_state_core(SHARD)
                    except:
                        # reset connection
                        for i in range(5):
                            try:
                                self.network.ssh_close()
                                self.network.ssh_connect()
                                self.network.stop_core()
                                time.sleep(2)
                                self.network.clean_state_core(SHARD)
                                break
                            except:
                                time.sleep(5)

            tps_sets_sets.append(tps_sets)

        self.tpsfh.write(json.dumps(tps_sets_sets))
        return tps_sets_sets

    def measure_bano(self, num_shards, runs):
        tps_sets_sets = []
        for num_dummies in range(1, num_shards):
            tps_sets = []

            for i in range(runs):
                try:
                    print "Running measurements for {2} dummy objects across {0} shards (run {1}).".format(num_shards, i, num_dummies)
                    print "config core"
                    self.network.config_core(num_shards, 4)
                    print "config me"
                    self.network.config_me(self.core_directory + '/ChainSpaceClientConfig')
                    print "start core"
                    self.network.start_core()

                    time.sleep(10)
                    print "start clients"
                    self.start_clients()
                    time.sleep(10)
                    print "start simulation"
                    dumper.simulation_batched(self.network, 1, 1, create_dummy_objects=1, num_dummy_objects=num_dummies, output_object_mode=-1)
                    print "simulation done"
                    time.sleep(20)
                    print "stop clients"
                    self.stop_clients()

                    tps_set = self.network.get_tpsm_set()
                    tps_sets.append(tps_set)
                    print "Result for {0} dummy objects (run {1}): {2}".format(num_dummies, i, tps_set)
                except Exception:
                    traceback.print_exc()
                finally:
                    try:
                        self.network.stop_core()
                        time.sleep(2)
                        self.network.clean_state_core(SHARD)
                        self.network.ssh_close(1)
                        self.network.ssh_close(1)

                    except:
                        # reset connection
                        for i in range(5):
                            try:
                                self.network.ssh_close()
                                self.network.ssh_connect()
                                self.network.stop_core()
                                time.sleep(2)
                                self.network.clean_state_core(SHARD)
                                break
                            except:
                                time.sleep(5)

            tps_sets_sets.append(tps_sets)

        self.tpsfh.write(json.dumps(tps_sets_sets))
        return tps_sets_sets

    def measure_sharding(self, min_validators, max_validators, num_shards, runs,shardListPath):

        tps_sets_sets = []
        latency_times_sets_sets = []

        for validators in range(min_validators,max_validators+1):
            print "Start test validators "+str(validators)
#            for num_dummies in range(1, num_shards):
            tps_sets = []
            latency_times_sets = []
#            for i in range(runs):
            file_list = os.listdir(shardListPath)
#            for r,d,f in walk (shardListPath):
            for file in sorted(file_list):
                try:
    #                    print "Running measurements for {2} dummy objects across {0} shards (run {1}).".format(num_shards, i, num_dummies)
    #                self.network.ssh_connect(0)
    #                self.network.ssh_connect(1)
                    print "config core"
                    self.network.config_core(num_shards, validators)
                    print "config me"
                    self.network.config_me(self.core_directory + '/ChainSpaceClientConfig')
                    print "start core"
                    self.network.start_core()

    #                time.sleep(10)
                    print "start clients"
                    self.start_clients()
                    time.sleep(10)
                    print "start simulation"
    #                print shardListPath+"/"+file
    #                print len(open(shardListPath+"/"+file).readlines())
                    dumper.simulation_batched(self.network,len(open(shardListPath).readlines()),shardListPath)
    #                dumper.simulation_batched(self.network,len(open(shardListPath+"/"+file).readlines()), shardListPath+"/"+file)
                    print "simulation done"
                    time.sleep(20)
                    print "stop clients"
                    self.stop_clients()

    #                log = self.network.get_r0_logs()
    #                print log
                    tps_set = self.network.get_tpsm_set()
                    tps_set_avg = sum(tps_set) / len(tps_set)
                    #print "Avg set "+str(tps_set_avg)
                    tps_sets.append(tps_set_avg)

                    latency_times = self.network.get_latency()
                    latency_times_avg = sum(latency_times) / len(latency_times)
                    #print "Latency: {0}".format(latency_times)
                    latency_times_sets.append(latency_times_avg)

                    #print "Result for {0} dummy objects (run {1}): {2}".format(num_dummies, i, tps_set)
                    #print "Result for {0} dummy objects (run {1}): {2}".format(num_dummies, i, latency_times_set)
                except Exception:
                    traceback.print_exc()
                finally:
                    try:
                        self.network.stop_core()
                        time.sleep(2)
                        self.network.clean_state_core(SHARD)
                        self.network.ssh_close(0)
                        self.network.ssh_close(1)
    #                    time.sleep(10)
                    except:
                        # reset connection
    #                            for i in range(5):
                        try:
                            self.network.ssh_close()
                            self.network.ssh_connect()
                            self.network.stop_core()
                            time.sleep(2)
                            self.network.clean_state_core(SHARD)
                            break
                        except:
                            time.sleep(5)

                tps_sets_avg = sum(tps_sets) / len(tps_sets)
                tps_sets_sets.append(tps_sets_avg)
                latency_times_sets_avg = sum(latency_times_sets) / len(latency_times_sets)
                latency_times_sets_sets.append(latency_times_sets_avg)

                for x in tps_sets_sets:
                    print "TPS "+str(x)

        self.tpsfh.write(json.dumps(tps_sets_sets))
        self.latfh.write(json.dumps(latency_times_sets_sets))
        return tps_sets_sets

if __name__ == '__main__':
    if sys.argv[1] == 'shardscaling':
        min_shards = int(sys.argv[2])
        max_shards = int(sys.argv[3])
        runs = int(sys.argv[4])
        outfile = sys.argv[5]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_shard_scaling(min_shards, max_shards, runs)
    elif sys.argv[1] == 'shardscaling_mi':
        inputs_per_tx = int(sys.argv[2])
        min_shards = int(sys.argv[3])
        max_shards = int(sys.argv[4])
        runs = int(sys.argv[5])
        outfile = sys.argv[6]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_shard_scaling(min_shards, max_shards, runs, inputs_per_tx)
    elif sys.argv[1] == 'shardscaling_mico':
        inputs_per_tx = int(sys.argv[2])
        outputs_per_tx = int(sys.argv[3])
        min_shards = int(sys.argv[4])
        max_shards = int(sys.argv[5])
        runs = int(sys.argv[6])
        outfile = sys.argv[7]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_shard_scaling(min_shards, max_shards, runs, inputs_per_tx, outputs_per_tx)
    elif sys.argv[1] == 'shardscaling_micod':
        inputs_per_tx = int(sys.argv[2])
        outputs_per_tx = int(sys.argv[3])
        min_shards = int(sys.argv[4])
        max_shards = int(sys.argv[5])
        runs = int(sys.argv[6])
        outfile = sys.argv[7]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_shard_scaling(min_shards, max_shards, runs, inputs_per_tx, outputs_per_tx, defences=True)
    elif sys.argv[1] == 'inputscaling':
        num_shards = int(sys.argv[2])
        min_inputs = int(sys.argv[3])
        max_inputs = int(sys.argv[4])
        num_outputs = int(sys.argv[5])
        runs = int(sys.argv[6])
        outfile = sys.argv[7]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_input_scaling(num_shards, min_inputs, max_inputs, num_outputs, runs)
    elif sys.argv[1] == 'inputscaling_d':
        num_shards = int(sys.argv[2])
        min_inputs = int(sys.argv[3])
        max_inputs = int(sys.argv[4])
        num_outputs = int(sys.argv[5])
        runs = int(sys.argv[6])
        outfile = sys.argv[7]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_input_scaling(num_shards, min_inputs, max_inputs, num_outputs, runs, defences=True)
    elif sys.argv[1] == 'inputscaling_f':
        num_shards = int(sys.argv[2])
        min_inputs = int(sys.argv[3])
        max_inputs = int(sys.argv[4])
        case = sys.argv[5]
        runs = int(sys.argv[6])
        outfile = sys.argv[7]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_input_scaling(num_shards, min_inputs, max_inputs, runs, case=case)
    elif sys.argv[1] == 'nodescaling':
        num_shards = int(sys.argv[2])
        min_nodes = int(sys.argv[3])
        max_nodes = int(sys.argv[4])
        step = int(sys.argv[5])
        runs = int(sys.argv[6])
        outfile = sys.argv[7]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_node_scaling(num_shards, min_nodes, max_nodes, runs, step=step)
    elif sys.argv[1] == 'clientlatency':
        shards = int(sys.argv[2])
        inputs = int(sys.argv[3])
        outputs = int(sys.argv[4])
        min_batch = int(sys.argv[5])
        max_batch = int(sys.argv[6])
        batch_step = int(sys.argv[7])
        runs = int(sys.argv[8])
        outfile = sys.argv[9]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_client_latency(shards, inputs, outputs, min_batch, max_batch, batch_step, runs)
    elif sys.argv[1] == 'clientlatency_d':
        shards = int(sys.argv[2])
        inputs = int(sys.argv[3])
        outputs = int(sys.argv[4])
        min_batch = int(sys.argv[5])
        max_batch = int(sys.argv[6])
        batch_step = int(sys.argv[7])
        runs = int(sys.argv[8])
        outfile = sys.argv[9]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_client_latency(shards, inputs, outputs, min_batch, max_batch, batch_step, runs, defences=True)
    elif sys.argv[1] == 'bano':
        num_shards = int(sys.argv[2])
        runs = int(sys.argv[3])
        outfile = sys.argv[4]

        n = ChainspaceNetwork(0)
        t = Tester(n, outfile=outfile)

        print t.measure_bano(num_shards, runs)


    elif sys.argv[1] == 'sharding_measurements':
        min_validators = int(sys.argv[2])
        max_validators = int(sys.argv[3])
#        num_transactions = int(sys.argv[4])
        num_shards = int(sys.argv[4])
        runs = int(sys.argv[5])
        shardListPath = sys.argv[6]
        tpsfile = sys.argv[7]
        latfile = sys.argv[8]
        n = ChainspaceNetwork(0)
        t = Tester(n, tpsfile=tpsfile,latencyfile=latfile)

        print t.measure_sharding(min_validators, max_validators, num_shards, runs, shardListPath)
