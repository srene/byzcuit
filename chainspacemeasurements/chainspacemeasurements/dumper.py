"""Transaction dumper."""
import time


def simulation_batched(network, num_transactions, shardListPath, batch_size=4000, batch_sleep=1):
    #if num_transactions is None:
    #    num_transactions = len(network.shards)*6000
    network.generate_objects(num_transactions*10)
    network.load_objects()
    time.sleep(5)

    network.prepare_transactions(num_transactions, shardListPath)
    network.send_transactions(batch_size / (len(network.clients) / len(network.shards)), batch_sleep)
