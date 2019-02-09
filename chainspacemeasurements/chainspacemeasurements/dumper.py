"""Transaction dumper."""
import time


def simulation_batched(network, inputs_per_tx, outputs_per_tx, num_transactions=None, batch_size=2000, batch_sleep=1):
    if num_transactions is None:
        num_transactions = len(network.shards)*3000
    network.generate_objects(num_transactions*inputs_per_tx*5)
    network.load_objects()
    time.sleep(5)

    network.prepare_transactions(num_transactions, inputs_per_tx, outputs_per_tx)
    network.send_transactions(batch_size / (len(network.clients) / len(network.shards)), batch_sleep)
