"""Transaction dumper."""
import time

from chainspaceapi import ChainspaceClient

client = ChainspaceClient()


def simulation_batched(network, num_transactions, inputs_per_tx, outputs_per_tx, batch_size=100, batch_sleep=2):
    network.generate_objects(num_transactions*inputs_per_tx*5)
    client.load_objects_from_file()
    time.sleep(5)

    network.generate_transactions(num_transactions, inputs_per_tx, outputs_per_tx)
    client.send_transactions_from_file()
