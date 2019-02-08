"""Transaction dumper."""
import time


def simulation_batched(network, num_transactions, inputs_per_tx, outputs_per_tx, batch_size=100, batch_sleep=2):
    network.generate_objects(num_transactions*inputs_per_tx*5)
    network.load_objects()
    time.sleep(5)

    network.prepare_transactions(num_transactions, inputs_per_tx, outputs_per_tx)
    network.send_transactions()
