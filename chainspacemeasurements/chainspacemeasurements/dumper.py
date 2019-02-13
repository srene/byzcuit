"""Transaction dumper."""
import time


def simulation_batched(network, inputs_per_tx, outputs_per_tx, num_transactions=None, batch_size=4000, batch_sleep=1, input_object_mode=0, create_dummy_objects=0, num_dummy_objects=0, output_object_mode=0):
    if num_transactions is None:
        num_transactions = len(network.shards)*6000
    network.generate_objects(num_transactions*inputs_per_tx*5)
    network.load_objects()
    time.sleep(5)

    network.prepare_transactions(num_transactions, inputs_per_tx, outputs_per_tx, input_object_mode=input_object_mode, create_dummy_objects=create_dummy_objects, num_dummy_objects=num_dummy_objects, output_object_mode=output_object_mode)
    network.send_transactions(batch_size / (len(network.clients) / len(network.shards)), batch_sleep)
