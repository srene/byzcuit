import requests

class ChainspaceClient(object):
    def __init__(self, host='127.0.0.1', port=5000):
        self.host = host
        self.port = port

    @property
    def url(self):
        return 'http://{}:{}'.format(self.host, self.port)

    def dump_transaction(self, transaction):
        endpoint = self.url + '/api/1.0/transaction/dump'
        r = requests.post(endpoint, json=transaction)
        return r

    def load_objects_from_file(self):
        endpoint = self.url + '/api/1.0/load_objects_from_file'
        r = requests.get(endpoint)
        return r

    def send_transactions_from_file(self ,batch_size, batch_sleep):
        endpoint = self.url + '/api/1.0/send_transactions_from_file?batch_size={0}&batch_sleep={1}'.format(batch_size, batch_sleep)
        r = requests.get(endpoint)
        return r
