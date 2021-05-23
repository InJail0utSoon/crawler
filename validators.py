class Validator:

    def __init__(self):

        self.mandatory_params = []
        self.config_key = None

    def is_valid(self, app_config):
        if self.config_key not in app_config.keys():
            return False
        for param in self.mandatory_params:
            if param not in app_config[self.config_key].keys():
                return False
        return True


class MongoDBConfigValidator(Validator):

    def __init__(self):

        self.mandatory_params = ['db_server_address', 'db_port']
        self.config_key = 'mongo'


class ESConfigValidator(Validator):

    def __init__(self):

        self.mandatory_params = ['db_server_address', 'db_port', 'verify_certs', 'index']
        self.config_key = 'es'

class KafkaConfigValidator(Validator):

    def __init__(self):

        self.mandatory_params = ['concerned_topic', 'kafka_broker_url']
        self.config_key = 'kafka'
