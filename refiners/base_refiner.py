import json


class BaseRefiner:
    def refine(self, raw_data):
        pass

    @staticmethod
    def str_to_dict(json_string):
        return json.loads(json_string)