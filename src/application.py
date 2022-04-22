import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))
from src.constants import APPLICATIONS_LIST


class Application:
    def __init__(self, app_name):
        self.app_name = app_name
        self._name = None

    # def __repr__(self):
    #     return str(self.name)

    @property
    def name(self):
        if not self._name:
            if self.is_valid_app_name():
                self._name = self.app_name
        return self._name

    def is_valid_app_name(self):
        if self.app_name not in APPLICATIONS_LIST:
            raise AttributeError(
                "Attribute 'app_name' is {}. He must be one of the following:\n {}"
                .format(self.app_name, "\n".join(["- " + i for i in APPLICATIONS_LIST]))
            )
        else:
            return True
