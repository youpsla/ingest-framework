import os
import sys
from pathlib import Path

import pytest

module_path = Path(__file__).parent.parent
sys.path.append(os.path.join(module_path))

from src.commons.application import Application  # noqa: E402


def test_is_valid_app_name():
    print("dadadadad")
    app = Application("test_name")

    # An AttributeError Exception has to be raised if app_name is not in APPLICATIONS_LIST # noqa: E501
    with pytest.raises(AttributeError):
        app.is_valid_app_name()

    # If app_name is in APPLICATIONS_LIST, should return True
    app = Application("ingest")
    assert app.is_valid_app_name() is True
