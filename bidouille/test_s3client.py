from datetime import datetime

from src.commons.application import Application
from src.commons.client import S3Client

# pytest should be run like this: PYTHONPATH=. python -m pytest


class FakeTask:
    def __init__(self):
        self.name = "test_task_name"


RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES = [
    ("application", "ingest"),
    ("environment", "test"),
    ("task", "test_task_name"),
]

RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES_WITH_DATE = (
    RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES
    + [
        ("year", "2012"),
        ("month", "09"),
        ("day", "26"),
    ]
)


def test_upload_data(monkeypatch):
    app = Application("ingest")
    s3_client = S3Client(application=app, env="test")
    monkeypatch.setattr(s3_client, "task", FakeTask())
    result = s3_client.upload_data("lililililililililililililili")
    assert result is True

    # result = s3_client.upload_data("dede")
    # assert result is False


# def test_get_object_key_path_elements(monkeypatch):

#     app = Application("ingest")
#     s3_client = S3Client(application=app, env="test")
#     monkeypatch.setattr(s3_client, "task", FakeTask())

#     # Without date arg.
#     result = s3_client.get_object_key_path_elements()
#     assert result == RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES

#     # With date arg.
#     dt = datetime.strptime("26 Sep 2012", "%d %b %Y")
#     s3_client.date = dt
#     result = s3_client.get_object_key_path_elements()
#     assert result == RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES_WITH_DATE


# def test_get_object_key(monkeypatch):
#     app = Application("ingest")
#     s3_client = S3Client(application=app, env="test")

#     # Without date
#     monkeypatch.setattr(s3_client, "task", FakeTask())
#     monkeypatch.setattr(
#         s3_client,
#         "get_object_key_path_elements",
#         lambda: RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES,
#     )
#     result = s3_client.get_object_key()
#     assert (
#         result
#         == "application=ingest/environment=test/task=test_task_name/ingest--test--test_task_name.csv"
#     )

#     monkeypatch.setattr(s3_client, "task", FakeTask())
#     monkeypatch.setattr(
#         s3_client,
#         "get_object_key_path_elements",
#         lambda: RESULT_FOR_OBJECT_NAME_ELEMENTS_AS_LIST_OF_TUPLES_WITH_DATE,
#     )

#     result = s3_client.get_object_key()
#     assert (
#         result
#         == "application=ingest/environment=test/task=test_task_name/year=2012/month=09/day=26/ingest--test--test_task_name--2012--09--26.csv"
#     )
