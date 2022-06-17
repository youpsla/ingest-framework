from src.commons.client import Client

import services

services.get_service("contacts")


class HubspotClient(Client):
    def __init__(self, task=None, env=None):
        super().__init__(env)

    def get(self, **_ignored):

        params = self.task.params

        db_params = self.get_request_params(self.task)

        result = []
        if self.task.name in [
            "daily_user_location_metrics_update",
            "daily_demographic_metrics_update",
            "daily_geographic_metrics_update",
        ]:
            request = ReportRequest(
                authorization_data=self.authorization_data,
                service_name=self.task.params["url"]["service_name"],
                task=self.task,
                kwargs=kwargs,
            )
        else:
            request = ServiceRequest(
                authorization_data=self.authorization_data,
                service_name=self.task.params["url"]["service_name"],
                task=self.task,
                kwargs=kwargs,
            )
        if db_params:
            for param in db_params:
                request.param = param[0]
                tmp = request.get()
                if isinstance(self.task.destination, s3.s3_client.S3Client):
                    result.append(tmp)
                else:
                    if tmp:
                        for t in tmp:
                            for i in param[0]:
                                for k, v in i.items():
                                    t[k] = v
                            result.append(t)
        else:
            result = request.get()

        tmp = []
        for r in result:
            tmp.append({"datas": r, "authorization_data": self.authorization_data})

        return tmp
