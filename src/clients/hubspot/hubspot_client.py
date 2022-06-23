from src.clients.hubspot import services
from src.commons.client import Client

from hubspot import HubSpot


class HubspotClient(Client):
    def __init__(self, task=None, env=None):
        super().__init__(env)
        self.task = task
        self.api_client = HubSpot(api_key="eu1-fd74-6c90-4dc8-a93b-a1b33969e03c")

    def get(self, **_ignored):

        result = []
        if self.task.name == "contacts":
            # services.get_service("contacts")
            result = self.api_client.crm.contacts.get_all()

        if self.task.name == "companies":
            # services.get_service("companies")
            result = self.api_client.crm.companies.get_all()

        if self.task.name == "campaigns":
            pass

        if self.task.name == "events":
            db_params = self.get_request_params(self.task)[0:20]
            if db_params:
                for param in db_params:
                    tmp = self.api_client.events.events_api.get_page(
                        **param[1][0]
                    ).results

                    if tmp:
                        for t in tmp:
                            for i in param[0]:
                                for k, v in i.items():
                                    t[k] = v
                            result.append(t)
            else:
                pass

        tmp = []
        for r in result:
            tmp.append({"datas": r.to_dict()})

        return tmp
