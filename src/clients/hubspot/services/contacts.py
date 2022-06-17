class ContactsService:
    def __init__(self, task):
        self.service_name = task["url"]["service_name"]
        self.services_dict = {"get_contacts": self.get}

    def get_all_contacts(self):
        pass

    def get(self):
        result = self.services_dict[self.service_name]()
        return result
