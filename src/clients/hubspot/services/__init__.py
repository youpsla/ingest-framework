from importlib import import_module

services = ["src.clients.hubspot.services.contacts"]

# modules = map(__import__, services)
modules = [import_module(s, "src") for s in services]
print(modules)


def get_service(service_name):
    if service_name == "contacts":
        print("contacts found")
