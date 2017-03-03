# -*- coding: utf-8 -*-
from openprocurement_client.client import APIBaseClient


class APIClient(APIBaseClient):
    def get_resource_dump(self, id, resource):
        prefix_path = self.prefix_path.replace('RESOURCE', resource)
        return self._get_resource_item('{}/{}/dump'.format(prefix_path, id))

    def delete_resource_dump(self, id, resource):
        prefix_path = self.prefix_path.replace('RESOURCE', resource)
        return self._delete_resource_item('{}/{}/dump'.format(prefix_path, id))
