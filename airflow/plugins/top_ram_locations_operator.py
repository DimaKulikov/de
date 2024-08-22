from airflow.models.baseoperator import BaseOperator
import requests


class GetTopLocationsOperator(BaseOperator):
    """
    Loads top 3 locations by number of residents from
    Rick and Morty api
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        results = []
        url = 'https://rickandmortyapi.com/api/location'
        while url:
            response = self.get_response(url)
            results.extend(response['results'])
            url = response['info']['next']
        return self.get_top_locations(results)

    @staticmethod
    def get_response(url) -> dict:
        """
        Runs a get request, returns parsed json, raises for status
        """
        r = requests.get(url)
        r.raise_for_status()
        if r.status_code == 200:
            return r.json()

    @staticmethod
    def get_top_locations(locations_list: list) -> list:
        """
        Returns top 3 locations from passed list of
        locations sorted by number of residents
        """
        return sorted(locations_list, key=lambda x: len(x['residents']))[-3:]
