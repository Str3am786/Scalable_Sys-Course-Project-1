from opencep.base.DataFormatter import DataFormatter, EventTypeClassifier
from datetime import datetime,timezone



class CityBikeEventTypeClassifier(EventTypeClassifier):
    # TODO
    RIDE_TYPE = "BikeTrip"

    def get_event_type(self, event_payload: dict):
        return self.RIDE_TYPE


class CitiBikeDataFormatter(DataFormatter):

    def __init__(self, event_type_classifier: EventTypeClassifier = CityBikeEventTypeClassifier()):
        super().__init__(event_type_classifier=event_type_classifier)

    def parse_event(self, raw_item):
        # raw_item is already a dict produced by CSVDictInputStream
        if not isinstance(raw_item, dict):
            raise TypeError("IdentityDictFormatter expects dict events.")
        return raw_item

    def get_event_timestamp(self, event_payload: dict):
        # return a datetime (OpenCEP expects datetime object)
        ms = int(event_payload["event_time_ms"])
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


