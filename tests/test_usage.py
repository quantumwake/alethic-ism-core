import datetime

from alethic_ism_core.core.base_model import Usage, UnitType, UnitSubType


def test_usage_entity():
    usage = Usage(
        resource_id="12345",
        resource_type="/provider/selector/id",

        #
        transaction_time=datetime.datetime.utcnow(),

        #
        unit_type=UnitType.TOKEN,
        unit_subtype=UnitSubType.INPUT,
        unit_count=123,
    )

    json_data = usage.model_dump_json(indent=2)
    print(json_data)
