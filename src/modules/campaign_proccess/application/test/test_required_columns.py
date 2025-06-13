import pytest
from modules.campaign_proccess.application.helpers.required_columns import build_required_columns
from modules.campaign_proccess.application.schemas.preload_camp_schema import ConfigFile


@pytest.mark.parametrize("content_tags, config_kwargs, expected", [
    # Case 1: Headers, no identifier
    (
        ["telefono"],
        {"useHeaders": True, "nameColumnDemographic": "nombre", "userIdentifier": False},
        ["telefono", "nombre"]
    ),
    # Case 2: Headers, with identifier
    (
        ["Col-3"],
        {"useHeaders": True, "nameColumnDemographic": "Col-1", "userIdentifier": True, "nameColumnIdentifier": "Col-2"},
        ["Col-1", "Col-2", "Col-3"]
    ),
    # Case 3: No headers, no identifier
    (
        ["Col-3"],
        {"useHeaders": False, "nameColumnDemographic": "Col-1", "userIdentifier": False},
        [0, 2]
    ),
    # Case 4: No headers, with identifier
    (
        ["Col-3"],
        {"useHeaders": False, "nameColumnDemographic": "Col-1", "userIdentifier": True, "nameColumnIdentifier": "Col-2"},
        [0, 1, 2]
    )
])
def test_build_required_columns(content_tags, config_kwargs, expected):
    config = ConfigFile(folder=".", file="x.csv", delimiter=",", useHeaders=config_kwargs["useHeaders"],
                        nameColumnDemographic=config_kwargs["nameColumnDemographic"],
                        userIdentifier=config_kwargs.get("userIdentifier", False),
                        nameColumnIdentifier=config_kwargs.get("nameColumnIdentifier", ""),
                        fileRecords=100)

    result = build_required_columns(content_tags, config)
    
    # Si es un set o lista sin orden
    assert sorted(result) == sorted(expected)
