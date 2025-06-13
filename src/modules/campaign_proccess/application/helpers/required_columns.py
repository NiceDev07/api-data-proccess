from modules.campaign_proccess.application.schemas.preload_camp_schema import ConfigFile 
from typing import List, Union

def build_required_columns(content_tags: list[str], config: ConfigFile) -> Union[List[str], List[int]]:
    columns = set(content_tags + [config.nameColumnDemographic])
    if config.userIdentifier:
        columns.add(config.nameColumnIdentifier)


    if not config.useHeaders:
        columns = {int(col.split("-")[1]) - 1 for col in columns}

    return list(columns)



