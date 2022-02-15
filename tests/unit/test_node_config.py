import pytest
from dbt.contracts.graph.model_config import NodeConfig

config_dict = {'enabled': True, 'alias': None, 'schema': None, 'database': None, 'tags': [], 'meta': {}, 'materialized': 'incremental', 'persist_docs': {'relation': True}, 'quoting': {}, 'column_types': {}, 'full_refresh': None, 'unique_key': 'id', 'on_schema_change': 'ignore', 'post-hook': [], 'pre-hook': []}


@pytest.mark.xfail
def test_node_config_from_dict():
    node_obj = NodeConfig.from_dict(config_dict)
    assert node_obj.unique_key == 'id'
