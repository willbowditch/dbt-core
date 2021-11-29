import os
from pathlib import Path
from typing import Dict, Any

from dbt.exceptions import DbtProjectError
from dbt.clients.system import load_file_contents
from dbt.clients.yaml_helper import load_yaml_text
from dbt.clients.system import path_exists

from dbt.flags import PROFILES_DIR


#TODO: update this for profiles_dir instead of project_root, --config-dir, --profiles-dir
#TODO: pick a hierarchy of profiles_dir, config_dir, DBT_PROFILES_DIR, project_root,
#TODO: DEFAULT_PROFILES_DIR is duplicated across flags.py and profile.py, helpful to note we duplicate as needed

class PathUtils:
    def __init__(self):
        self.project_root = PROFILES_DIR #TODO: check if this is "allowed"

    def _load_yaml(self, path):
        contents = load_file_contents(path)
        return load_yaml_text(contents)

    def _raw_project_from(self, project_root: str) -> Dict[str, Any]:
        project_root = os.path.normpath(project_root)
        project_yaml_filepath = os.path.join(project_root, 'dbt_project.yml')

        if not path_exists(project_yaml_filepath):
            raise DbtProjectError(
                'no dbt_project.yml found at expected path {}'
                .format(project_yaml_filepath)
            )

        project_dict = self._load_yaml(project_yaml_filepath)

        if not isinstance(project_dict, dict):
            raise DbtProjectError(
                'dbt_project.yml does not parse to a dictionary'
            )
        return project_dict

    def get_target_path(self) -> Path:
        project_dict = self._raw_project_from(self.project_root)
        return Path(project_dict.get('target-path','target'))