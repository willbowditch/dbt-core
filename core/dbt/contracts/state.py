from pathlib import Path
from .graph.manifest import WritableManifest
from .results import RunResultsArtifact
from .results import FreshnessExecutionResultArtifact
from typing import Optional, Dict, Any
import os
from dbt.exceptions import IncompatibleSchemaException, DbtProjectError
from dbt.clients.system import load_file_contents
from dbt.clients.yaml_helper import load_yaml_text
from dbt.clients.system import path_exists
from dbt.flags import PROFILES_DIR
# from dbt.task.base import get_nearest_project_dir


class PreviousState:
    def __init__(self, path: Path):
        self.path: Path = path
        self.manifest: Optional[WritableManifest] = None
        self.results: Optional[RunResultsArtifact] = None
        self.sources: Optional[FreshnessExecutionResultArtifact] = None

        manifest_path = self.path / 'manifest.json'
        if manifest_path.exists() and manifest_path.is_file():
            try:
                self.manifest = WritableManifest.read(str(manifest_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(manifest_path))
                raise

        results_path = self.path / 'run_results.json'
        if results_path.exists() and results_path.is_file():
            try:
                self.results = RunResultsArtifact.read(str(results_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(results_path))
                raise

        sources_path = self.path / 'sources.json'
        if sources_path.exists() and sources_path.is_file():
            try:
                self.sources = FreshnessExecutionResultArtifact.read(str(sources_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(sources_path))
                raise

# bring in the project class that needs to be instantiated
# define the target path at this step(how do I consider a different target path? based on dbt_project.yml)
# the problem I'm facing right now is that the project config is populated AFTER this step
# the reason this works with previous state is that we manually set the previous state path
# current state is more difficult because we have to read the project config first before this class is instantiated
class CurrentState:
    def __init__(self):
        self.project_root = PROFILES_DIR #TODO: check if this is "allowed"
        self.sources: Optional[FreshnessExecutionResultArtifact] = None

        target_path = self.get_target_path(self.project_root)
        sources_path = target_path / 'sources.json'

        if sources_path.exists() and sources_path.is_file():
            try:
                self.sources = FreshnessExecutionResultArtifact.read(str(sources_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(sources_path))
                raise

    # @staticmethod
    # def get_project_root() -> str:
    #     #TODO: update this for profiles_dir instead of project_root, --config-dir, --profiles-dir
    #     #TODO: pick a hierarchy of profiles_dir, config_dir, DBT_PROFILES_DIR, project_root,
    #     dbt_profiles_dir = os.getenv('DBT_PROFILES_DIR')
    #     if dbt_profiles_dir:
    #         project_root = dbt_profiles_dir
    #     else:
    #         project_root = os.getcwd()
    #     return project_root
    
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

    def get_target_path(self, project_root: str) -> Path:
        project_dict = self._raw_project_from(project_root)
        return Path(project_dict.get('target-path'))