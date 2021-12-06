from pathlib import Path
from .graph.manifest import WritableManifest

from .results import RunResultsArtifact
from .results import FreshnessExecutionResultArtifact
from typing import Optional

from dbt.exceptions import IncompatibleSchemaException

from dbt.path_utils import PathUtils

class PreviousState:
    def __init__(self, path: Path):
        self.path: Path = path
        self.manifest: Optional[WritableManifest] = None
        self.results: Optional[RunResultsArtifact] = None
        self.sources: Optional[FreshnessExecutionResultArtifact] = None
        self.previous_sources: Optional[FreshnessExecutionResultArtifact] = None

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
        
        previous_sources_path = self.path / 'historical' / 'sources.json'
        if previous_sources_path.exists() and previous_sources_path.is_file():
            try:
                self.previous_sources = FreshnessExecutionResultArtifact.read(str(sources_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(previous_sources_path))
                raise

# bring in the project class that needs to be instantiated
# define the target path at this step(how do I consider a different target path? based on dbt_project.yml)
# the problem I'm facing right now is that the project config is populated AFTER this step
# the reason this works with previous state is that we manually set the previous state path
# current state is more difficult because we have to read the project config first before this class is instantiated
class CurrentState(PathUtils):
    def __init__(self):
        super().__init__()
        self.sources: Optional[FreshnessExecutionResultArtifact] = None

        target_path = self.get_target_path()
        sources_path = target_path / 'sources.json'

        if sources_path.exists() and sources_path.is_file():
            try:
                self.sources = FreshnessExecutionResultArtifact.read(str(sources_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(sources_path))
                raise
