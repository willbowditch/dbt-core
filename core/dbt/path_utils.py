import os
import argparse
from pathlib import Path
from typing import Dict, Any

from dbt.exceptions import DbtProjectError
from dbt.clients.system import load_file_contents
from dbt.clients.yaml_helper import load_yaml_text
from dbt.clients.system import path_exists
from dbt.exceptions import (
    NotImplementedException, CompilationException, RuntimeException,
    InternalException
)

from dbt.flags import PROFILES_DIR
# from dbt.main import parse_args
# from dbt.task.base import get_nearest_project_dir


#TODO: update this for profiles_dir instead of project_root, --config-dir, --profiles-dir
#TODO: pick a hierarchy of profiles_dir, config_dir, DBT_PROFILES_DIR, project_root,
#TODO: DEFAULT_PROFILES_DIR is duplicated across flags.py and profile.py, helpful to note we duplicate as needed
# import a method from main.py to capture the project root from PROFILES_DIR being redefined
# 
# --output is a flag used for the freshness command and should be ignored?
# args = sys.argv[1:]
# print(args)
# parsed = parse_args(args)
# get the argument from the command line as needed
# copy over get_nearest_project_dir?
# pass in --project-dir flag and then default to current working directory

class PathArgsParser:
  def path_args_subparser(self):
      base_subparser = argparse.ArgumentParser(add_help=False)

      base_subparser.add_argument(
          '--project-dir',
          default=None,
          dest='project_dir',
          type=str,
          help='''
          Which directory to look in for the dbt_project.yml file.
          Default is the current working directory and its parents.
          '''
      )
      return base_subparser


  def get_nearest_project_dir(self):
      base_subparser = self.path_args_subparser()
      # If the user provides an explicit project directory, use that
      # but don't look at parent directories.
      project_dir = getattr(base_subparser, 'project_dir', None)
      print(f"project_dir: {project_dir}")
      if project_dir is not None:
          project_file = os.path.join(project_dir, "dbt_project.yml")
          if os.path.exists(project_file):
              return project_dir
          else:
              raise RuntimeException(
                  "fatal: Invalid --project-dir flag. Not a dbt project. "
                  "Missing dbt_project.yml file"
              )

      root_path = os.path.abspath(os.sep)
      cwd = os.getcwd()

      while cwd != root_path:
          project_file = os.path.join(cwd, "dbt_project.yml")
          if os.path.exists(project_file):
              return cwd
          cwd = os.path.dirname(cwd)

      raise RuntimeException(
          "fatal: Not a dbt project (or any of the parent directories). "
          "Missing dbt_project.yml file"
      )


PathArgs = PathArgsParser()
PathArgs.get_nearest_project_dir()

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