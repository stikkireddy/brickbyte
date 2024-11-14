import os
import subprocess
import shutil
import virtualenv
from typing import Optional, List, Dict

from brickbyte.types import Source, Destination


class VirtualEnvManager:
    def __init__(self, env_dir, libraries: List[str] = None):
        self.env_dir = env_dir
        self.libraries = libraries

    def create_virtualenv(self):
        virtualenv.cli_run([self.env_dir])
        print(f"Virtual environment created at {self.env_dir}")

    def install_library(self, library=None):
        library_to_install = [library] or self.libraries
        if not library_to_install:
            raise ValueError("No library specified for installation")
        subprocess.check_call([os.path.join(self.env_dir, 'bin', 'pip'), 'install', *library_to_install])
        print(f"Libraries {library_to_install} installed in virtual environment")

    def install_airbyte_source(self, source: str,
                               override_install: Optional[str] = None):
        if override_install:
            library_to_install = override_install
        else:
            library_to_install = f"airbyte-{source}"
        if not library_to_install:
            raise ValueError("No library specified for installation")
        subprocess.check_call([os.path.join(self.env_dir, 'bin', 'pip'), 'install', library_to_install])
        print(f"Library {library_to_install} installed in virtual environment")
        if not override_install:
            return os.path.join(self.bin_path, source)

    def install_airbyte_from_file(self, path: str):
        subprocess.check_call([os.path.join(self.env_dir, 'bin', 'pip'), 'install', path])
        print(f"Library {path} installed in virtual environment")

    def delete_virtualenv(self):
        shutil.rmtree(self.env_dir)
        print(f"Virtual environment at {self.env_dir} deleted")

    @property
    def bin_path(self):
        return os.path.join(self.env_dir, 'bin')

    def __enter__(self):
        self.create_virtualenv()
        if self.library:
            self.install_library()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.delete_virtualenv()


class BrickByte:

    def __init__(self, sources: List[Source],
                 destination: Destination,
                 sources_install: Optional[Dict[str, str]] = None,
                 destination_install: Optional[str] = None):
        self._sources = sources
        self._destination = destination
        self._sources_install = sources_install
        self._destination_install = destination_install
        self._source_env_managers: Dict[str, VirtualEnvManager] = {}
        self._destination_env_manager: Optional[VirtualEnvManager] = None
        assert len(sources) > 0, "At least one source should be provided"

    def setup(self):
        for source in self._sources:
            source_manager = VirtualEnvManager(f'/tmp/brickbyte-{source}')
            source_manager.create_virtualenv()
            source_manager.install_airbyte_source(source, self._sources_install.get(source))
            self._source_env_managers[source] = source_manager

        destination_env_manager = VirtualEnvManager(f'/tmp/brickbyte-destination-databricks')
        destination_env_manager.create_virtualenv()
        destination_env_manager.install_airbyte_from_file(self._destination_install)
        self._destination_env_manager = destination_env_manager

    def get_or_create_cache(self):
        try:
            import airbyte as ab
        except ImportError:
            print("please install airbyte it is not installed")
        return ab.new_local_cache(cache_name="brickbyte", cache_dir="/tmp/brickbyte/cache/", cleanup=True)

    def source_exec_path(self, source: Source):
        bin_path = self._source_env_managers[source].bin_path
        return os.path.join(bin_path, source)

    def destination_exec_path(self):
        return os.path.join(self._destination_env_manager.bin_path, "destination-databricks")

    def cleanup(self):
        for source_manager in self._source_env_managers.values():
            source_manager.delete_virtualenv()
        self._destination_env_manager.delete_virtualenv()
