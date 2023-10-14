from pydantic import Any, BaseModel, Optional
from typing import Literal


class AutoloaderConfigs(BaseModel):
    trigger_type: Literal["once", "availableNow", "stream"]
    file_detection_mode: Literal["DirectoryListing", "FileNotification"]
    use_incremental_listing: bool = True
    recursive_file_lookup: bool = True
    max_files_per_trigger: int | None  # databricks default is 1000
    max_bytes_per_trigger: int | None = 10000000000  # set your default here
    path_glob_filter: str = "*"


class PipelineMetadata(BaseModel):
    operation_id: int
    pk_column: str
    column_mapping: str
    autoloader_configs: Optional(AutoloaderConfigs)


class PipelineConfigs(BaseModel):
    metadata: PipelineMetadata
    result: Any
    dry_run: bool = True
