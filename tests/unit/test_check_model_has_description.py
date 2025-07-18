from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from dbt_checkpoint.check_model_has_description import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        ["aa/bb/with_description.sql"],
        {"models": [{"name": "with_description", "description": "test description"}]},
        True,
        True,
        False,  # only_changed_sql
        0,
    ),
    (
        ["aa/bb/with_description.sql"],
        {"models": [{"name": "with_description", "description": "test description"}]},
        False,
        True,
        False,
        1,
    ),
    (
        ["aa/bb/without_description.sql"],
        {"models": [{"name": "without_description"}]},
        True,
        False,
        False,
        1,
    ),
    # New tests for only_changed_sql=True
    (
        ["aa/bb/changed.sql"],
        {"models": [
            {"name": "changed", "description": "test", "original_file_path": "aa/bb/changed.sql"},
            {"name": "unchanged", "description": "test", "original_file_path": "aa/bb/unchanged.sql"}
        ]},
        True,
        True,
        True,
        0
    ),
    (
        ["aa/bb/changed.sql"],
        {"models": [
            {"name": "changed"},
            {"name": "unchanged", "description": "test"}
        ]},
        True,
        True,
        True,
        1
    ),
    # New tests for only_changed_sql=False
    (
        ["aa/bb/changed.sql"],
        {"models": [
            {"name": "changed"},
            {"name": "unchanged", "description": "test"}
        ]},
        True,
        True,
        False,
        1  # Fails because changed model has no description
    )
)


@pytest.mark.parametrize(
    ("input_args", "schema", "valid_manifest", "valid_config", "only_changed_sql", "expected_status_code"),
    TESTS,
)
def test_check_model_description(
    input_args,
    schema,
    valid_manifest,
    valid_config,
    only_changed_sql,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    if valid_config:
        input_args.extend(["--config", config_path_str])
    if only_changed_sql:
        input_args.append("--only-changed-sql")
        
    with patch("builtins.open", mock_open(read_data="data")):
        with patch("dbt_checkpoint.utils.safe_load") as mock_safe_load:
            # Add filename to each model to match input_args
            for model in schema.get("models", []):
                if "original_file_path" not in model:
                    model["original_file_path"] = f"aa/bb/{model['name']}.sql"
            mock_safe_load.return_value = schema
            
            # Also mock schema files for models
            with patch("dbt_checkpoint.utils.get_filenames") as mock_get_filenames:
                mock_get_filenames.return_value = {}
                
                # Mock config file to include version
                with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config:
                    mock_get_config.return_value = {"config_version": 1}
                    status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
@pytest.mark.parametrize("only_changed_sql", [True, False])
def test_check_model_description_in_changed(extension, only_changed_sql, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_desc
    description: blabla
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    argv = [
        "in_schema_desc.sql",
        str(yml_file),
        "--manifest",
        manifest_path_str,
    ]
    if only_changed_sql:
        argv.append("--only-changed-sql")
        
    result = main(argv=argv)
    assert result == 0
