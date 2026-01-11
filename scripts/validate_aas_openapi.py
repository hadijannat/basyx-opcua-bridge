import json
import os
import sys
import urllib.request
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012
from basyx.aas import model as aas_model
from basyx.aas.adapter.json import json_serialization

DEFAULT_PATHS_URL = (
    "https://raw.githubusercontent.com/admin-shell-io/aas-specs-api/main/"
    "Entire-API-Collection/V3.1.yaml"
)
DEFAULT_SCHEMAS_URL = (
    "https://raw.githubusercontent.com/admin-shell-io/aas-specs-api/main/"
    "Part1-MetaModel-Schemas/openapi.yaml"
)


def fetch_yaml(source: str, cache_path: Path) -> dict:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(source, timeout=20) as resp:
        data = resp.read()
    cache_path.write_bytes(data)
    return yaml.safe_load(data)


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _methods_for_path(paths: dict, path: str) -> set[str]:
    return {method.lower() for method in paths.get(path, {})}


def validate_paths(paths_spec: dict) -> None:
    paths = paths_spec.get("paths", {})
    missing: list[str] = []

    submodel_collection_candidates = ["/aas/submodels", "/submodels"]
    has_collection = False
    for candidate in submodel_collection_candidates:
        if {"post"}.issubset(_methods_for_path(paths, candidate)):
            has_collection = True
            break
    if not has_collection:
        missing.append("/aas/submodels or /submodels (POST)")

    for path, methods in {
        "/aas/submodels/{submodelIdentifier}": {"get"},
        "/aas/submodels/{submodelIdentifier}/submodel-elements": {"post"},
    }.items():
        available = _methods_for_path(paths, path)
        if not methods.issubset(available):
            missing.append(f"{path} missing {sorted(methods - available)}")

    value_path = "/aas/submodels/{submodelIdentifier}/submodel-elements/{idShortPath}/$value"
    available = _methods_for_path(paths, value_path)
    if not available:
        missing.append(value_path)
    else:
        if "get" not in available:
            missing.append(f"{value_path} missing ['get']")
        if not {"patch", "put"}.intersection(available):
            missing.append(f"{value_path} missing ['patch' or 'put']")

    if missing:
        raise SystemExit(f"OpenAPI paths missing: {missing}")


def validate_schemas(schema_spec: dict) -> None:
    schema_id = schema_spec.get("$id", "urn:aas-openapi")
    resource = Resource.from_contents(schema_spec, default_specification=DRAFT202012)
    registry = Registry().with_resource(schema_id, resource)
    validator = Draft202012Validator(
        {"$ref": f"{schema_id}#/components/schemas/Submodel"},
        registry=registry,
    )

    prop = aas_model.Property(
        id_short="Temperature",
        value_type=aas_model.datatypes.Double,
        value=42.0,
    )
    submodel = aas_model.Submodel(id_="urn:test:submodel", submodel_element=[prop])

    submodel_json = json.loads(json.dumps(submodel, cls=json_serialization.AASToJsonEncoder))
    validator.validate(submodel_json)


def main() -> None:
    cache_dir = Path(".cache/aas-openapi")
    paths_url = os.environ.get("AAS_OPENAPI_PATHS_URL", DEFAULT_PATHS_URL)
    schemas_url = os.environ.get("AAS_OPENAPI_SCHEMAS_URL", DEFAULT_SCHEMAS_URL)

    paths_path = Path(os.environ.get("AAS_OPENAPI_PATHS_PATH", ""))
    schemas_path = Path(os.environ.get("AAS_OPENAPI_SCHEMAS_PATH", ""))

    if paths_path.is_file():
        paths_spec = load_yaml(paths_path)
    else:
        paths_spec = fetch_yaml(paths_url, cache_dir / "paths.yaml")

    if schemas_path.is_file():
        schemas_spec = load_yaml(schemas_path)
    else:
        schemas_spec = fetch_yaml(schemas_url, cache_dir / "schemas.yaml")

    validate_paths(paths_spec)
    validate_schemas(schemas_spec)

    print("OpenAPI validation OK")


if __name__ == "__main__":
    main()
