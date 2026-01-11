import asyncio
import json
from pathlib import Path
from typing import Literal, cast

import structlog
import typer

from basyx_opcua_bridge.aas.providers import build_aas_provider
from basyx_opcua_bridge.config.models import BridgeConfig
from basyx_opcua_bridge.core.bridge import Bridge
from basyx_opcua_bridge.discovery import (
    DiscoveryOptions,
    discover_opcua,
    generate_aas_env_json,
    write_bridge_config,
)
from basyx_opcua_bridge.mapping.engine import MappingEngine

app = typer.Typer()
logger = structlog.get_logger()


@app.command()
def main(
    config: Path = typer.Option(..., "--config", "-c", help="Path to configuration file"),
) -> None:
    """Start the BaSyx OPC UA Bridge."""
    try:
        asyncio.run(_run_bridge(config))
    except KeyboardInterrupt:
        pass


@app.command()
def discover(
    opcua: str = typer.Option(..., "--opcua", help="OPC UA endpoint URL"),
    out: Path = typer.Option(Path("config/bridge.generated.yaml"), "--out", help="Output YAML path"),
    report: Path | None = typer.Option(None, "--report", help="Discovery report JSON path"),
    emit_aas_env: Path | None = typer.Option(None, "--emit-aas-env", help="Emit AAS environment JSON"),
    endpoint_name: str = typer.Option("opcua", "--endpoint-name", help="Endpoint name label"),
    namespace: list[int] | None = typer.Option(None, "--namespace", help="Namespace indices to include"),
    include: str | None = typer.Option(None, "--include", help="Regex include filter for browse path"),
    exclude: str | None = typer.Option(None, "--exclude", help="Regex exclude filter for browse path"),
    max_depth: int = typer.Option(5, "--max-depth", help="Maximum browse depth"),
    root: str = typer.Option("Objects", "--root", help="Root node (Objects or NodeId string)"),
    group: str = typer.Option("namespace", "--group", help="Grouping strategy: namespace|path|root"),
    aas_type: str = typer.Option("memory", "--aas-type", help="AAS provider type"),
    aas_url: str | None = typer.Option(None, "--aas-url", help="AAS REST base URL"),
    poll_interval: float = typer.Option(1.0, "--poll-interval", help="AAS poll interval in seconds"),
    encode_ids: bool = typer.Option(True, "--encode-identifiers/--no-encode-identifiers", help="Base64url encode IDs"),
) -> None:
    """Discover OPC UA nodes and generate a bridge configuration."""
    try:
        asyncio.run(
            _discover(
                opcua,
                out,
                report,
                emit_aas_env,
                endpoint_name,
                namespace,
                include,
                exclude,
                max_depth,
                root,
                group,
                aas_type,
                aas_url,
                poll_interval,
                encode_ids,
            )
        )
    except KeyboardInterrupt:
        pass


@app.command()
def bootstrap(
    opcua: str = typer.Option(..., "--opcua", help="OPC UA endpoint URL"),
    out: Path = typer.Option(Path("config/bridge.generated.yaml"), "--out", help="Output YAML path"),
    endpoint_name: str = typer.Option("opcua", "--endpoint-name", help="Endpoint name label"),
    namespace: list[int] | None = typer.Option(None, "--namespace", help="Namespace indices to include"),
    include: str | None = typer.Option(None, "--include", help="Regex include filter for browse path"),
    exclude: str | None = typer.Option(None, "--exclude", help="Regex exclude filter for browse path"),
    max_depth: int = typer.Option(5, "--max-depth", help="Maximum browse depth"),
    root: str = typer.Option("Objects", "--root", help="Root node (Objects or NodeId string)"),
    group: str = typer.Option("namespace", "--group", help="Grouping strategy: namespace|path|root"),
    aas_type: str = typer.Option("basyx", "--aas-type", help="AAS provider type"),
    aas_url: str | None = typer.Option(None, "--aas-url", help="AAS REST base URL"),
    poll_interval: float = typer.Option(1.0, "--poll-interval", help="AAS poll interval in seconds"),
    encode_ids: bool = typer.Option(True, "--encode-identifiers/--no-encode-identifiers", help="Base64url encode IDs"),
    provision: bool = typer.Option(True, "--provision/--no-provision", help="Provision AAS submodels/elements"),
    run: bool = typer.Option(False, "--run/--no-run", help="Start bridge after bootstrap"),
) -> None:
    """Discover mappings, provision the AAS, and optionally run the bridge."""
    try:
        asyncio.run(
            _bootstrap(
                opcua,
                out,
                endpoint_name,
                namespace,
                include,
                exclude,
                max_depth,
                root,
                group,
                aas_type,
                aas_url,
                poll_interval,
                encode_ids,
                provision,
                run,
            )
        )
    except KeyboardInterrupt:
        pass


async def _run_bridge(config_path: Path) -> None:
    try:
        cfg = BridgeConfig.from_yaml(config_path)
        bridge = Bridge(cfg)
        await bridge.run()
    except Exception as e:
        logger.critical("bridge_crashed", error=str(e))
        raise


async def _discover(
    opcua: str,
    out: Path,
    report: Path | None,
    emit_aas_env: Path | None,
    endpoint_name: str,
    namespace: list[int] | None,
    include: str | None,
    exclude: str | None,
    max_depth: int,
    root: str,
    group: str,
    aas_type: str,
    aas_url: str | None,
    poll_interval: float,
    encode_ids: bool,
) -> None:
    options = DiscoveryOptions(
        endpoint_url=opcua,
        endpoint_name=endpoint_name,
        namespaces=namespace,
        include_pattern=include,
        exclude_pattern=exclude,
        max_depth=max_depth,
        root_node=root,
        group_strategy=group,
        aas_type=cast(Literal["basyx", "aasx-server", "memory"], aas_type),
        aas_url=aas_url,
        poll_interval_seconds=poll_interval,
        encode_identifiers=encode_ids,
    )
    result = await discover_opcua(options)
    write_bridge_config(result.config, out)

    if report:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result.report, indent=2), encoding="utf-8")

    if emit_aas_env:
        env = generate_aas_env_json(result.mappings)
        emit_aas_env.parent.mkdir(parents=True, exist_ok=True)
        emit_aas_env.write_text(json.dumps(env, indent=2), encoding="utf-8")

    logger.info("discovery_written", config=str(out))


async def _bootstrap(
    opcua: str,
    out: Path,
    endpoint_name: str,
    namespace: list[int] | None,
    include: str | None,
    exclude: str | None,
    max_depth: int,
    root: str,
    group: str,
    aas_type: str,
    aas_url: str | None,
    poll_interval: float,
    encode_ids: bool,
    provision: bool,
    run: bool,
) -> None:
    options = DiscoveryOptions(
        endpoint_url=opcua,
        endpoint_name=endpoint_name,
        namespaces=namespace,
        include_pattern=include,
        exclude_pattern=exclude,
        max_depth=max_depth,
        root_node=root,
        group_strategy=group,
        aas_type=cast(Literal["basyx", "aasx-server", "memory"], aas_type),
        aas_url=aas_url,
        poll_interval_seconds=poll_interval,
        encode_identifiers=encode_ids,
    )
    result = await discover_opcua(options)

    if provision:
        result.config.aas.auto_create_submodels = True
        result.config.aas.auto_create_elements = True
        engine = MappingEngine(result.config.mappings, result.config.semantic)
        provider = build_aas_provider(result.config.aas, engine)
        await provider.start()
        await provider.provision(engine.resolved_mappings())
        await provider.stop()

    write_bridge_config(result.config, out)
    logger.info("bootstrap_written", config=str(out))

    if run:
        bridge = Bridge(result.config)
        await bridge.run()


if __name__ == "__main__":
    app()
