import asyncio
import typer
from pathlib import Path

from basyx_opcua_bridge.config.models import BridgeConfig
from basyx_opcua_bridge.core.bridge import Bridge
import structlog

app = typer.Typer()
logger = structlog.get_logger()

@app.command()
def main(
    config: Path = typer.Option(..., "--config", "-c", help="Path to configuration file"),
) -> None:
    """Start the BaSyx OPC UA Bridge."""
    asyncio.run(_run_bridge(config))

async def _run_bridge(config_path: Path) -> None:
    try:
        cfg = BridgeConfig.from_yaml(config_path)
        bridge = Bridge(cfg)
        
        await bridge.start()
        
        # Keep running until interrupted
        try:
            await bridge.wait_until_stopped()
        except KeyboardInterrupt:
            await bridge.stop()
            
    except Exception as e:
        logger.critical("bridge_crashed", error=str(e))
        raise

if __name__ == "__main__":
    app()
