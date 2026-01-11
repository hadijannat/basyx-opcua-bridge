import asyncio

from asyncua import Server


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840")
    server.set_server_name("BaSyx OPC UA Simulator")

    idx = await server.register_namespace("http://example.org/opcua")
    objects = server.nodes.objects

    temperature = await objects.add_variable(idx, "Temperature", 20.0)
    speed = await objects.add_variable(idx, "Speed", 10.0)

    await temperature.set_writable()
    await speed.set_writable()

    async with server:
        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
