import asyncio

from asyncua import Server, ua


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840")
    server.set_server_name("BaSyx OPC UA Simulator")

    idx = await server.register_namespace("http://example.org/opcua")
    while idx < 2:
        idx = await server.register_namespace(f"http://example.org/opcua/{idx}")
    ns_index = 2 if idx >= 2 else idx

    objects = server.nodes.objects
    obj = await objects.add_object(ns_index, "TestObject")

    temp_nodeid = ua.NodeId("Temperature", ns_index, ua.NodeIdType.String)
    speed_nodeid = ua.NodeId("Speed", ns_index, ua.NodeIdType.String)
    temperature = await obj.add_variable(temp_nodeid, "Temperature", 20.0)
    speed = await obj.add_variable(speed_nodeid, "Speed", 10.0)

    await temperature.set_writable()
    await speed.set_writable()

    async with server:
        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
