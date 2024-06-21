import socket
import asyncio

async def handle_client(reader, writer):
    while True:
        await reader.read(1024)
        writer.write(b"+PONG\r\n")
        await writer.drain()
    
async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = await asyncio.start_server(handle_client, "localhost", 6379)
 
    async with server_socket:
        await server_socket.serve_forever()
    
if __name__ == "__main__":
    asyncio.run(main())
