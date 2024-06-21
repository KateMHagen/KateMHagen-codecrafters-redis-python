import asyncio

def parseResp(data):
    if data.startswith(b'*'):
        # Split data in parts based on \r\n
        # Extract num of items
        parts = data.split(b'\r\n')
        arr = int(parts[0][1:])
        newArr = [] 
        i = 1
        # Loop through each part to parse RESP protocol
        while arr > 0:
            if parts[i].startswith(b'$'):
                # Extract length of following item
                length = int(parts[i][1:])
                newArr.append(parts[i+1][:length])
                # Move to new item skipping length and data
                i += 2
                # Decrease remaining item count
                arr -= 1
        return newArr 
    return []
   

async def handle_client(reader, writer):
    while True:
        # Read client data
        data = await reader.read(1024)
        # Parse received data
        parse = parseResp(data)
        # Decode first command from parsed RESP data
        command = parse[0].decode()
     
        if command == 'PING':
            writer.write(b"+PONG\r\n")
        elif command == 'ECHO':
            # The tester will expect to receive $3\r\nhey\r\n as a response (that's the string hey encoded as a RESP bulk string.
            if len(parse) > 1:
                response = f"${len(parse[1])}\r\n{parse[1].decode()}\r\n".encode()
                writer.write(response)
      
        await writer.drain() # Ensure data is written to client


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Start TCP server listening on localhost and port 6379
    server_socket = await asyncio.start_server(handle_client, "localhost", 6379)
 
    async with server_socket:
        await server_socket.serve_forever() # Serve clients forever
    
if __name__ == "__main__":
    asyncio.run(main())
