import asyncio
import argparse
import socket

dict = {}
expiry_of_tasks = {}
is_slave = False
def parse_resp(data):
    if data.startswith(b'*'):
        # Split data in parts based on \r\n
        # Extract num of items
        parts = data.split(b'\r\n')
        arr = int(parts[0][1:])
        new_arr = [] 
        i = 1
        # Loop through each part to parse RESP protocol
        while arr > 0:
            if parts[i].startswith(b'$'):
                # Extract length of following item
                length = int(parts[i][1:])
                new_arr.append(parts[i+1][:length])
                # Move to new item skipping length and data
                i += 2
                # Decrease remaining item count
                arr -= 1
        return new_arr 
    return []
   

async def handle_client(reader, writer):
    while True:
        # Read client data
        data = await reader.read(1024)
        # Parse received data
        parse = parse_resp(data)
        # Decode first command from parsed RESP data
        command = parse[0].decode()

        if command == 'PING':
            writer.write(b'+PONG\r\n')
        elif command == 'ECHO':
            # The tester will expect to receive $3\r\nhey\r\n as a response (that's the string hey encoded as a RESP bulk string.
            if len(parse) > 1:
                response = f"${len(parse[1])}\r\n{parse[1].decode()}\r\n".encode()
                writer.write(response)
        elif command == 'SET':
            expiry = None
            # Handle if key has an expiry
            if len(parse) > 3 and parse[3] == b'px':
                expiry = int(parse[4])
        
            dict[parse[1]] = parse[2]   

            if expiry:
                if parse[1] in expiry_of_tasks:
                    expiry_of_tasks[parse[1]].cancel()
                expiry_of_tasks[parse[1]] = asyncio.create_task(expire_key(parse[1], expiry))
            response = "+OK\r\n".encode()
            writer.write(response)
        elif command == 'GET':
            if parse[1] in dict:
                response = f"${len(dict[parse[1]])}\r\n{dict[parse[1]].decode()}\r\n".encode()
            else:
                response = b"$-1\r\n"
            writer.write(response)
        elif command == 'INFO':
            if is_slave:
                response = b'$10\r\nrole:slave\r\n'
            else:
                response = b'$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n'
            writer.write(response)
        await writer.drain() # Ensure data is written to client

async def expire_key(key, expiry):
    # Convert milliseconds to seconds
    await asyncio.sleep(expiry / 1000)
    # Removes key if it exists, returns None if it doesn.t
    dict.pop(key, None)
    expiry_of_tasks.pop(key, None)
    
async def connect_to_master(master_host, master_port):
    """
    Send handshake.
    Connects to the master server if the current server is a replica.
    The replica sends a PING command to the master server, then sends REPLCONF twice to the master,
    and lastly a PSYNC to the master.
    """
    reader, writer = await asyncio.open_connection(master_host, master_port)
    writer.write(b'*1\r\n$4\r\nPING\r\n')
    await reader.read(1024)
    writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n'
    )
    await reader.read(1024)
    writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n')
    await reader.read(1024)
    writer.write(b'*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n')
    await writer.drain()
    

async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    global is_slave
    # Start TCP server listening on localhost and port specified or default to 6379 if port is not specified
    parser = argparse.ArgumentParser("A Redis server written in Python")
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--replicaof', type=ascii)
    
    args = parser.parse_args()
   
    if args.replicaof:
        is_slave = True
        master_host = parser.parse_args().replicaof[1:10]
        master_port = parser.parse_args().replicaof[11:15]
        await connect_to_master(master_host, master_port)
       
    server_socket = await asyncio.start_server(handle_client, "localhost", args.port)

    async with server_socket:
        await server_socket.serve_forever() # Serve clients forever
    
if __name__ == "__main__":
    asyncio.run(main())
