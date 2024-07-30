#XV6
from collections import defaultdict
from dataclasses import dataclass
import time
from typing import List, Optional
import asyncio
import argparse

# Using data class because I want to store data, not use logic
@dataclass
class Item:
    value: str
    expiry: Optional[int] = None

store = defaultdict(Item)
replicas = []
replica_writers = []

WRITE_COMMANDS = ["SET"]

replica_port = None
replica_reader = None
master_port = None

def parse_resp(data):
    parts = data.split(b'\r\n')
    commands = []
    total_parsed_bytes = 0
    i = 0

    while i < len(parts):
        part = parts[i]

        if part.startswith(b'+'):  # Simple string
            commands.append(part[1:].decode('utf-8'))
            total_parsed_bytes += len(part) + 2  # +2 for '\r\n'
            i += 1
            
        elif part.startswith(b'$'):  # Bulk string
            length_str = part[1:]
            if length_str:
                length = int(length_str)
                # Ensure there are enough parts remaining for the bulk string and its terminator
                if i + 1 < len(parts):
                    total_parsed_bytes += len(part) + 2  # +2 for '\r\n'
                    i += 1
                    if len(parts[i]) == length:  # Check if the length matches
                        element = parts[i].decode('utf-8', errors='ignore')
                        commands.append(element)
                        total_parsed_bytes += len(parts[i]) + 2  # +2 for '\r\n'
                        i += 1
                    else:
                        print(f"Invalid length for bulk string at parts[{i}], expected {length} but got {len(parts[i])}")
                        break  # Exit since we can't parse the message completely
                else:
                    print("Not enough parts for bulk string")
                    break  # Exit if we don't have enough data
            else:
                print(f"parts[{i}] = {parts[i]}, length is empty")
                i += 1
                
        elif part.startswith(b'*'):  # Array
            num_str = part[1:]
            if num_str:
                num_elements = int(num_str)
                total_parsed_bytes += len(part) + 2  # +2 for '\r\n'
                i += 1
                elements = []
                for _ in range(num_elements):
                    if i < len(parts) and parts[i].startswith(b'$'):
                        length_str = parts[i][1:]
                        if length_str:
                            length = int(length_str)
                            total_parsed_bytes += len(parts[i]) + 2  # +2 for '\r\n'
                            i += 1
                            if i < len(parts) and len(parts[i]) == length:  # Check if the length matches
                                element = parts[i].decode('utf-8', errors='ignore')
                                elements.append(element)
                                total_parsed_bytes += len(parts[i]) + 2  # +2 for '\r\n'
                                i += 1
                            else:
                                print(f"Invalid length for bulk string at parts[{i}], expected {length} but got {len(parts[i])}")
                                break  # Exit since we can't parse the message completely
                        else:
                            print(f"parts[{i}] = {parts[i]}, length is empty")
                            i += 1
                    else:
                        print(f"Invalid or missing $ prefix at parts[{i}]")
                        break  # Exit since we can't parse the message completely
                commands.append(elements)
            else:
                print(f"parts[{i}] = {parts[i]}, num_elements is empty")
                i += 1
                
        else:
            print(f"Unknown prefix at parts[{i}]")
            total_parsed_bytes += len(part) + 2  # +2 for '\r\n'
            i += 1

    # Determine how much data to keep for future calls
    if i < len(parts):
        remaining_data = b'\r\n'.join(parts[i:])  # Keep unprocessed parts
    else:
        remaining_data = b''  # No remaining data

    print("COMMANDS: ", commands)
    return commands, total_parsed_bytes, remaining_data














async def handle_message(data, writer):
    global master_port, port, replica_port
    print("from handle msg")
    print("data: " + data)
  
    if data:
        data_split = data.split("\r\n")
        print(data_split)
        if len(data_split) > 2:
            cmd = data_split[2].upper()

            if "PING" in cmd:
                writer.write('+PONG\r\n'.encode())
                await writer.drain()
            elif "ECHO" in cmd:
                resp = data_split[-2]
                writer.write(f"${len(resp)}\r\n{resp}\r\n".encode())
                await writer.drain()
            elif "SET" in cmd:
                key = data_split[4]
                value = data_split[6]
              
                if len(data_split) == 12:
                    # Expire = current time in ms + additional time
                    expire = int(time.time_ns() // 10**6) + int(data_split[10])
                else:
                    expire = None
                
                store[key] = Item(value, expire) 
                writer.write("+OK\r\n".encode())
                await writer.drain()
            elif "GET" in cmd:
                key = data_split[4]
                store_item: Optional[Item] = store.get(key)
                print(f"store {store}")

                if (
                    # If store and value exists
                    store
                    and store_item.value
                    and (
                        store_item.expiry is None # No expiry
                        or (
                            # Expiry is in the future
                            store_item.expiry is not None
                            and store_item.expiry > (time.time_ns() // 10**6)
                        )
                    )
                ):
                    writer.write(f"${len(store_item.value)}\r\n{store_item.value}\r\n".encode())
                else:
                    writer.write("$-1\r\n".encode())
                await writer.drain()
            elif "INFO" in cmd: # Respond to INFO replication command
                info_type = data_split[4] if len(data_split) == 6 else None
                if info_type and info_type == "replication":
                    role = "slave" if args.replicaof else "master"
                    role_str = f"role:{role}"
                    if role == "master":
                        master_replid = "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                        master_repl_offset = "master_repl_offset:0"
                        master_info_str = f"{role_str}\n{master_replid}\n{master_repl_offset}"
                        writer.write(f"${len(master_info_str)}\r\n{master_info_str}\r\n".encode())
                    else:
                        writer.write(f"${len(role_str)}\r\n{role_str}\r\n".encode())
                    await  writer.drain()
                else:
                    writer.write("$-1\r\n".encode())
                    await writer.drain()
            elif "REPLCONF" in cmd: # Respond to REPLCONF command
                print(data_split)
                if "listening-port" in data_split:
                    replica_port = data_split[6]
                writer.write('+OK\r\n'.encode())
                await writer.drain()
                
            elif "PSYNC" in cmd:
                global replica_writer
                fullresync = '+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n'
                # Send empty RDB file
                rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
                binary_data = bytes.fromhex(rdb_hex)
                header = f"${len(binary_data)}\r\n"

                writer.write(fullresync.encode())
                await writer.drain()
                writer.write(header.encode() + binary_data)
                await writer.drain()
                replica_writers.append(writer)
                await writer.drain()
            
            if master_port == port and replica_port and cmd in WRITE_COMMANDS:
                await propagate_commands(data)

async def propagate_commands(data):
    print("In propagate commands")
    global replica_writers
    if data:
        for writer in replica_writers:
            if writer.is_closing():
                break
            writer.write(data.encode())
            await writer.drain()
    print(replica_writers)
            
  
async def handle_client(reader, writer):
    print("from handle client")
    while True:
        data = await reader.read(1024)
        decoded_msg = data.decode()
        
        if not decoded_msg:
            break
        await handle_message(decoded_msg, writer)
    writer.close()
    await writer.wait_closed()

async def handle_handshake(reader, writer):
    """
    Send handshake.
    Connects to the master server if the current server is a replica.
    The replica sends a PING command to the master server, then sends REPLCONF twice to the master,
    and lastly a PSYNC to the master.
    """
    print('from run handshake')
    replicas.append(writer)

    # Send PING command
    writer.write(b'*1\r\n$4\r\nPING\r\n')
    await writer.drain()
    await reader.read(1024)

    # Send REPLCONF for listening-port
    writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n')
    await writer.drain()
    await reader.read(1024)

    # Send REPLCONF for capabilities
    writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n')
    await writer.drain()
    await reader.read(1024)

    # PSYNC command to indicate full resynchronization
    writer.write(b'*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n')
    await writer.drain()

    print("Waiting for RDB file...")

    # Flag to indicate handshake completion
    handshake_complete = True

    remaining_data = b''
    while True:
        data = await reader.read(1024)
        if not data:
            break

        if handshake_complete:
            print("Received RDB file. Handshake complete")
            
            # Combine any remaining data from the previous parse with new data
            combined_data = remaining_data + data
            
            while combined_data:
                commands, total_parsed_bytes, remaining_data = parse_resp(combined_data)
                print(f"Received from master: {commands}")
                
                # Update combined_data for the next iteration
                combined_data = combined_data[total_parsed_bytes:]

                if isinstance(commands, list):
                    print('hello1')
                    it = iter(commands)
                    for command in it:
                        print(f'this is command: {command}')
                        
                        if isinstance(command, list) and command and command[0] == "SET" and len(command) == 3:
                            print('im in set')
                            key = command[1]
                            value = command[2]
                            handle_set_command(key, value)
                        elif command == "SET":
                            try:
                                key = next(it)
                                value = next(it)
                                handle_set_command(key, value)
                            except StopIteration:
                                print("Error: Not enough arguments for SET command.")
                                
                        elif command == "GETACK" or "GETACK" in command:
                            print('im in getack')
                            writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n")
                            await writer.drain()
                        
                else:
                    print(f"Invalid command format: {commands}")

        else:
            print("Handshake not complete, skipping data processing.")

        


    # if b"GETACK" in test1 or test2:
    #     writer.write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".encode())
    #     await writer.drain()
    
def handle_set_command(key, value):
   
    print(f"Handling SET command: key = {key}, value = {value}")
    # Store the value in the defaultdict
    store[key] = Item(value=value)
    print(f"Set {key} to {value}")
    print(f'this is store: {store}')





async def connect_master(replica):
    host, port = replica.split(" ")
    reader_writer = await asyncio.open_connection(host, port)
    reader = reader_writer[0]
    writer = reader_writer[1]
    
    await handle_handshake(reader, writer)

    print("back in connect master")
   
    # while True:
        
    #     print("connect master loop")
    #     data = await reader.read(1024)
    #     print(data)
        
    #     if not data:
    #         print("No data")
    #         break
    #     msg1 = data.decode()
    #     raw_reqs = list(msg1.split("*"))[1:]
    #     print(f"raw reqs: {raw_reqs}")
    #     for message in raw_reqs:
    #         print(message)
    #         message = "*" + message
    #         await handle_message(message, writer)
    # writer.close()
    # await writer.wait_closed()

async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    global port, master_host, master_port, args
    # Start TCP server listening on localhost and port specified or default to 6379 if port is not specified
    parser = argparse.ArgumentParser("A Redis server written in Python")
    parser.add_argument('--port', type=int, default=6379, help="Port to run the server on")
    parser.add_argument('--replicaof', type=str, default=None, help="Replica server and port")
    
    args = parser.parse_args()
    port = args.port
    master_port = port
    server_socket = await asyncio.start_server(handle_client, "localhost", port)
    
    try:
        if args.replicaof:
            await connect_master(args.replicaof)
        async with server_socket:
            await server_socket.serve_forever()
    finally:
        server_socket.close()


        
if __name__ == "__main__":
    asyncio.run(main())

