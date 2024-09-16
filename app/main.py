from collections import defaultdict
from dataclasses import dataclass
import os
import time
from typing import Optional
import asyncio
import argparse
from . import commands
# Using data class because I want to store data, not use logic
@dataclass
class Item:
    value: str
    expiry: Optional[int] = None

@dataclass
class StreamEntry:
    id: str
    data: dict

@dataclass
class StreamEntries:
    entries: list[StreamEntry]

store = defaultdict(Item)
stream_store = StreamEntries(entries={})

replicas = []
replica_writers = []
# ack_replicas = 0

WRITE_COMMANDS = ["SET"]
# set_cmd = False
replica_port = None
replica_reader = None
master_port = None
find_value = False

def toggle_find_value(value: bool):
    global find_value
    find_value = value

def parse_resp(data):
    print(f' this is input data {data}')
    parts = data.split(b'\r\n')
    print(f'parts: {parts}')
    commands = []
    total_parsed_bytes = 0
    command_bytes = 0
    i = 0

    while i < len(parts):
        part = parts[i]

        if part.startswith(b'+'):  # Simple string
            commands.append(part[1:].decode('utf-8'))
            command_bytes += len(part) + 2  # +2 for '\r\n'
            i += 1
            
        elif part.startswith(b'$'):  # Bulk string
            length_str = part[1:]
            
            if length_str:
                length = int(length_str)
                command_bytes += len(part) + 2  # +2 for '\r\n'
                i += 1
                if i < len(parts) and len(parts[i]) == length:  # Check if the length matches
                    element = parts[i].decode('utf-8', errors='ignore')
                    commands.append(element)
                    command_bytes += len(parts[i]) + 2  # +2 for '\r\n'
                    i += 1
                else:
                    print(f"Invalid length for bulk string at parts[{i}], expected {length} but got {len(parts[i])}")
                    break
            else:
                print(f"parts[{i}] = {parts[i]}, length is empty")
                i += 1
                
        elif part.startswith(b'*'):  # Array
            num_str = part[1:]
            if num_str:
                num_elements = int(num_str)
                command_bytes += len(part) + 2  # +2 for '\r\n'
                i += 1
                elements = []
                for _ in range(num_elements):
                    if i < len(parts) and parts[i].startswith(b'$'):
                        length_str = parts[i][1:]
                        if length_str:
                            length = int(length_str)
                            command_bytes += len(parts[i]) + 2  # +2 for '\r\n'
                            i += 1
                            if i < len(parts) and len(parts[i]) == length:
                                element = parts[i].decode('utf-8', errors='ignore')
                                elements.append(element)
                                print(f'dis is elements: {elements}')
                                command_bytes += len(parts[i]) + 2  # +2 for '\r\n'
                                i += 1
                            # else:
                            #     print(f"Invalid length for bulk string at parts[{i}], expected {length} but got {len(parts[i])}")
                            #     break
                        else:
                            # print(f"parts[{i}] = {parts[i]}, length is empty")
                            i += 1
                    # else:
                    #     print(f"Invalid or missing $ prefix at parts[{i}]")
                    #     break
                commands.append(elements)
            else:
                # print(f"parts[{i}] = {parts[i]}, num_elements is empty")
                i += 1
                
        else:
            # print(f"Unknown prefix at parts[{i}]")
            i += 1

    total_parsed_bytes = command_bytes

    # Determine how much data to keep for future calls
    if i < len(parts):
        remaining_data = b'\r\n'.join(parts[i:])  # Keep unprocessed parts
    else:
        remaining_data = b''  # No remaining data

    print(f"Parsed commands: {commands}, Total parsed bytes: {total_parsed_bytes}, Remaining data: {remaining_data}")

    return commands, total_parsed_bytes, remaining_data





async def handle_message(data, writer):
    global master_port, port, find_value, replica_port
    print("from handle msg")
    print("data: " + data)
    
    if data:
        data_split = data.split("\r\n")
        print(f"data split: {data_split}")
        if len(data_split) > 2:
            cmd = data_split[2].upper()

            if "PING" in cmd:
                await commands.handle_ping(writer)
                
            elif "ECHO" in cmd:
                await commands.handle_echo(data_split, writer)
                
            elif "SET" in cmd:
                print("in set cmd")
                await commands.handle_set(data_split, writer, store)
                
            elif "GET" in cmd:
                await commands.handle_get(data_split, store, dir, dbfilename, Item, writer)
                
            elif "INFO" in cmd: # Respond to INFO replication command
                await commands.handle_info(data_split, args, writer)
                
            elif "REPLCONF" in cmd: # Respond to REPLCONF command
                replica_port = await commands.handle_replconf(data_split, writer)
                
                
            elif "PSYNC" in cmd:
                await commands.handle_psync(replica_writers, writer)
                
            elif "WAIT" in cmd:
                await commands.handle_wait(data_split, replica_writers, writer)
                
            elif "CONFIG" in cmd:
                await commands.handle_config(data_split, dir, dbfilename, writer)
                
            elif "KEYS" in cmd:
                await commands.handle_keys(data_split, dir, dbfilename, writer)
                
            elif "TYPE" in cmd:
                await commands.handle_type(data_split, stream_store, store, writer)
                
            elif "XADD" in cmd:
                await commands.handle_xadd(data_split, stream_store, writer)
                
            elif "XRANGE" in cmd:
                await commands.handle_xrange(data_split, stream_store, writer)

            elif "XREAD" in cmd:
                await commands.handle_xread(data_split, stream_store, writer)

            print("hello im dome")
            print(master_port, port, replica_port, cmd, WRITE_COMMANDS)
            if master_port == port and replica_port and cmd in WRITE_COMMANDS:
                print(" I WILL NOW PROPAGATE")
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

async def run_handshake(reader, writer):
   
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
    total_offset = 0
    getack_received = False  # Track if a GETACK was processed
    set_received = False
    while True:
        data = await reader.read(1024)
        if not data:
            break

        if handshake_complete:
            print("Received RDB file. Handshake complete")
            
        # Combine any remaining data from the previous parse with new data
            combined_data = remaining_data + data
            # Replicas should update their offset to account for all commands propagated from the master, including PING and REPLCONF itself.
            while combined_data:
                # set_received = False
                print(f'this is combined data: {combined_data}')
                commands, total_parsed_bytes, remaining_data = parse_resp(combined_data)
                print(f"Received from master: {commands}")
                print(f"Total parsed bytes: {total_parsed_bytes}")

                if total_parsed_bytes == 0:
                    print("Total parsed bytes is 0, breaking the loop")
                    break

                # Update combined_data for the next iteration
                combined_data = combined_data[total_parsed_bytes:]
                print(f'this is next comb data: {combined_data}')
                if isinstance(commands, list):
                    print('hello1')
                    print(commands)
                    
                    for command in commands:
                        print(f'this is command: {command}')
                        print(f"curr total offset: {total_offset}")
                        
                        if isinstance(command, list) and command and command[0] == "SET" and len(command) == 3:
                            print('im in set')
                            key = command[1]
                            value = command[2]
                            handle_set_command(key, value)
                            print(f'offset at start of set: {total_offset}')
                            
                            getack_found = False

                            for cmd in commands:
                                if cmd == "GETACK" or "GETACK" in cmd:
                                    getack_found = True
                                    print("GETACK is in command")

                            if not set_received:
                                if getack_found:
                                    total_offset += total_parsed_bytes - 37
                                else:
                                    total_offset += total_parsed_bytes
                                set_received = True
                            print(f'offset after updating if it is updated: {total_offset}')
                            print(f'parsed bytes: {total_parsed_bytes}')
                         
                        elif command == "SET":
                            key = commands[1]
                            value = commands[2]
                            print(key,value)
                            handle_set_command(key, value)
                            total_offset += total_parsed_bytes
                        # The offset should only include number of bytes of commands processed before receiving the current REPLCONF GETACK command.
                        elif command == "GETACK" or "GETACK" in command:
                            print('im in getack')
                            if not getack_received:
                                
                                response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(total_offset))}\r\n{total_offset}\r\n"
                               
                                print(f"this is my res: {response.encode()}")
                                print(f"total offset in getack: {total_offset}")
                                writer.write(response.encode())
                                await writer.drain()
                                getack_received = True
                                total_offset += 37
                                print(f"offset after updating: {total_offset}")
                           
                        elif command == "PING" or "PING" in command:
                            print(f'in ping {total_offset}')
                            total_offset += 14
                            print(total_offset)
                            print(total_parsed_bytes)
                            # Silently process PING
              
                    
                    for cmd in commands:
                        if "GETACK" in cmd:
                            getack_received = False
                            break
                    if "GETACK" in commands:
                            getack_received = False
                            break
                    
                    
                else:
                    print(f"Invalid command format: {commands}")

        else:
            print("Handshake not complete, skipping data processing.")

        


    
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
    
    await run_handshake(reader, writer)

    print("back in connect master")
   
   

async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    global port, master_host, master_port, args, dir, dbfilename
    # Start TCP server listening on localhost and port specified or default to 6379 if port is not specified
    parser = argparse.ArgumentParser("A Redis server written in Python")
    parser.add_argument('--port', type=int, default=6379, help="Port to run the server on")
    parser.add_argument('--replicaof', type=str, default=None, help="Replica server and port")
    parser.add_argument('--dir', type=str, default=None, help="Path where the RDB file is stored")
    parser.add_argument('--dbfilename', type=str, default=None, help="Name of the RDB file")

    args = parser.parse_args()
    port = args.port
    master_port = port
    dir = args.dir
    dbfilename = args.dbfilename
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

