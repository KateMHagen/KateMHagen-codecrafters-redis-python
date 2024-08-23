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
ack_replicas = 0

WRITE_COMMANDS = ["SET"]
set_cmd = False
replica_port = None
replica_reader = None
master_port = None



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
    global master_port, port, replica_port, ack_replicas, set_cmd
    print("from handle msg")
    print("data: " + data)
  
    if data:
        data_split = data.split("\r\n")
        print(f"data split: {data_split}")
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
                set_cmd = True
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
                print("I'm in replconf")
                response = ""
                if "listening-port" in data_split:
                    replica_port = data_split[6]
                    response = '+OK\r\n'
                # writer.write('+OK\r\n'.encode())
                elif "capa" in data_split:
                    response = '+OK\r\n'
                elif data_split[4] == "ACK":
                    # Increase num of acknowledged replicas
                    ack_replicas += 1                
                    print(f"Increasing number of acknowledged replicas to: {ack_replicas}")
                    
                if response:
                    writer.write(response.encode())
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
                
            elif "WAIT" in cmd:
                # Answer with the amount of replicas that "confirmed" the response
                # if len(replica_writers) == data_split[4]:
                #     writer.write(f":{len(replica_writers)}\r\n".encode())
                # await writer.drain()
                num_replicas_created = int(data_split[4])
                timeout = float(data_split[6]) / 1000

                start = time.time()
                initial_ack_count = ack_replicas  # Capture initial count to avoid double counting

                for replica in replica_writers:
                    replica.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode())
                    await writer.drain()

                while (ack_replicas - initial_ack_count < num_replicas_created) and ((time.time() - start) < timeout):
                    current_time = time.time()
                    elapsed_time = current_time - start
                    print(
                        f"NUMACKS: {ack_replicas - initial_ack_count} of {num_replicas_created}, "
                        f"elapsed_time: {elapsed_time:.4f} seconds, "
                        f"timeout: {timeout:.4f} seconds"
                    )
                    await asyncio.sleep(0.005)
                

                final_acks = ack_replicas - initial_ack_count
                print(f"sending back {final_acks}")
                
                if ack_replicas > 1:
                    ack_replicas -= 1
                writer.write(f":{final_acks if set_cmd else len(replica_writers) }\r\n".encode())
               
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
                        
                        # elif command == "ACK" or "ACK" in command:
                        #     # Increase num of acknowledged replicas
                        #     ack_replicas += 1
                        #     print(f"Increasing number of acknowledged replicas to: {ack_replicas}")
                            
                        #     return None
                           
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
    
    await handle_handshake(reader, writer)

    print("back in connect master")
   
   

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

