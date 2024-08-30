from collections import defaultdict
from dataclasses import dataclass
import os
import time
from typing import Optional
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
find_value = False


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
    global master_port, port, replica_port, ack_replicas, set_cmd, find_value
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
                
                if dir and dbfilename:
                    find_value = True
                    response = get_value_from_rdb()
                    key = data_split[4]
                    store_item = store.get(key)
                    # print(f"this is key, storeitem expiry {key, store_item.expiry}")
                    
                    time_now = time.time()
                    print(f"time now {time_now}")
                    print(f"store {store}")
                    print(f"storeitem exp {store_item.expiry}")
                    is_expired = True if (store_item.expiry and store_item.expiry < time_now) else False
                    if not is_expired:
                        writer.write(f"${len(store_item.value)}\r\n{store_item.value}\r\n".encode())
                    else:
                        writer.write("$-1\r\n".encode())
                    await writer.drain()
                    
                    # writer.write(f"${len(store_item.value)}\r\n{store_item.value}\r\n".encode())
                    # await writer.drain()
                else:
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
                # This command waits for a specified number of replicas to acknowledge the received command.
                # It sends REPLCONF GETACK commands to all replicas, then waits for the specified number of acknowledgments
                # or until the given timeout expires. Finally, it returns the number of acknowledgments received to the client.

                num_replicas_created = int(data_split[4])
                timeout = float(data_split[6]) / 1000 # Convert to seconds

                # Start timing and capture the current number of acknowledgments to avoid double counting
                start = time.time()
                initial_ack_count = ack_replicas  
                
                # Send the REPLCONF GETACK command to all replicas
                for replica in replica_writers:
                    replica.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode())
                    await writer.drain()

                # Continuously check if the required number of replicas have acknowledged
                # or if the timeout has been reached
                while (ack_replicas - initial_ack_count < num_replicas_created) and ((time.time() - start) < timeout):
                    current_time = time.time()
                    elapsed_time = current_time - start
                    print(
                        f"NUMACKS: {ack_replicas - initial_ack_count} of {num_replicas_created}, "
                        f"elapsed_time: {elapsed_time:.4f} seconds, "
                        f"timeout: {timeout:.4f} seconds"
                    )
                    await asyncio.sleep(0.005) # Short sleep to avoid busy-waiting and allow other tasks to run
                
                # Calculate the final number of acknowledgments received within the timeout period
                final_acks = ack_replicas - initial_ack_count
    
                # Send the result back to the client: either the number of acknowledgments received
                # or the total number of replica writers if a different command (like SET) was issued
                writer.write(f":{final_acks if set_cmd else len(replica_writers) }\r\n".encode())
            
            elif "CONFIG" in cmd:
                print("config")
                if "GET" in data_split:
                    print("in config get")
                    if "dir" in data_split:
                        response = f"*2\r\n$3\r\ndir\r\n${len(dir)}\r\n{dir}\r\n"
                    elif "dbfilename" in data_split:
                        response = f"*2\r\n$10\r\ndbfilenamer\r\n${len(dbfilename)}\r\n{dbfilename}\r\n"
                    writer.write(response.encode())
            
            elif "KEYS" in cmd:
                # Return all keys in database
                if "*" in data_split:
                    keys = get_keys_from_rdb()
                    writer.write(keys)
                    await writer.drain()

            if master_port == port and replica_port and cmd in WRITE_COMMANDS:
                await propagate_commands(data)

def get_keys_from_rdb():
    print("im in get keys from rdb")
    if dir and dbfilename:
        print(f"dir: {dir} , filename: {dbfilename}")
        # Construct full path to RDB file
        rdb_file_path = os.path.join(dir, dbfilename)
        if os.path.exists(rdb_file_path):
            # Open file and read its content
            with open(rdb_file_path, "rb") as rdb_file:
                rdb_content = str(rdb_file.read())
                print(f"rdb content: {rdb_content}")
                if rdb_content:
                    # Parse content to extract keys

                    result = parse_redis_file_format(rdb_content)
                    print(f"eyoo {result}")
                    response = ""
                    for item in result:
                        response += f"${len(item)}\r\n{item}\r\n"
                    
                    return f"*{len(result)}\r\n{response}".encode()
    # If RDB file doesn't exist or no args provided
    return "*0\r\n".encode()

def get_value_from_rdb():
    print("im in get value from rdb")
    global find_value
    find_value = True
    if dir and dbfilename:
        print(f"dir: {dir} , filename: {dbfilename}")
        # Construct full path to RDB file
        rdb_file_path = os.path.join(dir, dbfilename)
        if os.path.exists(rdb_file_path):
            # Open file and read its content
            with open(rdb_file_path, "rb") as rdb_file:
                rdb_content = str(rdb_file.read())
                print(f"rdb content: {rdb_content}")
                if rdb_content:
                    # Parse content to extract keys
                    result = parse_redis_file_format(rdb_content)
                    print(f"result from parse value: {result}")
                   
    # If RDB file doesn't exist or no args provided
    return "*0\r\n".encode()

def parse_redis_file_format(file_format):
    print("in parse redis file format")
    # Content is seperated by "/" so get the different parts
    split_parts = file_format.split("\\")
    resizedb_index = split_parts.index("xfb")
    print(f"split parts: {split_parts}")
    start_index = 0
    result_key_arr = []
    result_value_arr = []
    result_expiry_arr = []
    if "xfc" in split_parts:
        while True:

            try:
                expiry_index = split_parts.index("xfc", start_index)
                print(f"'xfc' found at index: {expiry_index}")
                start_index = expiry_index + 1
                if not expiry_index + 9 > len(split_parts):
                    expiry = split_parts[expiry_index+1:expiry_index+9]
                    print(expiry)
                    expiry = convert_to_seconds(expiry)
                
                    print(f"expiry: {expiry}")
                    result_expiry_arr.append(expiry)
                    result_key_index = expiry_index + 9
                    result_key_arr.append(split_parts[result_key_index])
                

                    result_value_index = expiry_index + 10
                    result_value_arr.append(split_parts[result_value_index])
                
            except ValueError:
                break
    
   
    
    result_value_index = resizedb_index + 5
    result_value_arr.append(split_parts[result_value_index])
    while result_value_index < len(split_parts) - 4:
        result_value_index +=3
        result_value_arr.append(split_parts[result_value_index])

            
    result_key_index = resizedb_index + 4
    result_key_arr.append(split_parts[result_key_index])
    while result_key_index < len(split_parts) - 4:
        result_key_index +=3
        result_key_arr.append(split_parts[result_key_index])

    # Key/value bytes will be for example x04pear so need to remove the bytes
    print(f'result key arr {result_key_arr}')
    print(f'result val arr {result_value_arr}')
    print(f'result expiry arr {result_expiry_arr}')
    result = remove_bytes_chars(result_key_arr, result_value_arr, result_expiry_arr)
    print(f'result from parse {result}')
    return result

def convert_to_seconds(hex_strings):

    #
    # I tror denna funksjonen e feil!!!! :((
    #
    #
    #


    # Clean and concatenate hex strings
    # Function to clean the hex string by removing invalid characters
    def clean_hex_string(hex_str):
        valid_chars = set('0123456789abcdefABCDEF')
        # Remove 'x' and keep only valid hexadecimal characters
        return ''.join(c for c in hex_str[1:] if c in valid_chars)
        
        # Clean each hex string and join them
    cleaned_hex_data = ''.join(clean_hex_string(s) for s in hex_strings)

    # Convert cleaned hex string to bytes
    byte_data = bytes.fromhex(cleaned_hex_data)

    # Interpret the bytes as an integer (assuming little-endian for this example)
    number = int.from_bytes(byte_data[:4], byteorder='little')
    
    # Convert number to seconds (assuming the number represents milliseconds, for example)
    # seconds = number / 1000.0
    return number
    




def remove_bytes_chars(key_arr, value_arr, expiry_arr):
    i = 0
    new_key_arr = []
    new_value_arr = []
    new_key = ""
    new_value = ""
    for i in range(len(key_arr)):
        # If string starts "x", remove first 3 chars
        if key_arr[i].startswith("x"): 
            new_key = key_arr[i][3:]
        # If string starts "t", remove first char
        elif key_arr[i].startswith("t") or key_arr[i].startswith("n"): 
            new_key = key_arr[i][1:]
        # If item has non-alphabetic characters and are too short, dont include them
        if len(new_key) > 1 and new_key.isalpha():
            new_key_arr.append(new_key)

    print(f'new key arr {new_key_arr}')
    for i in range(len(value_arr)):
        # If string starts "x", remove first 3 chars
        if value_arr[i].startswith("x"):
            new_value = value_arr[i][3:]
        # If string starts "t", remove first char
        elif value_arr[i].startswith("t") or value_arr[i].startswith("n"):
            new_value = value_arr[i][1:]
        # If item has non-alphabetic characters and are too short, dont include them
        if len(new_value) > 1 and new_value.isalpha():
            new_value_arr.append(new_value)
    
    if len(expiry_arr) > 1:
        for key, value, expiry in zip(new_key_arr, new_value_arr, expiry_arr):
            print('helloooo')
            store[key] = Item(value, expiry)
    else:
        for key, value in zip(new_key_arr, new_value_arr):
            print('pelloooo')
            store[key] = Item(value, None)
    
    print(f'key arr {new_key_arr}')
    print(f'val arr {new_value_arr}')
    print(f"this is the store {store}")
    if find_value:
        return new_value_arr
    else:
        return new_key_arr


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

