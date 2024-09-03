from app.main import store, Item, find_value

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
    
   
    
    

    print(f'this is split parts before xff {split_parts}')
    
    def find_end_index(list, target):
        for i, item in enumerate(list):
            if target in item:
                return i

    end_index = find_end_index(split_parts, "xff")
    sliced_list = split_parts[:end_index + 1]
    print(f' this is sliced list {sliced_list}')

    result_value_index = resizedb_index + 5
    result_value_arr.append(sliced_list[result_value_index])
    while result_value_index < len(sliced_list):
        result_value_index +=3
        if result_value_index < len(sliced_list):
            result_value_arr.append(sliced_list[result_value_index])
        

        
    result_key_index = resizedb_index + 4
    result_key_arr.append(sliced_list[result_key_index])
    while result_key_index < len(sliced_list):
        result_key_index +=3
        if result_key_index < len(sliced_list):
            result_key_arr.append(sliced_list[result_key_index])
        
        

    # Key/value bytes will be for example x04pear so need to remove the bytes
    print(f'result key arr {result_key_arr}')
    print(f'result val arr {result_value_arr}')
    print(f'result expiry arr {result_expiry_arr}')
    result, store = remove_bytes_chars(result_key_arr, result_value_arr, result_expiry_arr)
    print(f'result from parse {result}')
    return result, store

def convert_to_seconds(hex_strings):
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
        print("returning val arr ")
        return new_value_arr, store
    else:
        print("returning key arr ")
        return new_key_arr, store
