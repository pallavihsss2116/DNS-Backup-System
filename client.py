#!/usr/bin/env python3

import socket   # Imports Python's native socket module for network communication
import sys      # Imports system module to read command-line arguments (file path)
import hashlib  # Imports hashlib to generate the MD5 checksum hash
import time     # Imports time module to measure transfer duration and throughput
import os       # Imports os module to check file existence, size, and paths

# Defines a list of backup servers with their IP addresses and port numbers
SERVERS = [('10.0.0.3', 5001), ('10.0.0.2', 5001)]

# Sets the standard chunk size to 4096 bytes (4KB) for reading and sending data
CHUNK_SIZE = 4096 

def calculate_checksum(file_path):
    hash_md5 = hashlib.md5()                    # Initializes a new MD5 hash object
    with open(file_path, "rb") as f:           # Opens the target file in binary read mode
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""): # Loops through file blocks until EOF
            hash_md5.update(chunk)              # Updates the hash object with the current block data
    return hash_md5.hexdigest()                 # Returns the final hexadecimal string representation of the hash

def send_backup(file_path):
    if not os.path.exists(file_path):           # Checks if the specified file path exists on disk
        print(f"[-] Error: The file {file_path} does not exist.") # Prints error if file is missing
        return                                  # Exits the function early

    file_size = os.path.getsize(file_path)      # Retrieves total size of the file in bytes
    file_name = os.path.basename(file_path)     # Extracts just the file name from the full path
    
    print("[*] Calculating MD5 checksum for file integrity...") # Status message for user
    checksum = calculate_checksum(file_path)    # Calls function to generate file MD5 hash

    for host, port in SERVERS:                  # Loops through the backup servers list for failover handling
        print(f"\n[*] Attempting connection to Backup Server at {host}:{port}...") # Status message
        try:                                    # Starts try block to catch network or socket errors
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Creates a TCP IPv4 socket
                s.settimeout(5.0)               # Sets a 5-second timeout limit to avoid infinite hanging
                s.connect((host, port))         # Establishes TCP connection with the current server target
                print(f"[+] Connected to {host}:{port}") # Prints success message upon connecting
                
                header = f"{file_name}|{file_size}|{checksum}".encode() # Formats metadata string into bytes
                s.sendall(header)               # Sends the metadata header across the socket connection

                response = s.recv(1024).decode() # Receives server response byte string and decodes it
                if response.startswith("OFFSET:"): # Checks if server requested a resumable byte offset
                    offset = int(response.split(":")[1]) # Extracts the offset integer value
                else:
                    offset = 0                  # Sets offset to 0 if starting a fresh transfer
                    
                if offset > 0:                  # Checks if a partial file resume offset exists
                    print(f"[*] Server found partial file. Resuming transfer from byte {offset}...")
                else:
                    print("[*] Starting fresh file transfer...") # Message for full transfer start
                
                start_time = time.time()        # Records the starting timestamp for throughput tracking
                bytes_sent = 0                  # Initializes counter for total bytes transmitted

                with open(file_path, "rb") as f: # Opens file in binary read mode again for streaming
                    f.seek(offset)              # Moves file pointer to offset position if resuming
                    while True:                 # Enters infinite loop to stream file data chunks
                        chunk = f.read(CHUNK_SIZE) # Reads next chunk of data from file
                        if not chunk:           # Checks if chunk is empty (End of File reached)
                            break               # Breaks out of loop when file reading finishes
                        s.sendall(chunk)        # Transmits the chunk across the TCP socket
                        bytes_sent += len(chunk)# Increments the total bytes sent counter

                print("[*] File sent. Waiting for server integrity verification...") # Status update
                receipt = s.recv(1024).decode() # Waits for and reads verification receipt from server
                end_time = time.time()          # Records ending timestamp upon receiving response
                
                if "OK" in receipt:             # Checks if the server verified the file successfully
                    duration = end_time - start_time # Calculates total transfer duration in seconds
                    throughput = (bytes_sent / 1024 / 1024) / duration if duration > 0 else 0 # Computes MB/s speed
                    
                    print(f"\n=====================================") # Formatted output banner
                    print(f"         TRANSFER COMPLETE           ") # Banner title
                    print(f"=====================================") # Formatted output banner
                    print(f" Integrity:  Checksum Verified")      # Success status indicator
                    print(f" Time Taken: {duration:.4f} seconds")   # Prints transfer duration
                    print(f" Throughput: {throughput:.2f} MB/s")    # Prints calculated network speed
                    print(f"=====================================") # Formatted output banner
                    return                      # Exits function successfully after completion
                else:
                    print("[-] Error: Server reported checksum mismatch (Corrupted file).") # Error log
                    return                      # Exits function on checksum mismatch

        except (ConnectionRefusedError, socket.timeout, socket.error): # Catches network failures
            print(f"[-] Warning: Server {host} unreachable or failed. Failing over to next server...") # Failover log
    
    print("\n[!] CRITICAL ERROR: All backup servers failed. Backup aborted.") # Final failure log

if __name__ == "__main__":                      # Checks if script is executed directly from command line
    if len(sys.argv) != 2:                      # Validates that exactly one argument (file path) is passed
        print("Usage: python3 client.py <file_path>") # Prints usage instructions if syntax is wrong
    else:
        send_backup(sys.argv[1])                # Calls main backup function with provided file path argument