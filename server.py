#!/usr/bin/env python3

import socket   # Imports Python's native socket module for network listening and connections
import os       # Imports os module to handle file paths, directories, and file sizes
import hashlib  # Imports hashlib to calculate MD5 hashes for data integrity checks

HOST = '0.0.0.0'                # Defines the host IP ('0.0.0.0' listens on all available network interfaces)
PORT = 5001                     # Defines the port number on which the server listens for incoming connections
BACKUP_DIR = "/tmp/backup_server/" # Sets the directory path where received backup files will be saved

if not os.path.exists(BACKUP_DIR): # Checks if the backup directory path already exists on disk
    os.makedirs(BACKUP_DIR)     # Creates the backup directory (and any parent directories) if missing

def calculate_checksum(file_path):
    hash_md5 = hashlib.md5()                    # Initializes a new MD5 hash object
    with open(file_path, "rb") as f:           # Opens the specified backup file in binary read mode
        for chunk in iter(lambda: f.read(4096), b""): # Loops through file blocks of 4KB until EOF
            hash_md5.update(chunk)              # Updates the hash object with the current block data
    return hash_md5.hexdigest()                 # Returns the final hexadecimal string representation of the hash

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Creates a TCP IPv4 socket wrapped in a context manager
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allows immediate reuse of the port after server restart
    s.bind((HOST, PORT))        # Binds the socket to the specified HOST IP address and PORT number
    s.listen()                  # Puts the socket into listening mode for incoming client connections
    print(f"[*] Distributed Backup Server listening on port {PORT}...") # Prints server start confirmation message
    
    while True:                 # Enters an infinite loop to continuously accept incoming client connections
        conn, addr = s.accept() # Accepts a client connection, returning connection socket and client address
        with conn:              # Manages the client connection lifecycle safely using a context manager
            print(f"\n[+] Connection established from {addr}") # Prints notification when a client connects
            try:                # Starts a try block to handle potential transmission errors safely
                header = conn.recv(1024).decode() # Receives metadata header from client and decodes it to text
                if not header:          # Checks if the received header is empty
                    continue            # Skips to the next iteration if no header data was sent
                
                file_name, expected_size, expected_checksum = header.split('|') # Splits header string into name, size, checksum
                expected_size = int(expected_size) # Converts the expected file size string into an integer
                file_path = os.path.join(BACKUP_DIR, file_name) # Combines backup directory and file name into full path
                
                offset = 0              # Initializes transfer offset to 0 (default: fresh transfer)
                if os.path.exists(file_path): # Checks if a file with the same name already exists on disk
                    offset = os.path.getsize(file_path) # Gets current size of existing partial file on disk
                    if offset >= expected_size: # Checks if file is already completely backed up previously
                        offset = 0      # Resets offset to 0 to start fresh
                        open(file_path, 'w').close() # Overwrites/clears the existing file to start a clean transfer
                
                conn.sendall(f"OFFSET:{offset}".encode()) # Sends the byte offset back to the client over socket
                print(f"[*] Requested transfer offset: {offset} bytes.") # Logs the negotiated transfer offset
                
                bytes_received = offset # Sets bytes received counter to the starting offset value
                with open(file_path, "ab" if offset > 0 else "wb") as f: # Opens file to append ('ab') or write fresh ('wb')
                    conn.settimeout(5.0) # Sets a 5-second socket timeout while waiting for client data chunks
                    while bytes_received < expected_size: # Loops until all expected file bytes are received
                        try:            # Starts inner try block to catch socket timeout exceptions during chunk receive
                            chunk = conn.recv(4096) # Receives up to 4096 bytes from the client socket
                            if not chunk: # Checks if the client stopped sending data prematurely
                                break   # Breaks out of loop if connection closes unexpectedly
                            f.write(chunk) # Writes the received data chunk into the local backup file
                            bytes_received += len(chunk) # Increments total bytes received counter
                        except socket.timeout: # Catches socket timeout exceptions
                            print("[-] Connection timed out waiting for chunks.") # Logs timeout error
                            break       # Breaks loop on timeout
                
                print("[*] Transfer complete. Verifying file integrity...") # Status message for checksum verification
                actual_checksum = calculate_checksum(file_path) # Computes MD5 checksum of the saved file on disk
                
                if actual_checksum == expected_checksum: # Compares server-calculated hash with client's expected hash
                    print("[+] Integrity check passed. Backup saved successfully!") # Success log message
                    conn.sendall(b"OK") # Sends confirmation receipt 'OK' back to the client
                else:
                    print("[-] Integrity check failed! Checksums do not match.") # Checksum mismatch error log
                    conn.sendall(b"ERROR") # Sends error receipt back to the client

            except Exception as e:      # Catches any unexpected runtime or connection exceptions
                print(f"[-] Error processing connection: {e}") # Prints detailed error message to console