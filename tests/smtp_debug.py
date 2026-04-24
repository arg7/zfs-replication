#!/usr/bin/env python3
import socket
import sys

def handle_client(conn, addr):
    print(f"--- Connection from {addr} ---")
    conn.sendall(b"220 Debug SMTP Server\r\n")
    
    in_data = False
    mail_data = ""
    
    while True:
        try:
            chunk = conn.recv(1024).decode('utf-8', errors='ignore')
            if not chunk:
                break
                
            if in_data:
                mail_data += chunk
                if mail_data.endswith("\r\n.\r\n"):
                    print("--- MAIL CONTENT ---")
                    print(mail_data[:-5]) # Strip the trailing .\r\n
                    print("--------------------")
                    conn.sendall(b"250 OK: queued as 12345\r\n")
                    in_data = False
                    mail_data = ""
            else:
                lines = chunk.strip().split("\r\n")
                for line in lines:
                    if not line: continue
                    print(f"C: {line}")
                    cmd = line.upper().split()[0]
                    
                    if cmd in ("EHLO", "HELO"):
                        conn.sendall(b"250-localhost\r\n250-AUTH LOGIN PLAIN\r\n250 HELP\r\n")
                    elif cmd == "AUTH":
                        conn.sendall(b"235 Authentication successful\r\n")
                    elif cmd in ("MAIL", "RCPT"):
                        conn.sendall(b"250 OK\r\n")
                    elif cmd == "DATA":
                        conn.sendall(b"354 End data with <CR>\n.\n<CR>\r\n")
                        in_data = True
                    elif cmd == "QUIT":
                        conn.sendall(b"221 Bye\r\n")
                        return
                    else:
                        conn.sendall(b"250 OK\r\n")
        except ConnectionResetError:
            break
            
    print(f"--- Connection closed by {addr} ---\n")

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 1025
    host = "127.0.0.1"
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        s.bind((host, port))
        s.listen(5)
        print(f"Listening for SMTP connections on {host}:{port}...")
        
        while True:
            conn, addr = s.accept()
            handle_client(conn, addr)
            conn.close()
            
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        s.close()

if __name__ == "__main__":
    main()
