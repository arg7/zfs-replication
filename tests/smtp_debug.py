#!/usr/bin/env python3
import socket
import argparse
import sys

def handle_client(conn, addr, show_mail_only, use_color):
    if not show_mail_only:
        print(f"--- Connection from {addr} ---")
    
    def send(msg):
        conn.sendall(msg.encode() + b"\r\n")

    send("220 Debug SMTP Server")
    
    in_data = False
    data_buffer = ""
    
    C_BOLD = "\033[1m" if use_color else ""
    C_RESET = "\033[0m" if use_color else ""
    
    # Read line by line for commands
    f = conn.makefile('rb')
    
    while True:
        try:
            line_raw = f.readline()
            if not line_raw: break
            line = line_raw.decode('utf-8', errors='ignore').strip()
            
            if not in_data:
                if not line: continue
                if not show_mail_only:
                    print(f"C: {line}")
                
                parts = line.split()
                cmd = parts[0].upper()
                
                if cmd in ("EHLO", "HELO"):
                    send("250-localhost\r\n250-AUTH LOGIN PLAIN\r\n250 HELP")
                elif cmd == "AUTH":
                    if len(parts) > 2:
                        send("235 Authentication successful")
                    else:
                        send("334 ")
                elif len(line) > 10 and not cmd in ("MAIL", "RCPT", "DATA", "QUIT"):
                    send("235 Authentication successful")
                elif cmd in ("MAIL", "RCPT"):
                    send("250 OK")
                elif cmd == "DATA":
                    send("354 End data with <CR>.<CR>")
                    in_data = True
                    data_buffer = ""
                    # Read until .\r\n
                    while True:
                        d_line_raw = f.readline()
                        if not d_line_raw: break
                        d_line = d_line_raw.decode('utf-8', errors='ignore')
                        if d_line == ".\r\n":
                            break
                        data_buffer += d_line
                    
                    # Process Payload
                    print(f"{C_BOLD}--- MAIL CONTENT ---{C_RESET}")
                    payload = data_buffer
                    headers_part, sep, body_part = payload.partition("\n\n")
                    if not sep: # Try Windows line endings
                        headers_part, sep, body_part = payload.partition("\r\n\r\n")
                    
                    for h_line in headers_part.splitlines():
                        if ":" in h_line:
                            h_parts = h_line.split(":", 1)
                            key = h_parts[0].strip()
                            val = h_parts[1].strip()
                            if key.lower() in ("from", "to", "subject", "date"):
                                label = f"{key}:".ljust(9)
                                print(f"{label}{val}")
                            else:
                                print(h_line)
                        else:
                            print(h_line)
                    
                    if sep:
                        print("|")
                        for b_line in body_part.splitlines():
                            print(f"| {C_BOLD}{b_line}{C_RESET}")
                    
                    print(f"{C_BOLD}--------------------{C_RESET}\n")
                    send("250 OK: queued as 12345")
                    in_data = False
                elif cmd == "QUIT":
                    send("221 Bye")
                    break
                else:
                    send("250 OK")
            
        except (ConnectionResetError, BrokenPipeError):
            break
            
    if not show_mail_only:
        print(f"--- Connection closed by {addr} ---\n")

def main():
    parser = argparse.ArgumentParser(description="Debug SMTP Server")
    parser.add_argument("port", nargs="?", type=int, default=1025, help="Port to listen on")
    parser.add_argument("--show-mail-only", action="store_true", help="Hide protocol details")
    parser.add_argument("--use-color", action="store_true", help="Visually enhance mail dump")
    
    args = parser.parse_args()
    host = "127.0.0.1"
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        s.bind((host, args.port))
        s.listen(5)
        print(f"Listening for SMTP connections on {host}:{args.port}...")
        if args.show_mail_only:
            print("(Protocol details hidden. Waiting for mail...)")
            
        while True:
            conn, addr = s.accept()
            handle_client(conn, addr, args.show_mail_only, args.use_color)
            conn.close()
            
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        s.close()

if __name__ == "__main__":
    main()