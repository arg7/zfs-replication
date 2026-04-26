#!/usr/bin/env python3
import socket
import argparse
import sys
import os
import logging
import re
from datetime import datetime

class ColorStrippingFormatter(logging.Formatter):
    """Strips ANSI escape sequences (colors) from log messages."""
    ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*[mK]')

    def format(self, record):
        message = super().format(record)
        return self.ANSI_ESCAPE.sub('', message)

def handle_client(conn, addr, show_mail_only, use_color):
    if not show_mail_only:
        logging.info(f"--- Connection from {addr} ---")
    
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
                    logging.info(f"C: {line}")
                
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
                    logging.info(f"{C_BOLD}--- MAIL CONTENT ---{C_RESET}")
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
                                logging.info(f"{label}{val}")
                            else:
                                logging.info(h_line)
                        else:
                            logging.info(h_line)
                    
                    if sep:
                        logging.info("|")
                        for b_line in body_part.splitlines():
                            logging.info(f"| {C_BOLD}{b_line}{C_RESET}")
                    
                    logging.info(f"{C_BOLD}--------------------{C_RESET}\n")
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
        logging.info(f"--- Connection closed by {addr} ---\n")

def show_logs(log_file, filter_arg, oneline, grep_pattern=None):
    if not os.path.exists(log_file):
        print(f"Log file {log_file} not found.")
        return

    with open(log_file, 'r') as f:
        content = f.read()

    # Split by the start delimiter with timestamp
    # Format: 2026-04-26 12:34:56,789 --- MAIL CONTENT ---
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) --- MAIL CONTENT ---'
    parts = re.split(pattern, content)
    
    parsed_emails = []
    # parts[0] is garbage before first mail
    for i in range(1, len(parts), 2):
        timestamp = parts[i]
        body = parts[i+1]
        
        # Split body into mail content and other potential log noise
        mail_parts = body.split('--------------------', 1)
        mail_content = mail_parts[0]
        
        # Strip timestamps from each line to find headers
        subject = "No Subject"
        for line in mail_content.splitlines():
            # Remove leading timestamp: "2026-04-26 12:34:56,789 "
            clean_line = re.sub(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\s+', '', line).strip()
            if clean_line.lower().startswith("subject:"):
                subject = clean_line.split(":", 1)[1].strip()
                break
                
        parsed_emails.append({
            'timestamp': timestamp,
            'subject': subject,
            'full': f"{timestamp} --- MAIL CONTENT ---{mail_content}--------------------"
        })

    if not parsed_emails:
        print("No emails found in log.")
        return

    # Newest first
    parsed_emails.reverse()

    # Apply grep filter if provided
    if grep_pattern:
        filtered = []
        try:
            regex = re.compile(grep_pattern, re.IGNORECASE)
            for i, email in enumerate(parsed_emails):
                # If oneline, match against subject, otherwise full content
                search_target = email['subject'] if oneline else email['full']
                if regex.search(search_target):
                    # Keep track of original relative position for the label
                    email['_orig_idx'] = i
                    filtered.append(email)
            parsed_emails = filtered
        except re.error as e:
            print(f"Invalid regex pattern: {e}")
            return

    # Parse range filter
    try:
        if filter_arg.upper() == 'HEAD':
            indices = [0]
        elif '-' in filter_arg and not filter_arg.startswith('-'):
            start_str, end_str = filter_arg.split('-')
            start = abs(int(start_str))
            end = abs(int(end_str))
            indices = range(min(start, end), max(start, end) + 1)
        else:
            indices = [abs(int(filter_arg))]
    except ValueError:
        print(f"Invalid filter: {filter_arg}. Use 0, -1, 0-2, or HEAD.")
        return

    for idx in indices:
        if idx < len(parsed_emails):
            email = parsed_emails[idx]
            # Use original index for relative position label if grep was used
            orig_idx = email.get('_orig_idx', idx)
            label = 0 if orig_idx == 0 else -orig_idx
            if oneline:
                print(f"({label:>4}) {email['timestamp']} {email['subject']}")
            else:
                print(f"--- Position: {label} ---")
                print(email['full'])
                print()

def main():
    parser = argparse.ArgumentParser(description="Debug SMTP Server")
    parser.add_argument("port_or_filter", nargs="?", default=None, help="Port to listen on OR filter for --log")
    parser.add_argument("--show-mail-only", action="store_true", help="Hide protocol details")
    parser.add_argument("--use-color", action="store_true", help="Visually enhance mail dump")
    parser.add_argument("--log", action="store_true", help="Read emails from log instead of starting server")
    parser.add_argument("--oneline", action="store_true", help="Show only time and subject in log mode")
    parser.add_argument("--grep", help="Pattern to search for in log mode")
    
    args = parser.parse_args()
    
    log_file = f"/var/log/{os.path.basename(__file__)}.log"

    if args.log:
        filter_val = args.port_or_filter if args.port_or_filter is not None else "0"
        show_logs(log_file, filter_val, args.oneline, args.grep)
        return

    # Server mode
    host = "127.0.0.1"
    port = int(args.port_or_filter) if args.port_or_filter and args.port_or_filter.isdigit() else 1025
    
    # Setup logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Console handler (with colors if requested)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)

    # File handler (colors always stripped, includes timestamp for parsing)
    try:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(ColorStrippingFormatter('%(asctime)s %(message)s'))
        logger.addHandler(file_handler)
    except Exception as e:
        sys.stderr.write(f"Warning: Could not open log file {log_file}: {e}\n")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        s.bind((host, port))
        s.listen(5)
        logging.info(f"Listening for SMTP connections on {host}:{port}...")
        if args.show_mail_only:
            logging.info("(Protocol details hidden. Waiting for mail...)")
            
        while True:
            conn, addr = s.accept()
            handle_client(conn, addr, args.show_mail_only, args.use_color)
            conn.close()
            
    except KeyboardInterrupt:
        logging.info("\nShutting down.")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        s.close()

if __name__ == "__main__":
    main()
