if __name__ == '__main__':
    import socket
    import binascii
    import time

    # What addresses to listen on (0.0.0.0 means all addresses)
    listen_address = '0.0.0.0'
    # Port to listen on
    listen_port = 5432

    # configure the socket to listen on.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((listen_address, listen_port))

    while True:
        try:
            # get the data from the logger
            rawdata = sock.recv(1000)

            # using the current date create the file_name in the format
            # yyyy-mm-dd.txt
            current_date = time.strftime("%Y-%m-%d")
            file_name = current_date + ".txt"

            # build the log message
            current_time = time.strftime("%H:%M:%S")
            data = binascii.hexlify(rawdata)
            log_message = f"{current_time} {str(data)}"

            # display message to the console
            print(log_message)

            # write the message to the log file
            file = open(file_name, "a")
            file.write(log_message)
            file.write("\n")
            file.close()

        except socket.timeout:
            # handle the socket timeout by displaying a message and then
            # return to processing messages.
            print("Socket timeout ignored")
