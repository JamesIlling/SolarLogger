# logger.py
# Interpreter: Python v3.9
#
# This script is intended to log all incoming udp packages on a given port. This is intended to be used to capture all
# messages sent from a Solis Data Logger LAN Stick to a target machine.


if __name__ == '__main__':
    import socket
    import binascii
    import time

    # What addresses to listen on (0.0.0.0 means all addresses)
    listen_address = '0.0.0.0'
    # Port to listen on.
    # This is taken from the 'Manual' section of the configuration website.
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
            # This may be due to solar panels not generating electricity
            # as it is dark, which powers down the data logger.
            print("Socket timeout ignored")
