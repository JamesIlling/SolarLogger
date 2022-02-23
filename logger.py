# processor.py
# Interpreter: Python v3.9
#
# This script reads the incoming message, and at the end of the solar day (when it gets dark, or disconnected from the
# logger) will generate a xlsx with the data for the day.

import gc
import pandas as pd
import socket
import time
import binascii
from datetime import datetime
from dataclasses import dataclass


@dataclass
class SolarInfo:
    time: datetime.time
    inverter_serial: str
    current_power: float
    temperature: float
    frequency: float
    vac1: float
    vac2: float
    vac3: float
    iac1: float
    iac2: float
    iac3: float
    vdc1: float
    vdc2: float
    idc1: float
    idc2: float


class DayInfoStore:
    latest_entry: datetime.time
    data: pd.DataFrame
    is_end_of_day = False
    day: str

    def __init__(self):
        self.latest_entry = datetime.min.time()
        self.data = pd.DataFrame()
        self.day = time.strftime("%Y-%m-%d")

    def end_of_day(self) -> None:
        if not self.is_end_of_day:
            self.__to_excel()

            #  release the memory for the data frame.
            del self.data
            gc.collect()

            self.is_end_of_day = True
            self.latest_entry = datetime.min.time()

    def __to_excel(self) -> None:
        self.data.to_excel(self.day + ".xlsx", sheet_name=self.day, index=False)

    def count(self) -> int:
        if not self.data:
            return 0
        return len(self.data.index)

    def add_solar_info(self, solar_info: SolarInfo) -> None:

        # dedupe data based on time received (we can get two messages for each entry)
        if solar_info.time > self.latest_entry:
            self.latest_entry = solar_info.time

            # reset after end of day by reconfiguring the day and setting as set up
            if self.is_end_of_day:
                self.day = time.strftime("%Y-%m-%d")
                self.data = pd.DataFrame()
                self.is_end_of_day = False

            # write the data into the data frame
            current = pd.DataFrame({
                "Time": [solar_info.time],
                "Inverter Serial": [solar_info.inverter_serial],
                "Current Power (W)": [solar_info.current_power],
                "Temperature (C)": [solar_info.temperature],
                "DC feed 1 (V)": [solar_info.vdc1],
                "DC feed 1 (A)": [solar_info.idc1],
                "DC feed 2 (V)": [solar_info.vdc1],
                "DC feed 2 (A)": [solar_info.idc2],
                "AC feed 1 (V)": [solar_info.vac1],
                "AC feed 1 (A)": [solar_info.iac1],
                "AC feed 2 (V)": [solar_info.vac2],
                "AC feed 2 (A)": [solar_info.iac2],
                "AC feed 3 (V)": [solar_info.vac3],
                "AC feed 3 (A)": [solar_info.iac3],
                "Frequency (Hz)": [solar_info.frequency]
            })
            self.data = pd.concat([self.data, current])


class SolarMANCustomerParser:
    start_of_message = 0xa5
    end_of_message = 0x16
    temperature_offset = 48
    vdc1_offset = 50
    vdc2_offset = 52
    idc1_offset = 54
    idc2_offset = 56
    iac1_offset = 58
    iac2_offset = 60
    iac3_offset = 62
    vac1_offset = 64
    vac2_offset = 66
    vac3_offset = 68
    frequency_offset = 70
    power_offset = 72
    checksum_offset = 246

    def parse_message(self, raw_message: bytes):
        length = len(raw_message)
        if length == 248:
            return self.__parse_long_message(raw_message)
        if length == 14:
            return self.__parse_short_message(raw_message)
        return

    def __parse_short_message(self, raw_message: bytes) -> None:
        return

    @staticmethod
    def __get_uint16(data_array: bytes, start: int, divisor: int) -> float:
        value_bytes = data_array[start:start + 2]
        return int.from_bytes(value_bytes, byteorder="little", signed=False) / divisor

    def __parse_long_message(self, data_array: bytes) -> SolarInfo:
        checksum = data_array[self.checksum_offset]
        calculated = self.__calculate_checksum(data_array[1:self.checksum_offset])
        if calculated != checksum:
            print("Invalid checksum :- aborting processing")

        inverter_serial = rawdata[32:47].decode()
        temperature_c = self.__get_uint16(rawdata, self.temperature_offset, 10)
        vdc1 = self.__get_uint16(rawdata, self.vdc1_offset, 10)
        vdc2 = self.__get_uint16(rawdata, self.vdc2_offset, 10)
        idc1 = self.__get_uint16(rawdata, self.idc1_offset, 10)
        idc2 = self.__get_uint16(rawdata, self.idc2_offset, 10)
        iac1 = self.__get_uint16(rawdata, self.iac1_offset, 10)
        iac2 = self.__get_uint16(rawdata, self.iac2_offset, 10)
        iac3 = self.__get_uint16(rawdata, self.iac3_offset, 10)
        vac1 = self.__get_uint16(rawdata, self.vac3_offset, 10)
        vac2 = self.__get_uint16(rawdata, self.vac3_offset, 10)
        vac3 = self.__get_uint16(rawdata, self.vac3_offset, 10)
        frequency_hz = self.__get_uint16(rawdata, self.frequency_offset, 100)
        current_power = self.__get_uint16(rawdata, self.power_offset, 1)

        return SolarInfo(
            None,
            inverter_serial,
            current_power,
            temperature_c,
            frequency_hz,
            vac1,
            vac2,
            vac3,
            iac1,
            iac2,
            iac3,
            vdc1,
            vdc2,
            idc1,
            idc2)

    @staticmethod
    def __calculate_checksum(data_to_checksum: bytes) -> int:
        checksum = 0
        for byte in data_to_checksum:
            checksum = checksum + byte
        return checksum % 256


if __name__ == '__main__':

    # What addresses to listen on (0.0.0.0 means all addresses)
    listen_address = '0.0.0.0'

    # Port to listen on.
    # This is taken from the 'Manual' section of the configuration website.
    listen_port = 5432

    write_to_screen = False

    # if no communication for 3 mins assume device is off due to end of day.
    socket_timeout = 180

    # configure the socket to listen on.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((listen_address, listen_port))
    sock.settimeout(socket_timeout)

    # create the data store and parser
    store = DayInfoStore()
    parser = SolarMANCustomerParser()

    while True:
        try:
            if write_to_screen:
                print('waiting for a connection')

            # get the data from the logger
            rawdata = sock.recv(1000)

            # get the time the message was received
            time_val = datetime.utcnow().time()

            # if required log the incoming message
            if write_to_screen:
                print(time_val.strftime("%H:%M:%S") + ":" + str(binascii.hexlify(rawdata)))

            # extract the solar info from the network message
            info = parser.parse_message(rawdata)

            # only attempt to add the solar info to the store if it was extracted.
            if info:
                info.time = time_val
                store.add_solar_info(info)

                # if required log the number of records which are being held.
                if write_to_screen:
                    print(str(store.count())+" records")

        except socket.timeout:
            # handle the socket timeout by displaying a message and then
            # return to processing messages.

            # This may be due to solar panels not generating electricity
            # as it is dark, which powers down the data logger. So perform end of day actions
            store.end_of_day()
