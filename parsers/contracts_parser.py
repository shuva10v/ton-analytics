#!/usr/bin/env python

import logging
from bitarray import bitarray
import codecs
import argparse
import csv
from tqdm import tqdm
from tvm_valuetypes.cell import Cell, deserialize_boc

class BitReader:
        
    def __init__(self, array: bitarray):
        self.array = array
        
    def read_bits(self, count):
        assert len(self.array) >= count, f"Array size: {len(self.array)}, requested {count}"
        bits = self.array[:count]
        self.array = self.array[count:]
        return bits

    # based on pytonlib: https://github.com/toncenter/pytonlib/blob/main/pytonlib/utils/address.py
    @staticmethod
    def calc_crc(message):
        poly = 0x1021
        reg = 0
        message += b'\x00\x00'
        for byte in message:
            mask = 0x80
            while(mask > 0):
                reg <<= 1
                if byte & mask:
                    reg += 1
                mask >>= 1
                if reg > 0xffff:
                    reg &= 0xffff
                    reg ^= poly
        return reg.to_bytes(2, "big")
    
    """
    From whitepaper:
    
    addr_none$00 = MsgAddressExt;
    addr_extern$01 len:(## 8) external_address:(len * Bit)
    = MsgAddressExt;
    anycast_info$_ depth:(## 5) rewrite_pfx:(depth * Bit) = Anycast;
    addr_std$10 anycast:(Maybe Anycast)
    workchain_id:int8 address:uint256 = MsgAddressInt;
    addr_var$11 anycast:(Maybe Anycast) addr_len:(## 9)
    workchain_id:int32 address:(addr_len * Bit) = MsgAddressInt;
    _ MsgAddressInt = MsgAddress;
    _ MsgAddressExt = MsgAddress;
    
    """
    def read_address(self):
        addr_type = self.read_bits(2).to01()
        if addr_type == '10': # addr_std
            assert self.read_bits(1).to01() == '0', f"Anycast not supported"
            wc = int(self.read_bits(8).to01(), 2)
            account_id = int(self.read_bits(256).to01(), 2).to_bytes(32, "big")
            tag = b'\xff' if wc == -1 else wc.to_bytes(1, "big")
            addr = b'\x11' + tag + account_id
            return codecs.decode(codecs.encode(addr + BitReader.calc_crc(addr), "base64"), "utf-8").strip()
        elif addr_type == '00': #addr_none
            return None
        else:
            raise Exception(f"Unsupported addr type: {addr_type}")


class Parser:
    """
    Parser name, for cli option --contract
    """
    @staticmethod
    def parser_name():
        raise Exception("Not implemented")
        
    """
    Parse cell and return list of parsed items, order has to be the same as in header method
    """
    def parse(self, cell: Cell) -> list:
        raise Exception("Not implemented")
        
    """
    Names for CSV header
    """
    def header(self) -> list:
        raise Exception("Not implemented")

"""
Domain smart-contract: https://github.com/ton-blockchain/dns-contract/blob/main/func/nft-item.fc
;;  uint256 index
;;  MsgAddressInt collection_address
;;  MsgAddressInt owner_address
;;  cell content
;;  cell domain - e.g contains "alice" (without ending \0) for "alice.ton" domain
;;  cell auction - auction info
;;  int last_fill_up_time

"""
class DomainParser(Parser):
    def __init__(self):
        super().__init__()
        
    @staticmethod
    def parser_name():
        return "domain"
    
    def header(self):
        return ["owner_address", "domain_name"]
    
    def parse(self, cell):
        reader = BitReader(cell.data.data)
        domain_index = reader.read_bits(256).tobytes()
        collection_address = reader.read_address()
        owner_address = reader.read_address()
        domain_name = codecs.decode(cell.refs[1].data.data, "utf-8")
        return [owner_address, domain_name]

if __name__ == '__main__':
    contracts = dict([(klass.parser_name(), klass) for klass in Parser.__subclasses__()])
    parser = argparse.ArgumentParser(
                    prog = 'Contracts Parser',
                    description = 'Parse TON smart-contracts data into meaningfull datasets')
    parser.add_argument('source', help="Source file name, CSV with two fields - id and data (base64)")  
    parser.add_argument('destination', help="Output file name")  
    parser.add_argument('-c', '--contract', required=True, help="Contract type", choices=contracts.keys())
    parser.add_argument('-n', '--no-header', help="Do not write header")
    
    args = parser.parse_args()

    parser = contracts[args.contract]()
    header = parser.header()
    assert len(header) > 1
    with open(args.destination, "w") as out, open(args.source) as src:
        csvreader = csv.reader(src)
        csvwriter = csv.writer(out)
        if not args.no_header:
            header_row = ['id']
            header_row.extend(header)
            csvwriter.writerow(header_row)
        for row_number, row in tqdm(enumerate(csvreader)):
            if row_number == 0:
                continue
            _id, base64_data = row
            try:
                data = codecs.decode(codecs.encode(base64_data, "utf-8"), "base64")
            except:
                logging.warning(f"Skipping row #{line_number}, not a base64 string")
                continue
            cell = deserialize_boc(data)
            try:
                res = parser.parse(cell)
                assert len(res) == len(header), f"Parser returned {res}, wrong size (expecred {len(header)})"
            except:
                logging.error(f"Unable to parse row {row}")
                raise
            res.insert(0, _id)
            csvwriter.writerow(res)
    
    