#!/usr/bin/env python

import logging
import codecs
import argparse
import csv
import requests
import os
import json
import psycopg2
from tqdm import tqdm
from tvm_valuetypes.cell import Cell, deserialize_boc
from functools import lru_cache

from bitreader import BitReader
from datafetcher import RemoteDataFetcher

"""
Base class for code provider
"""
class CodeProvider:
    """
    Gets code for contract by code hash
    """
    def get(self, code_hash: str) -> str:
        raise Exception("Not implemented")


class PostgresCodeProvider(CodeProvider):
    def __init__(self, uri):
        self.conn = psycopg2.connect(uri)

    @lru_cache
    def get(self, code_hash):
        with self.conn.cursor() as cursor:
            cursor.execute(f"select code from code where hash = '{code_hash}'")
            res = cursor.fetchone()
            assert res is not None, f"Unable to get code for hash {code_hash}"
            return res[0]


class ContractExecutor:
    def __init__(self, executor_url: str, code_provider: CodeProvider):
        self.executor_url = executor_url
        self.code_provider = code_provider

    def execute(self, code_hash, data, method, types):
        code = self.code_provider.get(code_hash)
        res = requests.post(self.executor_url, json={'code': code, 'data': data, 'method': method, 'expected': types})
        assert res.status_code == 200, "Error during contract executor call: %s" % res
        res = res.json()
        if res['exit_code'] != 0:
            logging.error("Non-zero exit code: %s" % res)
            return None
        return res['result']


class Parser:
    def __init__(self):
        self.fetcher: RemoteDataFetcher = None
        self.executor: ContractExecutor = None

    def set_fetcher(self, fetcher):
        self.fetcher = fetcher

    def set_executor(self, executor):
        self.executor = executor

    """
    Parser name, for cli option --contract
    """
    @staticmethod
    def parser_name():
        raise Exception("Not implemented")

    """
    Does the parser expect parsed cell or just plain b64 string?
    """
    @staticmethod
    def need_parse_boc():
        return True

    """
    Parse cell and return list of parsed items, order has to be the same as in header method
    """
    def parse(self, cell, code_hash: str = None, _id = None) -> list:
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

    def parse(self, cell, code_hash=None):
        reader = BitReader(cell.data.data)
        reader.read_bits(256).tobytes() # domain_index
        reader.read_address() # collection contract, unused
        owner_address = reader.read_address()
        domain_name = codecs.decode(cell.refs[1].data.data, "utf-8")
        return [owner_address, domain_name]


"""
Based on reference implementation: https://github.com/ton-blockchain/token-contract/blob/main/ft/jetton-minter.fc
"""
class JettonParser(Parser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def parser_name():
        return "jetton"

    @staticmethod
    def need_parse_boc():
        return False

    def header(self):
        return ["name", "symbol", "image", "image_data", "decimals", "metadata_url", "admin_address", "total_supply",
                "mintable", "wallet_hash", "description"]

    def parse(self, cell, code_hash=None, _id=None):
        """
        TEP-74: get_jetton_data() returns (
        int total_supply,
        int mintable,
        slice admin_address,
        cell jetton_content,
        cell jetton_wallet_code)
        """
        get_jetton_data_res = self.executor.execute(code_hash, cell, 'get_jetton_data',
                                                    ["int", "int", "address", "metadata", "cell_hash"])
        if get_jetton_data_res is None or len(get_jetton_data_res) != 5:
            logging.error(f"Unable to get jetton data for {_id}")
            return None
        total_supply, mintable, admin_address, jetton_content, wallet_hash = get_jetton_data_res
        logging.info(total_supply, mintable, admin_address, jetton_content, wallet_hash)
        if jetton_content is None or 'content_layout' not in jetton_content:
            logging.warning(f"No content layout extracted: {jetton_content}, id: {_id}")
            metadata_url = None
            metadata = {}
        else:
            if jetton_content['content_layout'] == 'off-chain':
                metadata_url = jetton_content['content']
                metadata_json = self.fetcher.fetch(metadata_url)
                try:
                    metadata = json.loads(metadata_json)
                except Exception as e:
                    logging.error(f"Failed to parse metadata for {metadata_url}: {e}")
                    metadata = {}
                metadata['content_layout'] = jetton_content['content_layout']
            else:
                metadata = jetton_content['content']
                metadata_url = None

        return [metadata.get('name', None), metadata.get('symbol', None), metadata.get('image', None),
                metadata.get('image_data', None), metadata.get('decimals', None), metadata_url, admin_address,
                total_supply, mintable, wallet_hash, metadata.get('description', None)]


"""
TEP-74
"""
class JettonWalletParser(Parser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def parser_name():
        return "jetton-wallet"

    @staticmethod
    def need_parse_boc():
        return False

    def header(self):
        return ["balance", "owner", "jetton"]

    def parse(self, cell, code_hash=None, _id=None):
        """
        TEP-74: get_wallet_data() returns (
        int balance,
        slice owner,
        slice jetton,
        cell jetton_wallet_code)
        """
        get_jetton_data_res = self.executor.execute(code_hash, cell, 'get_wallet_data',
                                                    ["int", "address", "address", "cell_hash"])
        if get_jetton_data_res is None:
            logging.error(f"Unable to get jetton data for {_id}")
            return None
        balance, owner, jetton, _ = get_jetton_data_res

        return [balance, owner, jetton]


if __name__ == '__main__':
    contracts = dict([(klass.parser_name(), klass) for klass in Parser.__subclasses__()])
    parser = argparse.ArgumentParser(
        prog='Contracts Parser',
        description='Parse TON smart-contracts data into meaningfull datasets')
    parser.add_argument('source', help="Source file name, CSV with two fields - id and data (base64)")
    parser.add_argument('destination', help="Output file name")
    parser.add_argument('-c', '--contract', required=True, help="Contract type", choices=contracts.keys())
    parser.add_argument('-n', '--no-header', help="Do not write header")
    parser.add_argument('-d', '--cache-dir', help="Cache directory for off-chain requests data")
    parser.add_argument('-e', '--executor', help="Contract executor url")

    args = parser.parse_args()

    parser = contracts[args.contract]()
    parser.set_fetcher(RemoteDataFetcher(args.cache_dir))
    parser.set_executor(ContractExecutor(args.executor, PostgresCodeProvider(os.getenv("POSTGRES_URI"))))

    header = parser.header()
    assert len(header) > 1
    with open(args.destination, "w") as out, open(args.source) as src:
        csvreader = csv.reader(src)
        csvwriter = csv.writer(out, delimiter='\t')
        if not args.no_header:
            header_row = ['id']
            header_row.extend(header)
            csvwriter.writerow(header_row)
        for row_number, row in tqdm(enumerate(csvreader)):
            if row_number == 0:
                continue
            _id, code_hash, base64_data = row
            if parser.need_parse_boc():
                try:
                    data = codecs.decode(codecs.encode(base64_data, "utf-8"), "base64")
                except:
                    logging.warning(f"Skipping row #{line_number}, not a base64 string")
                    continue
                cell = deserialize_boc(data)
            else:
                cell = base64_data
            try:
                res = parser.parse(cell, code_hash, _id)
                if res is None:
                    logging.warning(f"Skipping broken row {_id}")
                    continue
                assert len(res) == len(header), f"Parser returned {res}, wrong size (expecred {len(header)})"
            except:
                logging.error(f"Unable to parse row {row}")
                raise
            res.insert(0, _id)
            csvwriter.writerow(res)
