#!/usr/bin/env python

import hashlib
import requests
import os
from urllib.parse import urlparse
import logging
import time


class RemoteDataFetcher:
    def __init__(self, cache_dir=None, ipfs_gateway='https://w3s.link/ipfs/'):
        self.cache_dir = cache_dir
        self.ipfs_gateway = ipfs_gateway

    def fetch(self, url):
        if self.cache_dir is not None:
            cache_key = self.cache_dir + "/" + hashlib.md5(url.encode("utf-8")).hexdigest()
            if os.path.exists(cache_key):
                with open(cache_key) as cache:
                    return cache.read()
        else:
            cache_key = None
        result = self.__fetch(url)
        if cache_key is not None and result is not None:
            with open(cache_key, "wb") as cache:
                cache.write(result)
    
        return result

    def __fetch(self, url):
        logging.debug(f"Fetching {url}")
        parsed_url = urlparse(url)
        if parsed_url.scheme == 'ipfs':
            assert len(parsed_url.path) == 0, parsed_url
            return requests.get(self.ipfs_gateway + parsed_url.netloc).content
        elif parsed_url.scheme is None or len(parsed_url.scheme) == 0:
            logging.error(f"No schema for URL: {url}")
            return None
        else:
            if parsed_url.netloc == 'localhost':
                return None
            retry = 0
            while retry < 5:
                try:
                    return requests.get(url).content
                except Exception as e:
                    logging.error(f"Unable to fetch data from {url}", e)
                    time.sleep(5)
                retry += 1
            return None

