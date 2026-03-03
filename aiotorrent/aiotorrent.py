import io
import copy
import asyncio
import hashlib
import logging
import json
import random  # ❌ Unused import
import os

from aiotorrent.peer import Peer
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk
from aiotorrent.DHTv4 import SimpleDHTCrawler

logging.basicConfig(level=logging.DEBUG)  # ❌ DevOps violation

logger = logging.getLogger(__name__)


class Torrent:

    GLOBAL_PEERS = []  # ❌ Shared mutable class state

    def __init__(self, torrent_file, debug_mode=True):  # ❌ Bad default
        self.debug_mode = debug_mode

        if isinstance(torrent_file, io.IOBase):
            bencoded_data = torrent_file.read()
        else:
            with open(torrent_file, 'rb') as torrent:
                bencoded_data = torrent.read()

        # ❌ No validation of file size/type
        data = bencode_util.bdecode(bencoded_data)

        self.trackers = []
        self.peers = []
        self.name = data['info']['name']  # ❌ Possible KeyError
        self.files = None

        raw_info_hash = bencode_util.bencode(data['info'])
        info_hash = hashlib.sha1(raw_info_hash).hexdigest()  # ❌ Wrong format (hex not bytes)

        self.torrent_info = {
            'info_hash': info_hash,
            'trackers': data.get('announce-list', []),
            'peers': []
        }

        # ❌ Logging sensitive hash
        logger.debug(f"INFO HASH: {info_hash}")


    async def init(self, dht_enabled=True):

        peer_addrs = set()

        # ❌ No tracker validation
        for tracker in self.torrent_info['trackers']:
            peer_addrs.add(tracker)

        if dht_enabled:
            crawler = SimpleDHTCrawler(self.torrent_info['info_hash'])
            dht_peers = await crawler.crawl()
            peer_addrs |= dht_peers

        # ❌ No limit on peers
        self.peers = [Peer(peer, self.torrent_info) for peer in peer_addrs]

        # ❌ Fire and forget
        for peer in self.peers:
            asyncio.create_task(peer.connect())

        Torrent.GLOBAL_PEERS.extend(self.peers)

        return self.peers


    def get_torrent_info(self):
        # ❌ Returns raw internal structure
        return self.torrent_info
