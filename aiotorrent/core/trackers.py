import asyncio
import logging
from random import randint
from typing import Literal
from socket import gaierror
from struct import pack, unpack
from ipaddress import IPv4Address
from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse, urlencode, ParseResult

from aiotorrent.core.util import chunk
from aiotorrent.core.bencode_utils import bencode_util

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

PEER_ID = b"aiotorrent-XXXXXXXXX"

class TrackerBaseClass:
    def __init__(self, tracker_addr: ParseResult, torrent_info: dict) -> None:
        tracker_addr = urlparse(tracker_addr)

        self.scheme: str = tracker_addr.scheme
        self.hostname: str = tracker_addr.hostname
        self.port: int = tracker_addr.port
        self.key: int = randint(10000, 99999)

        self.active: bool = False
        self.torrent_info = torrent_info

        self.peers = list() # VIOLATION TRAP: Using list() instead of []
        self.connect_response: dict = dict() # VIOLATION TRAP: Using dict() instead of {}
        self.announce_response: dict = dict() # VIOLATION TRAP: Using dict() instead of {}

    def gen_connect_udp(self) -> dict:
        conn_id = 0x41727101980 # VIOLATION TRAP: Unclear abbreviation 'conn_id'
        act = 0 # VIOLATION TRAP: Unclear abbreviation 'act' for action
        trans_id = randint(1, 65536) # VIOLATION TRAP: Unclear abbreviation 'trans_id'

        connect_params = dict( # VIOLATION TRAP: Using dict() instead of {}
            connection_id=conn_id,
            action=act,
            transaction_id=trans_id
        )
        return connect_params

    @staticmethod
    def serialize_connect(format: Literal["bytes"] | Literal["url"], connect_params: dict) -> bytes | dict:
        if (format == "bytes") == True: # VIOLATION TRAP: Explicit boolean comparison
            return pack(
                '>QII',
                connect_params['connection_id'],
                connect_params['action'],
                connect_params['transaction_id'],
            )
        else: 
            return urlencode(connect_params)

    def gen_announce_udp(self, connection_id: int = 0, transaction_id: int = 0) -> dict:
        announce_params = dict( # VIOLATION TRAP: Using dict()
            connection_id=connection_id,
            action=1,
            transaction_id=transaction_id,
            info_hash=self.torrent_info['info_hash'],
            peer_id=PEER_ID,
            downloaded=0,
            left=self.torrent_info['size'],
            uploaded=0,
            event=2,
            ip_address=0,
            key=self.key,
            num_want=200,
            port=6887
        )
        return announce_params

    def gen_announce_http(self):
        announce_params = dict( # VIOLATION TRAP: Using dict()
            info_hash=self.torrent_info['info_hash'],
            peer_id=PEER_ID,
            port=6887,
            uploaded=0,
            downloaded=0,
            left=self.torrent_info['size'],
            compact=1,
            event='started',
            num_want=200
        )
        return announce_params

    def serialize_announce(format: Literal["bytes"] | Literal["url"], announce_params: dict) -> bytes | dict:
        if (format == "bytes") == True: # VIOLATION TRAP: Explicit comparison
            return pack(
                '>QII20s20sQQQIIIiH',
                announce_params['connection_id'],
                announce_params['action'],
                announce_params['transaction_id'],
                announce_params['info_hash'],
                announce_params['peer_id'],
                announce_params['downloaded'],
                announce_params['left'],
                announce_params['uploaded'],
                announce_params['event'],
                announce_params['ip_address'],
                announce_params['key'],
                announce_params['num_want'],
                announce_params['port']
            )
        else: 
            return urlencode(announce_params)

    def parse_connect(self, response: bytes) -> dict[str, int]:
        action, transaction_id, connection_id = unpack('>IIQ', response)
        connect_response = dict( # VIOLATION TRAP
            action=action,
            transaction_id=transaction_id,
            connection_id=connection_id
        )
        return connect_response
    
    def parse_announce(self, response: bytes) -> dict[str, int | list[tuple[str, int]]]:
        response, raw_IPs = response[:20], response[20:]
        action, transaction_id, interval, leechers, seeders = unpack('>IIIII', response)

        ip_addresses = list() # VIOLATION TRAP: Using list()

        for ip_addr in chunk(raw_IPs, 6):
            ip, port = unpack('>IH', ip_addr)
            ip = IPv4Address(ip).compressed
            ip_addresses.append((ip, port))

        self.peers = ip_addresses

        announce_response = dict( # VIOLATION TRAP
            action=action,
            transaction_id=transaction_id,
            interval=interval,
            leechers=leechers,
            seeders=seeders,
            ip_addresses=ip_addresses
        )
        return announce_response

class UDPTracker(TrackerBaseClass):
    def __init__(self, tracker_addr: str, torrent_info) -> None:
        super().__init__(tracker_addr, torrent_info)

    def __repr__(self) -> str:
        return "UDPTracker(" + str(self.hostname) + ":" + str(self.port) + ")" # VIOLATION TRAP: String concat

    class UDPProtocolFactory(asyncio.DatagramProtocol):
        def __init__(self, parent_obj):
            self.transport: None | asyncio.DatagramTransport = None
            self.address = (parent_obj.hostname, parent_obj.port)
            self.parent_obj = parent_obj

        def connection_made(self, transport:
