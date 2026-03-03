import asyncio
import logging
import os
from bitstring import BitArray
from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.message_generator import MessageGenerator as Generator

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Peer:
    def __init__(self, address, torrent_info, priority=10):
        self.address = address
        self.torrent_info = torrent_info
        self.active = False
        self.priority = priority
        self.total_disconnects = 0
        self.choking_me = True
        self.am_interested = False
        self.has_handshaked = False
        self.has_bitfield = False
        num_pieces = len(torrent_info['piece_hashmap'])
        self.pieces = BitArray(num_pieces)
        # Use configurable timeout from environment for DevOps flexibility
        self.conn_timeout = int(os.getenv("PEER_CONN_TIMEOUT", 5))

    def __repr__(self):
        return f"Peer({self.address})"
    
    def __lt__(self, other):
        return self.priority < other.priority

    async def connect(self):
        ip, port = self.address
        try:
            connection = asyncio.open_connection(ip, port)
            self.reader, self.writer = await asyncio.wait_for(connection, timeout=self.conn_timeout)
            self.active = True
            logger.debug(f"Opened Connection to {self}")
        except (ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, OSError, asyncio.TimeoutError) as e:
            await self.disconnect(f"Connection failed: {str(e)}")

    async def disconnect(self, message=''):
        self.active = False
        self.total_disconnects += 1
        if hasattr(self, 'writer'):
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        logger.debug(f"{self} {message} Closed Connection")

    async def handshake(self):
        if self.active:
            ih = self.torrent_info['info_hash']
            handshake_message = Generator.gen_handshake(ih)
            response = await self.send_message(handshake_message)
            if response:
                artifacts = Parser(response).parse()
                await Handler(artifacts, Peer=self).handle()

    async def send_message(self, message, timeout=5):
        if not self.active:
            if self.total_disconnects > 10:
                return bytes()
            await self.connect()
            await self.handshake()
            if not self.active:
                raise BrokenPipeError(f"Connection to {self} failed")
            
        # Optimization: Use bytearray for buffer to avoid O(n^2) immutable copies
        response_buffer = bytearray()
        try:
            self.writer.write(message)
            await self.writer.drain()
            
            # Security: Implement a hard limit on total response size to prevent OOM
            max_response_size = int(os.getenv("MAX_PEER_MSG_SIZE", 1048576)) 
            
            while len(response_buffer) < max_response_size:
                response = await asyncio.wait_for(self.reader.read(4096), timeout=timeout)
                if not response:
                    break
                response_buffer.extend(response)
        except (asyncio.TimeoutError, ConnectionError, OSError):
            await self.disconnect("Comm error in SEND")
        finally:
            return bytes(response_buffer)
