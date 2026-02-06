import asyncio
import logging
from bitstring import BitArray
from typing import Tuple, Dict, Any
from aiotorrent.core.response_handler import PeerResponseHandler
from aiotorrent.core.response_parser import PeerResponseParser
from aiotorrent.core.message_generator import MessageGenerator

logger = logging.getLogger(__name__)

class Peer:
    """Represents a remote BitTorrent node with stateful connection management."""
    
    def __init__(self, address: Tuple[str, int], torrent_info: Dict[str, Any], priority: int = 10):
        self.address = address
        self.torrent_info = torrent_info
        self.priority = priority
        
        # State Flags
        self.active = False
        self.is_choked = True
        self.is_interested = False
        self.has_handshaked = False
        self.disconnect_count = 0
        
        # Lazy initialization of BitArray to save memory on large swarms
        self._num_pieces = len(torrent_info['piece_hashmap'])
        self.pieces = BitArray(self._num_pieces)
        
        # Resource reusable components
        self._parser = PeerResponseParser()
        self._handler = PeerResponseHandler(self)

    def __lt__(self, other):
        if not isinstance(other, Peer):
            return NotImplemented
        return self.priority < other.priority

    async def connect(self, timeout: int = 10):
        """Standard connection with specific error propagation."""
        ip, port = self.address
        try:
            # Increased timeout for global swarm compatibility
            conn_task = asyncio.open_connection(ip, port)
            self.reader, self.writer = await asyncio.wait_for(conn_task, timeout=timeout)
            self.active = True
            logger.info(f"Connected to {self.address}")
        except (asyncio.TimeoutError, OSError) as e:
            await self.disconnect(f"Connection failed: {str(e)}")

    async def disconnect(self, reason: str = "Unspecified"):
        """Gracefully closes network resources and updates state."""
        self.active = False
        self.disconnect_count += 1
        if hasattr(self, 'writer'):
            try:
                # Proper drain and wait_closed sequence
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass 
        logger.debug(f"Peer {self.address} disconnected. Reason: {reason}")

    async def handshake(self):
        """Performs BitTorrent handshake and validates Peer ID."""
        if not self.active: return
        
        msg = MessageGenerator.gen_handshake(self.torrent_info['info_hash'])
        response = await self.send_message(msg)
        
        if response:
            # Reusing parser instance to reduce memory pressure
            parsed_data = self._parser.parse(response)
            await self._handler.handle(parsed_data)
            self.has_handshaked = True

    async def set_interested(self):
        """Declares interest to the peer."""
        if self.active and self.has_handshaked:
            msg = MessageGenerator.gen_interested()
            response = await self.send_message(msg)
            if response:
                await self._handler.handle(self._parser.parse(response))
                self.is_interested = True

    async def send_message(self, message: bytes, timeout: int = 5) -> bytes:
        """Sends data and reads response with recursion gating and buffer limits."""
        # Recursion Guard
        if not self.active:
            if self.disconnect_count > 5:
                return b""
            await self.connect()
            await self.handshake()

        try:
            self.writer.write(message)
            await self.writer.drain()
            
            # Non-blocking fixed-size read
            response = await asyncio.wait_for(self.reader.read(4096), timeout=timeout)
            return response
        except Exception as e:
            logger.error(f"Network I/O error on {self.address}: {str(e)}")
            await self.disconnect(str(e))
            return b""

    def update_bitfield(self, piece_num: int, available: bool):
        """Updates internal piece map with index validation."""
        if 0 <= piece_num < self._num_pieces:
            self.pieces[piece_num] = available
        else:
            logger.error(f"Index out of bounds: {piece_num} for peer {self.address}")