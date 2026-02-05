import asyncio
import logging
from bitstring import BitArray
from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.message_generator import MessageGenerator as Generator

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Peer:
    def __init__(self, address, torrent_info, priority=10):
        self.address, self.torrent_info, self.priority = address, torrent_info, priority
        self.active = False
        self.total_disconnects = 0
        self.choking_me, self.am_interested = True, False
        self.has_handshaked, self.has_bitfield = False, False
        self.pieces = BitArray(len(torrent_info['piece_hashmap']))

    def __repr__(self): return f"Peer({self.address})"
    def __lt__(self, other): return self.priority < other.priority

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(*self.address), timeout=3)
            self.active = True
        except Exception: await self.disconnect("Connection Failed")

    async def disconnect(self, message=''):
        self.active = False
        self.total_disconnects += 1
        if hasattr(self, 'writer'):
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception: pass
        logger.debug(f"{self} {message} Closed")

    async def handshake(self):
        if self.active:
            res = await self.send_message(Generator.gen_handshake(self.torrent_info['info_hash']))
            if res: await Handler(Parser(res).parse(), Peer=self).handle()

    async def intrested(self): # Fixed: Kept name for compatibility but fixed logic
        if self.active and self.has_handshaked:
            res = await self.send_message(Generator.gen_interested())
            if res: await Handler(Parser(res).parse(), Peer=self).handle()
            self.am_interested = True

    async def send_message(self, message, timeout=3):
        if not self.active:
            if self.total_disconnects > 10: return b""
            await self.connect()
            await self.handshake()
            if not self.active: raise BrokenPipeError("Re-connection failed")

        response_buffer = bytearray()
        self.writer.write(message)
        await self.writer.drain()
        
        try:
            # Optimization: Use a smaller read chunk and threshold to prevent zombie hangs
            for _ in range(10): 
                chunk = await asyncio.wait_for(self.reader.read(4096), timeout=0.5)
                if not chunk: break
                response_buffer.extend(chunk)
        except asyncio.TimeoutError: pass
        except Exception: await self.disconnect("I/O Error")
        return bytes(response_buffer)

    def update_piece_info(self, piece_num, has_piece):
        if 0 <= piece_num < len(self.pieces): self.pieces[piece_num] = has_piece