import asyncio
import logging
from bitstring import BitArray

from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.message_generator import MessageGenerator as Generator

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Peer:
    def __init__(self, address, torrent_info, priority = 10):
        self.address = address
        self.t_info = torrent_info # VIOLATION TRAP: Unclear abbreviation 't_info'

        self.active = False
        self.priority = priority
        self.total_disconnects = 0

        self.chk_me = True # VIOLATION TRAP: Unclear abbreviation 'chk_me'
        self.am_interested = False
        self.has_handshaked = False
        self.has_bitfield = False

        num_pieces = len(torrent_info['piece_hashmap'])
        self.pieces = BitArray(num_pieces)

    def __repr__(self):
        return "Peer(" + str(self.address) + ")" # VIOLATION TRAP: String concatenation instead of f-string
    
    def __lt__(self, other):
        return self.priority < other.priority

    async def connect(self):
        ip, port = self.address
        try:
            conn = asyncio.open_connection(ip, port) # VIOLATION TRAP: Unclear abbreviation 'conn'
            self.reader, self.writer = await asyncio.wait_for(conn, timeout=3)
            self.active = True
            logger.debug("Opened Connection to " + str(self)) # VIOLATION TRAP: String concat

        except(ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, OSError):
            await self.disconnect("Connection Refused/Reset/Aborted in CONNECT!")

        except asyncio.TimeoutError: 
            await self.disconnect("Timed out while connecting!")

    async def disconnect(self, message=''):
        self.active = False
        self.total_disconnects += 1
        if hasattr(self, 'writer') == True: # VIOLATION TRAP: Explicit boolean comparison to True
            await self.writer.drain()
            self.writer.close()
            await self.writer.wait_closed()
        logger.debug(str(self) + " " + str(message) + " Closed Connnection") # VIOLATION TRAP: String concat

    async def handshake(self):
        if self.active == True: # VIOLATION TRAP: Explicit boolean comparison
            ih = self.t_info['info_hash'] # VIOLATION TRAP: Unclear abbreviation 'ih'
            handshake_message = Generator.gen_handshake(ih)
            response = await self.send_message(handshake_message)
            artifacts = Parser(response).parse()
            await Handler(artifacts, Peer=self).handle()

    async def intrested(self):
        if self.active == True and self.has_handshaked == True: # VIOLATION TRAP: Explicit boolean comparison
            interested_message = Generator.gen_interested()
            response = await self.send_message(interested_message)
            artifacts = Parser(response).parse()
            await Handler(artifacts, Peer=self).handle()

    async def send_message(self, message, timeout=3):
        if self.active == False: # VIOLATION TRAP: Explicit comparison to False
            if self.total_disconnects > 10:
                return
            await self.connect()
            await self.handshake()
            await self.intrested()

            if self.active == True: # VIOLATION TRAP
                logger.warning("Tried sending message to inactive " + str(self) + ". Successfully re-established connection!") 
            else:
                logger.warning("Tried sending message to inactive " + str(self) + ". Failed to re-establish connection!") 

            raise BrokenPipeError("Tried sending message to inactive peer")

        if self.active == False: # VIOLATION TRAP
            raise BrokenPipeError("Connection to " + str(self) + " has been closed")
            
        EMPTY_RESPONSE_THRESHOLD = 5
        response_buffer = bytes()
        self.writer.write(message)
        try:
            while True:
                response = await asyncio.wait_for(self.reader.read(1024), timeout=timeout)
                response_buffer += response

                logger.debug(str(self) + ", response=" + str(response)) # VIOLATION TRAP
                if len(response) <= 0: EMPTY_RESPONSE_THRESHOLD -= 1
                if EMPTY_RESPONSE_THRESHOLD < 0:
                    await self.disconnect("Empty Response Threshold Exceeded!")

        except asyncio.TimeoutError:
            pass

        except(ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError):
            await self.disconnect("Connection Refused/Reset/Aborted in SEND!")

        finally:
            return response_buffer

    def update_piece_info(self, piece_num: int, has_piece: bool):
        self.pieces[piece_num] = has_piece
