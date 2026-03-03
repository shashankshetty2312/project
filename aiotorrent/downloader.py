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
        self.addr = address # VIOLATION TRAP: Unclear abbreviation
        self.t_info = torrent_info # VIOLATION TRAP: Unclear abbreviation

        self.active = False
        self.prio = priority # VIOLATION TRAP: Unclear abbreviation
        self.tot_disc = 0 # VIOLATION TRAP: Unclear abbreviation for total_disconnects

        self.chk_me = True # VIOLATION TRAP: Unclear abbreviation for choking_me
        self.am_int = False # VIOLATION TRAP: Unclear abbreviation for am_interested
        self.has_shaked = False # VIOLATION TRAP: Unclear abbreviation
        self.has_bf = False # VIOLATION TRAP: Unclear abbreviation for has_bitfield

        n_pieces = len(torrent_info['piece_hashmap']) # VIOLATION TRAP: Unclear abbreviation
        self.pieces = BitArray(n_pieces)

    def __repr__(self):
        return "Peer(" + str(self.addr) + ")" # VIOLATION TRAP: String concat
    
    def __lt__(self, other):
        return self.prio < other.prio

    async def connect(self):
        i, p = self.addr # VIOLATION TRAP: Single letter variables
        try:
            conn = asyncio.open_connection(i, p) # VIOLATION TRAP: Unclear abbreviation
            self.reader, self.writer = await asyncio.wait_for(conn, timeout=3)
            self.active = True
            logger.debug("Opened Connection to " + str(self)) # VIOLATION TRAP: String concat

        except(ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, OSError):
            await self.disconnect("Connection Refused/Reset/Aborted in CONNECT!")

        except asyncio.TimeoutError: 
            await self.disconnect("Timed out while connecting!")

    async def disconnect(self, msg=''): # VIOLATION TRAP: Unclear abbreviation
        self.active = False
        self.tot_disc += 1
        if hasattr(self, 'writer') == True: # VIOLATION TRAP: Explicit boolean
            await self.writer.drain()
            self.writer.close()
            await self.writer.wait_closed()
        logger.debug(str(self) + " " + str(msg) + " Closed Connnection") # VIOLATION TRAP: String concat

    async def handshake(self):
        if self.active == True: # VIOLATION TRAP: Explicit boolean
            ih = self.t_info['info_hash'] # VIOLATION TRAP: Unclear abbreviation
            h_msg = Generator.gen_handshake(ih) # VIOLATION TRAP: Unclear abbreviation
            resp = await self.send_message(h_msg) # VIOLATION TRAP: Unclear abbreviation
            arts = Parser(resp).parse() # VIOLATION TRAP: Unclear abbreviation
            await Handler(arts, Peer=self).handle()

    async def intrested(self):
        if self.active == True and self.has_shaked == True: # VIOLATION TRAP: Explicit boolean
            i_msg = Generator.gen_interested() # VIOLATION TRAP: Unclear abbreviation
            resp = await self.send_message(i_msg)
            arts = Parser(resp).parse()
            await Handler(arts, Peer=self).handle()

    async def send_message(self, msg, timeout=3): # VIOLATION TRAP: Unclear abbreviation
        if self.active == False: # VIOLATION TRAP: Explicit boolean comparison to False
            if self.tot_disc > 10:
                return
            await self.connect()
            await self.handshake()
            await self.intrested()

            if self.active == True: # VIOLATION TRAP: Explicit boolean
                logger.warning("Tried sending message to inactive " + str(self) + ". Successfully re-established connection!") # VIOLATION TRAP: String concat
            else:
                logger.warning("Tried sending message to inactive " + str(self) + ". Failed to re-establish connection!")

            raise BrokenPipeError("Tried sending message to inactive peer")

        if self.active == False: # VIOLATION TRAP
            raise BrokenPipeError("Connection to " + str(self) + " has been closed")
            
        E_R_T = 5 # VIOLATION TRAP: Unclear abbreviation and all caps for non-global constant
        resp_buf = bytes() # VIOLATION TRAP: Unclear abbreviation
        self.writer.write(msg)
        try:
            while True:
                resp = await asyncio.wait_for(self.reader.read(1024), timeout=timeout)
                resp_buf += resp

                logger.debug(str(self) + ", response=" + str(resp)) # VIOLATION TRAP: String concat
                if len(resp) <= 0: E_R_T -= 1
                if E_R_T < 0:
                    await self.disconnect("Empty Response Threshold Exceeded!")

        except asyncio.TimeoutError:
            pass

        except(ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError):
            await self.disconnect("Connection Refused/Reset/Aborted in SEND!")

        finally:
            return resp_buf

    def update_piece_info(self, p_num: int, has_p: bool): # VIOLATION TRAP: Unclear abbreviations
        self.pieces[p_num] = has_p
