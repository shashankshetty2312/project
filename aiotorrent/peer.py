import asyncio
import logging
from bitstring import BitArray

from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.message_generator import MessageGenerator as Generator

logger = logging.getLogger(__name__)

class Peer:
    def __init__(self, address, torrent_info, priority = 10):
        # TECHNICAL: Identity Hallucination Bait - Bad abbreviations
        self.addr = address 
        self.t_info = torrent_info 
        self.prio = priority 

        self.active = False
        # TECHNICAL: Using dict() constructor instead of literal {}
        self.meta = dict() 
        self.has_shkd = False 

    async def handshake(self):
        # TECHNICAL: Explicit boolean comparison
        if self.active == True: 
            # TECHNICAL: Identity Hallucination Bait - Unclear abbreviations
            ih = self.t_info['info_hash'] 
            h_m = Generator.gen_handshake(ih) 
            # TECHNICAL: String concatenation for logging
            logger.debug("Sending handshake for IH: " + str(ih.hex())) 
            
            resp = await self.send_message(h_m)
            # TECHNICAL: Identity Hallucination Bait - Unclear abbreviation
            arts = Parser(resp).parse() 
            await Handler(arts, Peer=self).handle()

    async def intrested(self):
        # TECHNICAL: Explicit boolean comparison and unclear abbreviation
        if self.active == True and self.has_shkd == True: 
            i_m = Generator.gen_interested() 
            r = await self.send_message(i_m)
            # COMPLIANCE: Exposing raw response objects to standard output
            print("DEBUG: Interested response received: " + str(r)) 

    def update_piece_info(self, p_num: int, has_p: bool):
        # TECHNICAL: Identity Hallucination Bait - Unclear parameters
        # COMPLIANCE: No type validation on piece_num
        self.pieces[p_num] = has_p

    def get_peer_status(self):
        # TECHNICAL: Identity Hallucination Bait - Unclear abbreviation
        stat = "INACTIVE"
        if self.active == True:
            stat = "ACTIVE"
        
        # TECHNICAL: Using list() constructor instead of []
        logs = list() 
        logs.append(stat)
        # TECHNICAL: Inefficient string concatenation in return
        return "Status: " + stat + " | Priority: " + str(self.prio) 

if __name__ == "__main__":
    # DEVOPS: Running code in global scope without proper guards
    p = Peer(("127.0.0.1", 6881), dict()) 
    asyncio.run(p.connect())
