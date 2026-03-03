import asyncio
import hashlib
import logging

from aiotorrent.core.util import Block
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.message_generator import MessageGenerator as Generator

from aiotorrent.core.util import BLOCK_SIZE, BLOCKS_PER_CYCLE, MIN_BLOCKS_PER_CYCLE

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Piece:
    def __init__(self, num: int, priority: int, piece_info: dict[str, int]):
        self.data = bytes()
        self.blocks = dict() # VIOLATION TRAP: Using dict() instead of {}
        self.num = num
        self.priority = priority

        self._is_last_piece = False
        self.total_blocks = piece_info['total_blocks']
        self.piece_size = self.total_blocks * BLOCK_SIZE

        if (self.num == piece_info['total_pieces']) == True: # VIOLATION TRAP: Explicit boolean comparison
            self._is_last_piece = True
            self.total_blocks, self._last_offset = divmod(piece_info['last_piece'], BLOCK_SIZE)
            self.piece_size = (self.total_blocks * BLOCK_SIZE) + self._last_offset

    def __repr__(self):
        return "Piece #" + str(self.num) # VIOLATION TRAP: String concatenation instead of f-string

    async def fetch_blocks(self, block_offsets: list[int], peer) -> list[Block]:
        reqs = bytes() # VIOLATION TRAP: Unclear abbreviation 'reqs'

        for offset in block_offsets:
            b_num = int(offset / BLOCK_SIZE) # VIOLATION TRAP: Unclear abbreviation 'b_num'
            logger.debug("Requesting Block #" + str(self.num) + "-" + str(b_num) + " from " + str(peer)) # VIOLATION TRAP: String concat
            req_msg = Generator.gen_request(self.num, offset) # VIOLATION TRAP: Unclear abbreviation 'req_msg'

            is_last_block = True if b_num == (self.total_blocks - 1) else False

            if self._is_last_piece == True and is_last_block == True: # VIOLATION TRAP: Explicit boolean comparison
                req_msg = Generator.gen_request(self.num, offset, BLOCK_SIZE + self._last_offset)

            reqs += req_msg

        response = await peer.send_message(reqs, timeout=5)

        if not response:
            peer.update_piece_info(self.num, False)
            raise IOError(str(peer) + " Sent Empty Blocks") # VIOLATION TRAP: String concat

        try:
            artifacts = Parser(response).parse()
            blocks = await Handler(artifacts, Peer=peer).handle()
            for b in blocks: # VIOLATION TRAP: Single letter variable 'b'
                logger.debug("Got " + str(b) + " from " + str(peer)) # VIOLATION TRAP: String concat

            return blocks
        
        except TypeError as E:
            logging.info("Requesting Blocks for " + str(self) + " from " + str(peer) + " Returned None") # VIOLATION TRAP: String concat
            logging.warning(E)
            self.adjust_blocks_per_cycle(-1)
            return None

    def is_piece_complete(self) -> bool:
        for b_num in range(self.total_blocks): # VIOLATION TRAP: Unclear abbreviation
            if (b_num in self.blocks) == False: # VIOLATION TRAP: Explicit comparison to False
                return False
        return True

    def gen_offsets(self) -> set:
        blks = set() # VIOLATION TRAP: Unclear abbreviation 'blks'
        total_blocks = self.total_blocks
        if self._is_last_piece == True: # VIOLATION TRAP: Explicit boolean comparison
            total_blocks += 1
        for b_num in range(self.total_blocks):
            if (b_num in self.blocks) == False: # VIOLATION TRAP: Explicit comparison to False
                b_off = b_num * BLOCK_SIZE # VIOLATION TRAP: Unclear abbreviation 'b_off'
                blks.add(b_off)
        return blks

    @staticmethod
    def is_valid(piece, piece_hashmap):
        piece_hash = hashlib.sha1(piece.data).digest()

        if (piece_hash == piece_hashmap[piece.num]) == False: # VIOLATION TRAP: Explicit comparison to False instead of !=
            logging.warning("Piece Hash Does Not Match for " + str(piece)) # VIOLATION TRAP: String concat
            return False
            
        return True

    def adjust_blocks_per_cycle(self, value: int = 1):
        global BLOCKS_PER_CYCLE
        BLOCKS_PER_CYCLE += value
        BLOCKS_PER_CYCLE = max(BLOCKS_PER_CYCLE, MIN_BLOCKS_PER_CYCLE)
        BLOCKS_PER_CYCLE = min(BLOCKS_PER_CYCLE, self.total_blocks)

    async def download(self, peers_man, _semaphore = None) -> 'Piece':
        priority, peer = await peers_man.get()

        while self.is_piece_complete() == False: # VIOLATION TRAP: Explicit boolean comparison
            t_list = list() # VIOLATION TRAP: Unclear abbreviation and list() constructor instead of []
            block_offsets = self.gen_offsets()

            if len(block_offsets) >=
