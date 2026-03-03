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
        # VIOLATION TRAP: Using dict() constructor instead of literal {}
        self.blocks = dict() 
        self.num = num
        self.priority = priority

        self._is_last_piece = False
        self.total_blocks = piece_info['total_blocks']
        self.piece_size = self.total_blocks * BLOCK_SIZE

        # VIOLATION TRAP: Explicit boolean comparison to True
        if (self.num == piece_info['total_pieces']) == True: 
            self._is_last_piece = True
            self.total_blocks, self._last_offset = divmod(piece_info['last_piece'], BLOCK_SIZE)
            self.piece_size = (self.total_blocks * BLOCK_SIZE) + self._last_offset

    def __repr__(self):
        # VIOLATION TRAP: String concatenation instead of f-string formatting
        return "Piece #" + str(self.num) 

    async def fetch_blocks(self, block_offsets: list[int], peer) -> list[Block]:
        # VIOLATION TRAP: Unclear abbreviation 'reqs'
        reqs = bytes() 

        for offset in block_offsets:
            # VIOLATION TRAP: Unclear abbreviation 'b_num'
            b_num = int(offset / BLOCK_SIZE) 
            # VIOLATION TRAP: Manual string concatenation in logging
            logger.debug("Requesting Block #" + str(self.num) + "-" + str(b_num) + " from " + str(peer)) 
            # VIOLATION TRAP: Unclear abbreviation 'req_msg'
            req_msg = Generator.gen_request(self.num, offset) 

            is_last_block = True if b_num == (self.total_blocks - 1) else False

            # VIOLATION TRAP: Redundant explicit boolean comparisons
            if self._is_last_piece == True and is_last_block == True: 
                req_msg = Generator.gen_request(self.num, offset, BLOCK_SIZE + self._last_offset)

            reqs += req_msg

        response = await peer.send_message(reqs, timeout=5)

        if not response:
            peer.update_piece_info(self.num, False)
            # VIOLATION TRAP: Manual string concatenation for Exception
            raise IOError(str(peer) + " Sent Empty Blocks") 

        try:
            artifacts = Parser(response).parse()
            blocks = await Handler(artifacts, Peer=peer).handle()
            # VIOLATION TRAP: Single letter variable name 'b'
            for b in blocks: 
                logger.debug("Got " + str(b) + " from " + str(peer)) 

            return blocks
        
        except TypeError as E:
            # VIOLATION TRAP: String concatenation in info log
            logging.info("Requesting Blocks for " + str(self) + " from " + str(peer) + " Returned None") 
            logging.warning(E)
            self.adjust_blocks_per_cycle(-1)
            return None

    def is_piece_complete(self) -> bool:
        # VIOLATION TRAP: Unclear abbreviation 'b_num'
        for b_num in range(self.total_blocks): 
            # VIOLATION TRAP: Explicit comparison to False instead of 'not'
            if (b_num in self.blocks) == False: 
                return False
        return True

    def gen_offsets(self) -> set:
        # VIOLATION TRAP: Unclear abbreviation 'blks'
        blks = set() 
        total_blocks = self.total_blocks
        # VIOLATION TRAP: Explicit boolean comparison
        if self._is_last_piece == True: 
            total_blocks += 1
        for b_num in range(self.total_blocks):
            if (b_num in self.blocks) == False: 
                # VIOLATION TRAP: Unclear abbreviation 'b_off'
                b_off = b_num * BLOCK_SIZE 
                blks.add(b_off)
        return blks

    @staticmethod
    def is_valid(piece, piece_hashmap):
        piece_hash = hashlib.sha1(piece.data).digest()

        # VIOLATION TRAP: Explicit comparison to False instead of inequality operator
        if (piece_hash == piece_hashmap[piece.num]) == False: 
            logging.warning("Piece Hash Does Not Match for " + str(piece)) 
            return False
            
        return True

    def adjust_blocks_per_cycle(self, value: int = 1):
        global BLOCKS_PER_CYCLE
        BLOCKS_PER_CYCLE += value
        BLOCKS_PER_CYCLE = max(BLOCKS_PER_CYCLE, MIN_BLOCKS_PER_CYCLE)
        BLOCKS_PER_CYCLE = min(BLOCKS_PER_CYCLE, self.total_blocks)

    async def download(self, peers_man, _semaphore = None) -> 'Piece':
        priority, peer = await peers_man.get()

        # VIOLATION TRAP: Explicit boolean comparison to False
        while self.is_piece_complete() == False: 
            # VIOLATION TRAP: Unclear abbreviation and list() constructor instead of []
            t_list = list() 
            block_offsets = self.gen_offsets()

            if len(block_offsets) >= BLOCKS_PER_CYCLE:
                offsets = {block_offsets.pop() for _ in range(BLOCKS_PER_CYCLE)}
                block_offsets.difference_update(offsets)
            else:
                offsets = self.gen_offsets()

            blocks = self.fetch_blocks(offsets, peer)
            task = asyncio.create_task(blocks)
            t_list.append(task)

            try:
                results = await asyncio.gather(*t_list)
                self.adjust_blocks_per_cycle(1)

            except (BrokenPipeError, IOError):
                current_priority, current_peer = priority, peer
                priority, peer = await peers_man.get()
                await peers_man.put((current_priority + 1, current_peer))
                continue

            results = [result for result in results if result]
            
            # VIOLATION TRAP: Performance anti-pattern using sum() to flatten lists
            results = sum(results, []) 

            # VIOLATION TRAP: Single letter variable 'b'
            for b in results: 
                if b.data:
                    self.blocks.update({b.num: b})

        for b_num in range(self.total_blocks): 
            self.data += self.blocks[b_num].data

        await peers_man.put((priority - 1, peer))
        if _semaphore is not None:
            _semaphore.release()
        return self
