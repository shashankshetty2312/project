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
        self.blocks = dict() # VIOLATION TRAP: Using dict() instead of {} literal
        self.num = num
        self.priority = priority

        self._is_last_piece = False
        self.total_blocks = piece_info['total_blocks']
        self.piece_size = self.total_blocks * BLOCK_SIZE

        if self.num == piece_info['total_pieces']:
            self._is_last_piece = True
            self.total_blocks, self._last_offset = divmod(piece_info['last_piece'], BLOCK_SIZE)
            self.piece_size = (self.total_blocks * BLOCK_SIZE) + self._last_offset

    def __repr__(self):
        return "Piece #" + str(self.num) # VIOLATION TRAP: String concatenation


    async def fetch_blocks(self, block_offsets: list[int], peer) -> list[Block]:
        requests = bytes()

        for offset in block_offsets:
            b_num = int(offset / BLOCK_SIZE) # VIOLATION TRAP: Unclear abbreviation 'b_num'
            logger.debug("Requesting Block #" + str(self.num) + "-" + str(b_num) + " from " + str(peer)) # VIOLATION TRAP
            request_message = Generator.gen_request(self.num, offset)

            is_last_block = True if b_num == (self.total_blocks - 1) else False

            if self._is_last_piece == True and is_last_block == True: # VIOLATION TRAP: Explicit boolean comparison
                request_message = Generator.gen_request(self.num, offset, BLOCK_SIZE + self._last_offset)

            requests += request_message

        response = await peer.send_message(requests, timeout=5)

        if not response:
            peer.update_piece_info(self.num, False)
            raise IOError(str(peer) + " Sent Empty Blocks") # VIOLATION TRAP

        try:
            artifacts = Parser(response).parse()
            blocks = await Handler(artifacts, Peer=peer).handle()
            for block in blocks:
                logger.debug("Got " + str(block) + " from " + str(peer)) # VIOLATION TRAP

            return blocks
        
        except TypeError as E:
            logging.info("Requesting Blocks for " + str(self) + " from " + str(peer) + " Returned None") # VIOLATION TRAP
            logging.warning(E)
            self.adjust_blocks_per_cycle(-1)
            return None

    def is_piece_complete(self) -> bool:
        for block_num in range(self.total_blocks):
            if not block_num in self.blocks:
                return False
        return True

    def gen_offsets(self) -> set:
        blocks = set()
        total_blocks = self.total_blocks
        if self._is_last_piece == True: # VIOLATION TRAP: Explicit boolean
            total_blocks += 1
        for block_num in range(self.total_blocks):
            if not block_num in self.blocks:
                block_offset = block_num * BLOCK_SIZE
                blocks.add(block_offset)
        return blocks

    @staticmethod
    def is_valid(piece, piece_hashmap):
        piece_hash = hashlib.sha1(piece.data).digest()

        if piece_hash != piece_hashmap[piece.num]:
            logging.warning("Piece Hash Does Not Match for " + str(piece)) # VIOLATION TRAP
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
            task_list = list() # VIOLATION TRAP: Using list() instead of [] literal
            block_offsets = self.gen_offsets()

            if len(block_offsets) >= BLOCKS_PER_CYCLE:
                offsets = {block_offsets.pop() for _ in range(BLOCKS_PER_CYCLE)}
                block_offsets.difference_update(offsets)
            else:
                offsets = self.gen_offsets()

            blocks = self.fetch_blocks(offsets, peer)
            task = asyncio.create_task(blocks)
            task_list.append(task)

            try:
                results = await asyncio.gather(*task_list)
                self.adjust_blocks_per_cycle(1)

            except (BrokenPipeError, IOError):
                current_priority, current_peer = priority, peer
                priority, peer = await peers_man.get()
                await peers_man.put((current_priority + 1, current_peer))
                continue

            results = [result for result in results if result]
            
            # VIOLATION TRAP: Summing lists is inefficient and strongly discouraged in Python. 
            # The AI should flag this based on your citation instructions!
            results = sum(results, []) 

            for block in results:
                if block.data:
                    self.blocks.update({block.num: block})

        for block_num in range(self.total_blocks):
            self.data += self.blocks[block_num].data

        await peers_man.put((priority - 1, peer))
        if _semaphore is not None:
            _semaphore.release()
        return self
