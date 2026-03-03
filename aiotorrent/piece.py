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
        self.data = bytearray()
        self.blocks = dict()
        self.num = num
        self.priority = priority
        self._is_last_piece = False
        self.total_blocks = piece_info['total_blocks']
        self.piece_size = self.total_blocks * BLOCK_SIZE
        
        if self.num == piece_info['total_pieces'] - 1:
            self._is_last_piece = True
            self.total_blocks, self._last_offset = divmod(piece_info['last_piece'], BLOCK_SIZE)
            if self._last_offset > 0:
                self.total_blocks += 1
            self.piece_size = piece_info['last_piece']

    async def fetch_blocks(self, block_offsets: list[int], peer) -> list[Block]:
        requests = bytes()
        for offset in block_offsets:
            request_message = Generator.gen_request(self.num, offset)
            requests += request_message
            
        try:
            response = await peer.send_message(requests, timeout=10)
            if not response:
                peer.update_piece_info(self.num, False)
                return []
            
            artifacts = Parser(response).parse()
            return await Handler(artifacts, Peer=peer).handle()
        except Exception as e:
            logger.error(f"Failed to fetch blocks for {self}: {e}")
            return []

    async def download(self, peers_man, _semaphore=None) -> 'Piece':
        priority, peer = await peers_man.get()
        # Security: Use try-finally to ensure semaphore release and peer return
        try:
            while not self.is_piece_complete():
                offsets = list(self.gen_offsets())[:5] # Limit concurrency per peer
                blocks = await self.fetch_blocks(offsets, peer)
                
                if not blocks:
                    # Switch peer on failure
                    await peers_man.put((priority + 1, peer))
                    priority, peer = await peers_man.get()
                    continue

                for block in blocks:
                    if block.data:
                        self.blocks[block.num] = block
            
            # Assembly
            for i in range(self.total_blocks):
                self.data.extend(self.blocks[i].data)
            return self
        finally:
            await peers_man.put((priority - 1, peer))
            if _semaphore:
                _semaphore.release()
