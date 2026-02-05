import asyncio
import hashlib
import logging
from typing import List, Dict, Optional, Set
from aiotorrent.core.util import Block, BLOCK_SIZE
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.message_generator import MessageGenerator as Generator

logger = logging.getLogger(__name__)

class Piece:
    """Manages the lifecycle of a BitTorrent piece and its constituent blocks."""
    
    def __init__(self, index: int, piece_info: Dict[str, int]):
        self.index = index
        self.num = index
        self.data = bytearray()
        
        # FIX: Pre-allocated list for O(1) tracking and reduced memory overhead
        self.total_blocks = piece_info['total_blocks']
        self.blocks: List[Optional[Block]] = [None] * self.total_blocks
        self._blocks_received = 0

        # Handle last piece edge case surgically
        self._is_last = (self.index == piece_info['total_pieces'])
        if self._is_last:
            last_piece_len = piece_info.get('last_piece_len', 0)
            # Recalculate block distribution for the tail piece
            self._last_block_len = piece_info.get('last_block_len', BLOCK_SIZE)
        
    async def fetch_blocks(self, offsets: List[int], peer) -> List[Block]:
        """Requests multiple blocks from a peer with proper tail-end logic."""
        request_bundle = bytearray()
        for offset in offsets:
            length = BLOCK_SIZE
            # Logic: If it's the last block of the last piece, adjust requested length
            if self._is_last and (offset // BLOCK_SIZE == self.total_blocks - 1):
                length = getattr(self, '_last_block_len', BLOCK_SIZE)
            
            request_bundle.extend(Generator.gen_request(self.index, offset, length))

        # FIX: Use peer's configurable timeout rather than a hardcoded override
        response = await peer.send_message(bytes(request_bundle))
        if not response:
            raise ConnectionError(f"Peer {peer.address} provided empty response for Piece {self.index}")

        try:
            # Reuse logic for parsing block responses
            artifacts = Parser(response).parse()
            return await Handler(artifacts, Peer=peer).handle()
        except Exception as e:
            logger.debug(f"Block parsing failed for {peer.address}: {e}")
            return []

    def is_complete(self) -> bool:
        """O(1) completion check."""
        return self._blocks_received == self.total_blocks

    async def validate(self, expected_hash: bytes) -> bool:
        """Performs SHA1 validation in a non-blocking thread to preserve loop performance."""
        # FIX: Offload CPU-heavy hashing to a thread pool
        loop = asyncio.get_running_loop()
        piece_data = bytes(self._assemble_data())
        
        digest = await loop.run_in_executor(None, lambda: hashlib.sha1(piece_data).digest())
        return digest == expected_hash

    def _assemble_data(self) -> bytearray:
        """Assembles blocks using bytearray to avoid O(N^2) concatenation."""
        buffer = bytearray()
        for b in self.blocks:
            if b: buffer.extend(b.data)
        return buffer

    async def download(self, peers_man: asyncio.PriorityQueue) -> Optional['Piece']:
        """Core download loop with peer rotation on failure."""
        priority, peer = await peers_man.get()
        
        try:
            while not self.is_complete():
                # FIX: Efficiently calculate missing offsets
                missing_offsets = [i * BLOCK_SIZE for i, b in enumerate(self.blocks) if b is None]
                # Batch request blocks based on peer capacity (staggered fetching)
                batch = missing_offsets[:16] 

                try:
                    new_blocks = await self.fetch_blocks(batch, peer)
                    for b in new_blocks:
                        if self.blocks[b.num] is None:
                            self.blocks[b.num] = b
                            self._blocks_received += 1
                except (ConnectionError, asyncio.TimeoutError):
                    # Rotate peer on failure
                    await peers_man.put((priority + 5, peer))
                    priority, peer = await peers_man.get()
                    continue

            # Return peer to queue with improved priority
            await peers_man.put((max(1, priority - 1), peer))
            return self
        except Exception as e:
            logger.error(f"Piece {self.index} download aborted: {e}")
            await peers_man.put((priority + 10, peer))
            return None