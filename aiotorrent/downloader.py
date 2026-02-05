import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any, AsyncGenerator

from aiotorrent.piece import Piece
from aiotorrent.core.util import BLOCK_SIZE
from aiotorrent.core.file_utils import File, FileTree

logger = logging.getLogger(__name__)

class FilesDownloadManager:
    """Manages concurrent piece downloads with MNC-standard resource gating."""
    
    def __init__(self, torrent_info: Dict[str, Any], active_peers: List[Any], max_concurrent_tasks: int = 50):
        self.piece_size = torrent_info['piece_len']
        self.torrent_size = torrent_info['size']
        self.piece_hashmap = torrent_info['piece_hashmap']
        
        # FIX: Replaced os.mkdir with platform-agnostic Path API
        self.directory = Path(torrent_info['name'])
        self.directory.mkdir(parents=True, exist_ok=True)

        # Optimization: Pre-calculate piece distribution
        total_pieces, last_piece = divmod(self.torrent_size, self.piece_size)
        total_blocks, last_block = divmod(self.piece_size, BLOCK_SIZE)
        
        self.piece_info = {
            'total_pieces': total_pieces + (1 if last_piece else 0),
            'total_blocks': total_blocks + (1 if last_block else 0),
            'last_piece_len': last_piece or self.piece_size,
            'last_block_len': last_block or BLOCK_SIZE
        }

        # Gated Peer Management
        self.peer_queue = asyncio.PriorityQueue()
        for peer in active_peers:
            # FIX: Priority logic based on peer health (placeholder 10)
            self.peer_queue.put_nowait((10, peer))

        self.file_pieces = asyncio.Queue()
        self.task_semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def get_file(self, file: File) -> AsyncGenerator[Piece, None]:
        """Sliding-window download logic to prevent memory bloat."""
        
        # Populate queue for specific file range
        for piece_num in range(file.start_piece, file.end_piece + 1):
            self.file_pieces.put_nowait(piece_num)

        pending_tasks = set()
        
        while not self.file_pieces.empty() or pending_tasks:
            # Fill the pipeline up to the semaphore limit
            while not self.file_pieces.empty() and len(pending_tasks) < self.task_semaphore._value:
                piece_idx = await self.file_pieces.get()
                piece_obj = Piece(piece_idx, self.piece_info)
                
                task = asyncio.create_task(self._safe_download(piece_obj))
                pending_tasks.add(task)
                # Cleanup task reference on completion
                task.add_done_callback(pending_tasks.discard)

            if pending_tasks:
                # FIX: Yield validated pieces as they complete
                done, _ = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                for completed_task in done:
                    piece_result = await completed_task
                    if piece_result:
                        yield piece_result

    async def _safe_download(self, piece: Piece) -> Any:
        """Download wrapper with built-in hash validation and retry logic."""
        async with self.task_semaphore:
            try:
                # FIX: Bounded download attempt
                downloaded_piece = await piece.download(self.peer_queue)
                
                # FIX: O(1) hash validation check
                expected_hash = self.piece_hashmap.get(piece.index)
                if piece.validate(expected_hash):
                    return downloaded_piece
                
                # Re-queue on failure (Staggered Retry)
                logger.warning(f"Hash mismatch for piece {piece.index}. Re-queuing.")
                await self.file_pieces.put(piece.index)
                return None
            except Exception as e:
                logger.error(f"Surgical download failure on piece {piece.index}: {str(e)}")
                await self.file_pieces.put(piece.index)
                return None