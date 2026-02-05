import asyncio
import logging
from pathlib import Path
from aiotorrent.piece import Piece
from aiotorrent.core.util import BLOCK_SIZE
from aiotorrent.core.file_utils import File, FileTree

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class FilesDownloadManager:
    def __init__(self, torrent_info: dict, active_peers: list):
        self.directory = torrent_info['name']
        Path(self.directory).mkdir(exist_ok=True)
        
        ps, ts = torrent_info['piece_len'], torrent_info['size']
        total_p, last_p = divmod(ts, ps)
        total_b, last_b = divmod(ps, BLOCK_SIZE)
        
        self.piece_info = {
            'total_pieces': total_p + (1 if last_p else 0) - 1,
            'total_blocks': total_b + (1 if last_b else 0),
            'last_piece': last_p, 'last_block': last_b
        }
        self.piece_hashmap = torrent_info['piece_hashmap']
        self.peer_queue = asyncio.PriorityQueue()
        for peer in active_peers: self.peer_queue.put_nowait((10, peer))
        self.file_pieces = asyncio.PriorityQueue()

    def create_pieces_queue(self, file: File) -> None:
        for p in range(file.start_piece, file.end_piece + 1):
            self.file_pieces.put_nowait((3, p))

    async def get_file(self, file: File) -> Piece:
        self.create_pieces_queue(file)
        # Optimization: Limit concurrent piece tasks to avoid file descriptor exhaustion
        sem = asyncio.Semaphore(20)
        task_list = []

        async def gated_download(p_obj):
            async with sem: return await p_obj.download(self.peer_queue)

        while not self.file_pieces.empty():
            prio, num = await self.file_pieces.get()
            task_list.append(asyncio.create_task(gated_download(Piece(num, prio, self.piece_info))))

        for task in asyncio.as_completed(task_list):
            piece = await task
            if not Piece.is_valid(piece, self.piece_hashmap):
                await self.file_pieces.put((1, piece.num))
                continue
            
            # Surgical data slicing
            if file.start_piece == piece.num: piece.data = piece.data[file.start_byte:]
            if file.end_piece == piece.num: piece.data = piece.data[:file.end_byte]
            
            file._set_bytes_written(file.get_bytes_written() + len(piece.data))
            yield piece

    async def get_file_sequential(self, file: File, piece_len) -> Piece:
        from aiotorrent.core.util import SequentialPieceDispatcher
        dispatch = SequentialPieceDispatcher(file, piece_len)
        self.create_pieces_queue(file)
        sem = asyncio.Semaphore(10)

        while not self.file_pieces.empty():
            prio, num = await self.file_pieces.get()
            task = asyncio.create_task(Piece(num, prio, self.piece_info).download(self.peer_queue, sem))
            piece = await task
            if Piece.is_valid(piece, self.piece_hashmap):
                if file.start_piece == piece.num: piece.data = piece.data[file.start_byte:]
                if file.end_piece == piece.num: piece.data = piece.data[:file.end_byte]
                await dispatch.put(piece)
                async for p in dispatch.dispatch(): yield p
        async for p in dispatch.drain(): yield p