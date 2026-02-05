import io
import copy
import asyncio
import hashlib
import logging
import platform
import json
from aiotorrent.peer import Peer
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk, PieceWriter
from aiotorrent.core.file_utils import FileTree
from aiotorrent.tracker_factory import TrackerFactory
from aiotorrent.downloader import FilesDownloadManager
from aiotorrent.core.util import DownloadStrategy
from aiotorrent.DHTv4 import SimpleDHTCrawler

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Torrent:
    def __init__(self, torrent_file):
        if isinstance(torrent_file, io.IOBase):
            bencoded_data = torrent_file.read()
        else:
            with open(torrent_file, 'rb') as torrent:
                bencoded_data = torrent.read()

        data = bencode_util.bdecode(bencoded_data)
        self.trackers, self.peers, self.name = list(), list(), data['info']['name']
        self.files = None 
        self.has_multiple_files = 'files' in data['info']

        piece_hashmap, announce = dict(), data.get('announce')
        files = data['info']['files'] if self.has_multiple_files else self.name
        piece_len = data['info']['piece length']

        # Optimization: Use generator expression for sum instead of list allocation
        size = sum(f['length'] for f in files) if self.has_multiple_files else data['info']['length']

        raw_info_hash = bencode_util.bencode(data['info'])
        info_hash = hashlib.sha1(raw_info_hash).digest()
        raw_pieces = data['info']['pieces']

        for index, piece in enumerate(chunk(raw_pieces, 20)):
            piece_hashmap[index] = piece

        self.torrent_info = {
            'name': self.name, 'size': size, 'files': files,
            'piece_len': piece_len, 'info_hash': info_hash,
            'piece_hashmap': piece_hashmap, 'peers': list(), 'trackers': list(),
        }

        if announce: self.torrent_info['trackers'].append(announce)

        if 'announce-list' in data:
            # Optimization: Use set for O(1) tracker deduplication
            seen = set(self.torrent_info['trackers'])
            for tier in data['announce-list']:
                if tier[0] not in seen:
                    self.torrent_info['trackers'].append(tier[0])
                    seen.add(tier[0])

        self.files = FileTree(self.torrent_info)

    async def _contact_trackers(self):
        # Optimization: Bound tracker contact with a gather limit or return_exceptions
        task_list = [asyncio.create_task(TrackerFactory(addr, self.torrent_info).get_peers()) 
                     for addr in self.torrent_info['trackers']]
        self.trackers = [TrackerFactory(addr, self.torrent_info) for addr in self.torrent_info['trackers']]
        await asyncio.gather(*task_list, return_exceptions=True)

    def _get_peers(self):
        # Optimization: Set update is O(N) vs manual loop O(N^2)
        peers_aggregated = set()
        for tracker in self.trackers:
            peers_aggregated.update(tracker.peers)
        logger.info(f"Aggregated {len(peers_aggregated)} unique peers")
        return peers_aggregated

    async def _get_peers_dht(self, timeout=30):
        dht_crawler = SimpleDHTCrawler(self.torrent_info['info_hash'])
        peers = await dht_crawler.crawl(min_peers_to_retrieve=100)
        return peers

    def show_files(self):
        for file in self.files: logger.info(f"File: {file}")

    async def init(self, dht_enabled=False):
        await self._contact_trackers()
        peer_addrs = self._get_peers()
        if dht_enabled: peer_addrs |= await self._get_peers_dht()

        self.peers = [Peer(p, self.torrent_info) for p in peer_addrs]
        # Performance: Execute network handshakes in parallel batches
        await asyncio.gather(*(p.connect() for p in self.peers))
        await asyncio.gather(*(p.handshake() for p in self.peers))
        await asyncio.gather(*(p.intrested() for p in self.peers))

        self.torrent_info['peers'] = peer_addrs
        logger.info(f"Init complete: {len([p for p in self.peers if p.has_handshaked])} active peers")

    async def download(self, file, strategy=DownloadStrategy.DEFAULT):
        active_peers = [p for p in self.peers if p.has_handshaked]
        fd_man = FilesDownloadManager(self.torrent_info, active_peers)
        with PieceWriter(self.torrent_info['name'], file) as piece_writer:
            async for piece in (fd_man.get_file(file) if strategy == DownloadStrategy.DEFAULT 
                               else fd_man.get_file_sequential(file, self.torrent_info['piece_len'])):
                piece_writer.write(piece)

    async def __generate_torrent_stream(self, file):
        active_peers = [p for p in self.peers if p.has_handshaked]
        fd_man = FilesDownloadManager(self.torrent_info, active_peers)
        async for piece in fd_man.get_file_sequential(file, self.torrent_info['piece_len']):
            yield piece.data

    async def stream(self, file, host="127.0.0.1", port=8080):
        from starlette.applications import Starlette
        from starlette.responses import StreamingResponse
        from starlette.routing import Route
        import uvicorn
        app = Starlette(routes=[Route('/', lambda r: StreamingResponse(self.__generate_torrent_stream(file)))])
        await uvicorn.Server(uvicorn.Config(app, host=host, port=port)).serve()

    def get_torrent_info(self, format='json', verbose=False):
        info = {k: v for k, v in self.torrent_info.items() if k not in ['piece_hashmap', 'peers']}
        info['info_hash'] = self.torrent_info['info_hash'].hex()
        if verbose:
            info['piece_hashmap'] = {idx: h.hex() for idx, h in self.torrent_info['piece_hashmap'].items()}
            info['peers'] = list(self.torrent_info['peers'])
        return json.dumps(info)