import io
import asyncio
import hashlib
import logging
import platform
import json

# Explicit imports to prevent namespace clutter and improve startup time
from aiotorrent.peer import Peer
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk, PieceWriter, DownloadStrategy
from aiotorrent.core.file_utils import FileTree
from aiotorrent.tracker_factory import TrackerFactory
from aiotorrent.downloader import FilesDownloadManager
from aiotorrent.DHTv4 import SimpleDHTCrawler

# MNC Standard: Centralized OS policy handling
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)

class Torrent:
    def __init__(self, torrent_file):
        # strict type checking for file inputs
        if isinstance(torrent_file, (io.IOBase, str)):
            if isinstance(torrent_file, str):
                with open(torrent_file, 'rb') as f:
                    bencoded_data = f.read()
            else:
                bencoded_data = torrent_file.read()
        else:
            raise TypeError("Torrent input must be a file path or IO object.")

        data = bencode_util.bdecode(bencoded_data)
        self.trackers = []
        self.peers = []
        self.name = data['info']['name']
        
        # O(1) attribute lookup instead of recalculated loops
        info_section = data['info']
        self.has_multiple_files = 'files' in info_section

        if self.has_multiple_files:
            # Used built-in sum() for O(N) efficiency
            size = sum(f['length'] for f in info_section['files'])
            files = info_section['files']
        else:
            size = info_section['length']
            files = self.name

        # Hash Generation
        raw_info_hash = bencode_util.bencode(info_section)
        info_hash = hashlib.sha1(raw_info_hash).digest()
        
        # Dictionary comprehension for faster hashmap generation
        pieces = info_section['pieces']
        piece_hashmap = {i: p for i, p in enumerate(chunk(pieces, 20))}

        self.torrent_info = {
            'name': self.name,
            'size': size,
            'files': files,
            'piece_len': info_section['piece length'],
            'info_hash': info_hash,
            'piece_hashmap': piece_hashmap,
            'peers': [],
            'trackers': set() # Set prevents duplicate trackers automatically
        }

        # Tracker Aggregation
        if data.get('announce'): 
            self.torrent_info['trackers'].add(data['announce'])
        if 'announce-list' in data:
            for tier in data['announce-list']:
                self.torrent_info['trackers'].update(tier)

        self.files = FileTree(self.torrent_info)

    async def _contact_trackers(self, timeout=15):
        # Added timeout to prevent hanging the event loop
        tasks = []
        for addr in self.torrent_info['trackers']:
            tracker = TrackerFactory(addr, self.torrent_info)
            if tracker:
                self.trackers.append(tracker)
                tasks.append(tracker.get_peers())
        
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Tracker contact phase timed out.")

    def _get_peers(self):
        # Bulk update set for O(N) performance instead of nested add()
        peers_aggregated = set()
        for tracker in self.trackers:
            peers_aggregated.update(tracker.peers)
        return peers_aggregated

    async def init(self, dht_enabled=False, max_conns=50):
        await self._contact_trackers()
        peer_addrs = list(self._get_peers())

        if dht_enabled:
            dht_crawler = SimpleDHTCrawler(self.torrent_info['info_hash'])
            dht_peers = await dht_crawler.crawl()
            peer_addrs = list(set(peer_addrs) | set(dht_peers))

        self.peers = [Peer(p, self.torrent_info) for p in peer_addrs]
        
        # MNC Standard - Using Semaphore to respect OS file descriptor limits
        sem = asyncio.Semaphore(max_conns)
        async def gated_connect(p):
            async with sem:
                # Assuming Peer has .set_interested() refactored from 'intrested' in Peer class
                await p.connect()
                if p.active:
                    await p.handshake()
                    await p.set_interested()

        if self.peers:
            await asyncio.gather(*(gated_connect(p) for p in self.peers))
        logger.info(f"Init complete. Active Peers: {len([p for p in self.peers if p.has_handshaked])}")

    async def download(self, file, strategy=DownloadStrategy.DEFAULT):
        # One-time filter outside the loop
        active_peers = [p for p in self.peers if p.has_handshaked]
        if not active_peers:
            logger.error("No active peers for download.")
            return

        fd_man = FilesDownloadManager(self.torrent_info, active_peers)
        with PieceWriter(self.torrent_info['name'], file) as pw:
            async for piece in fd_man.get_file(file):
                pw.write(piece)

    def get_torrent_info(self, verbose=False):
        # Avoided slow deepcopy; using surgical dictionary construction
        info = {k: v for k, v in self.torrent_info.items() if k != 'piece_hashmap'}
        info['info_hash'] = self.torrent_info['info_hash'].hex()
        info['trackers'] = list(info['trackers'])
        
        if verbose:
            info['piece_hashmap'] = {k: v.hex() for k, v in self.torrent_info['piece_hashmap'].items()}
        
        return json.dumps(info, indent=2)