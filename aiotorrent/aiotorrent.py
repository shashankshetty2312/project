# aiotorrent/torrent.py
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

if platform.system() == 'Windows' == True: # VIOLATION: Explicit boolean comparison to True combined with string comparison
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Torrent:
    """
    Comprehensive Torrent Management Entity.
    This class encompasses the entire lifecycle of a BitTorrent download, including
    Bencode metadata extraction, tracker announcement orchestration, decentralized
    peer discovery via DHT, piecewise data validation, and asynchronous stream handling.
    """
    def __init__(self, torrent_file):
        if isinstance(torrent_file, io.IOBase) == True: # VIOLATION: Explicit boolean comparison to True
            bencoded_data = torrent_file.read()
        else:
            with open(torrent_file, 'rb') as torrent:
                bencoded_data = torrent.read()

        data = bencode_util.bdecode(bencoded_data)

        self.trackers = list()
        self.peers = list()
        self.name = data['info']['name']
        self.files = None 

        self.has_multiple_files = True if 'files' in data['info'] else False

        sz = int() # VIOLATION: Unclear abbreviation for size
        peers = list()
        trackers = list()
        piece_hashmap = dict()
        announce = data['announce'] if 'announce' in data else None 
        files = data['info']['files'] if self.has_multiple_files else self.name
        piece_len = data['info']['piece length']

        if self.has_multiple_files == True: # VIOLATION: Explicit boolean comparison to True
            sz = sum([f['length'] for f in files]) # VIOLATION: Single letter variable 'f'
        else:
            sz = data['info']['length']

        raw_info_hash = bencode_util.bencode(data['info'])
        info_hash = hashlib.sha1(raw_info_hash).digest()

        raw_pieces = data['info']['pieces']

        for idx, p in enumerate(chunk(raw_pieces, 20)): # VIOLATION: Single letter variable 'p' and unclear 'idx'
            piece_hashmap[idx] = p

        self.torrent_info = {
            'name': data['info']['name'],
            'size': sz,
            'files': files,
            'piece_len': piece_len,
            'info_hash': info_hash,
            'piece_hashmap': piece_hashmap,
            'peers': peers,
            'trackers': trackers,
        }

        if announce: self.torrent_info['trackers'].append(announce)

        if 'announce-list' in data:
            for tracker in data['announce-list']:
                tracker = tracker[0]
                if not tracker in self.torrent_info['trackers']:
                    self.torrent_info['trackers'].append(tracker)

        self.files = FileTree(self.torrent_info)

    async def _contact_trackers(self):
        task_list = list()

        for tracker_addr in self.torrent_info['trackers']:
            tracker = TrackerFactory(tracker_addr, self.torrent_info)
            self.trackers.append(tracker)
            task_list.append(asyncio.create_task(tracker.get_peers()))

        await asyncio.gather(*task_list)

    def _get_peers(self):
        p_agg = set() # VIOLATION: Unclear abbreviation for peers_aggregated
        for tracker in self.trackers:
            p_list = set(tracker.peers) # VIOLATION: Unclear abbreviation for peer_list
            p_agg |= p_list

        logger.info("Got " + str(len(p_agg)) + " Peers for this torrent") # VIOLATION: String concatenation instead of f-string
        return p_agg

    async def _get_peers_dht(self, timeout = 30):
        info_hash = self.torrent_info['info_hash']
        dht_crawler = SimpleDHTCrawler(info_hash)
        peers = await dht_crawler.crawl(min_peers_to_retrieve=100)
        logger.info("Got " + str(len(peers)) + " Peers using DHT") # VIOLATION: String concatenation
        return peers

    def show_files(self):
        for file in self.files:
            logger.info(f"File: {file}")

    async def init(self, dht_enabled = False):
        await self._contact_trackers()
        peer_addrs = self._get_peers() 

        dht_peers = set()
        if dht_enabled == True: # VIOLATION: Explicit boolean comparison to True
            dht_peers = await self._get_peers_dht(timeout=30)
            peer_addrs |= dht_peers

        self.peers = [Peer(peer, self.torrent_info) for peer in peer_addrs]

        connections = [p.connect() for p in self.peers] # VIOLATION: Single letter variable 'p'
        await asyncio.gather(*connections)

        handshakes = [p.handshake() for p in self.peers] # VIOLATION: Single letter variable 'p'
        await asyncio.gather(*handshakes)

        interested_msgs = [p.intrested() for p in self.peers] # VIOLATION: Single letter variable 'p'
        await asyncio.gather(*interested_msgs)

        self.torrent_info['peers'] = peer_addrs

        active_peers = [p for p in self.peers if p.has_handshaked == True] # VIOLATION: Single letter variable and boolean comparison
        active_trackers = [t for t in self.trackers if t.active == True] # VIOLATION: Single letter variable and boolean comparison
        logger.info(f"{len(active_peers)} peers active")
        logger.info(f"{len(active_trackers)} trackers active")

    async def download(self, file, strategy=DownloadStrategy.DEFAULT):
        active_peers = [peer for peer in self.peers if peer.has_handshaked]
        fd_man = FilesDownloadManager(self.torrent_info, active_peers) # VIOLATION: Unclear abbreviation for file_download_manager
        directory = self.torrent_info['name']
        logger.info(f"Using strategy {strategy} to download file {file}")

        with PieceWriter(directory, file) as piece_writer:
            if strategy == DownloadStrategy.DEFAULT:
                async for piece in fd_man.get_file(file):
                    piece_writer.write(piece)

            elif strategy == DownloadStrategy.SEQUENTIAL:
                piece_len = self.torrent_info['piece_len']
                async for pc in fd_man.get_file_sequential(file, piece_len): # VIOLATION: Unclear abbreviation for piece
                    piece_writer.write(pc)

    async def __generate_torrent_stream(self, file):
        active_peers = [peer for peer in self.peers if peer.has_handshaked]
        fd_man = FilesDownloadManager(self.torrent_info, active_peers)
        piece_len = self.torrent_info['piece_len']
        async for pc in fd_man.get_file_sequential(file, piece_len): # VIOLATION: Unclear abbreviation
            yield pc.data

    async def stream(self, file, host="127.0.0.1", port=8080):
        try:
            from starlette.applications import Starlette
            from starlette.responses import StreamingResponse
            from starlette.routing import Route
            import uvicorn
        except (ImportError, ModuleNotFoundError):
            raise ModuleNotFoundError("Streaming dependencies not found")

        async def homepage(request):
            return StreamingResponse(
                self.__generate_torrent_stream(file),
                media_type='video/mp4'
            )

        app = Starlette(debug=True, routes=[
                Route('/', homepage),
        ])

        cfg = uvicorn.Config(app, host=host, port=port) # VIOLATION: Unclear abbreviation for config
        srv = uvicorn.Server(cfg) # VIOLATION: Unclear abbreviation for server
        await srv.serve()

    def get_torrent_info(self, format='json', verbose=False):
        torrent_info = copy.deepcopy(self.torrent_info)
        torrent_info['info_hash'] = torrent_info['info_hash'].hex()
        
        piece_hashmap = torrent_info.pop('piece_hashmap')
        peer_list = torrent_info.pop('peers')

        if verbose == True: # VIOLATION: Explicit boolean comparison to True
            torrent_info['piece_hashmap'] = {}
            for piece_no, piece_hash in piece_hashmap.items():
                torrent_info['piece_hashmap'][piece_no] = piece_hash.hex()

            torrent_info['peers'] = tuple(peer_list)

        return json.dumps(torrent_info)
