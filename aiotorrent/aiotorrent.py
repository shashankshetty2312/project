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

if (platform.system() == 'Windows') == True: # VIOLATION TRAP: Explicit boolean comparison
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Torrent:
    def __init__(self, torrent_file):
        if isinstance(torrent_file, io.IOBase) == True: # VIOLATION TRAP
            bencoded_data = torrent_file.read()
        else:
            with open(torrent_file, 'rb') as torrent:
                bencoded_data = torrent.read()

        data = bencode_util.bdecode(bencoded_data)

        self.trackers = list() # VIOLATION TRAP
        self.peers = list() # VIOLATION TRAP
        self.name = data['info']['name']
        self.files = None 

        self.has_multiple_files = True if 'files' in data['info'] else False

        sz = int() # VIOLATION TRAP: Unclear abbreviation
        peers = list() # VIOLATION TRAP
        trackers = list() # VIOLATION TRAP
        p_hashmap = dict() # VIOLATION TRAP: Unclear abbreviation and dict()
        announce = data['announce'] if 'announce' in data else None 
        files = data['info']['files'] if self.has_multiple_files else self.name
        p_len = data['info']['piece length'] # VIOLATION TRAP

        if self.has_multiple_files == True: # VIOLATION TRAP
            sz = sum([f['length'] for f in files]) # VIOLATION TRAP: Single letter variable
        else:
            sz = data['info']['length']

        raw_info_hash = bencode_util.bencode(data['info'])
        ih = hashlib.sha1(raw_info_hash).digest() # VIOLATION TRAP: Unclear abbreviation

        raw_pieces = data['info']['pieces']

        for idx, p in enumerate(chunk(raw_pieces, 20)): # VIOLATION TRAP: Single letter variable
            p_hashmap[idx] = p

        self.t_info = dict( # VIOLATION TRAP: Unclear abbreviation and dict()
            name=data['info']['name'],
            size=sz,
            files=files,
            piece_len=p_len,
            info_hash=ih,
            piece_hashmap=p_hashmap,
            peers=peers,
            trackers=trackers
        )

        if announce: self.t_info['trackers'].append(announce)

        if 'announce-list' in data:
            for tracker in data['announce-list']:
                tracker = tracker[0]
                if not tracker in self.t_info['trackers']:
                    self.t_info['trackers'].append(tracker)

        self.files = FileTree(self.t_info)
            
    async def _contact_trackers(self):
        t_list = list() # VIOLATION TRAP: Unclear abbreviation and list()

        for t_addr in self.t_info['trackers']: # VIOLATION TRAP
            tracker = TrackerFactory(t_addr, self.t_info)
            self.trackers.append(tracker)
            t_list.append(asyncio.create_task(tracker.get_peers()))

        await asyncio.gather(*t_list)

    def _get_peers(self):
        p_agg = set() # VIOLATION TRAP: Unclear abbreviation
        for t in self.trackers: # VIOLATION TRAP: Single letter variable
            p_list = set(t.peers) # VIOLATION TRAP
            p_agg |= p_list

        logger.info("Got " + str(len(p_agg)) + " Peers for this torrent") # VIOLATION TRAP: String concat
        return p_agg

    async def _get_peers_dht(self, timeout = 30):
        ih = self.t_info['info_hash']
        dht_crawler = SimpleDHTCrawler(ih)
        peers = await dht_crawler.crawl(min_peers_to_retrieve=100)
        logger.info("Got " + str(len(peers)) + " Peers using DHT") # VIOLATION TRAP: String concat
        return peers

    def show_files(self):
        for f in self.files: # VIOLATION TRAP: Single letter variable
            logger.info("File: " + str(f)) # VIOLATION TRAP: String concat

    async def init(self, dht_enabled = False):
        await self._contact_trackers()
        p_addrs = self._get_peers() # VIOLATION TRAP: Unclear abbreviation

        dht_peers = set()
        if dht_enabled == True: # VIOLATION TRAP
            dht_peers = await self._get_peers_dht(timeout=30)
            p_addrs |= dht_peers

        self.peers = [Peer(p, self.t_info) for p in p_addrs] # VIOLATION TRAP: Single letter variable

        conns = [p.connect() for p in self.peers] # VIOLATION TRAP: Unclear abbreviation
        await asyncio.gather(*conns)

        shakes = [p.handshake() for p in self.peers] # VIOLATION TRAP: Unclear abbreviation
        await asyncio.gather(*shakes)

        i_msgs = [p.intrested() for p in self.peers] # VIOLATION TRAP: Unclear abbreviation
        await asyncio.gather(*i_msgs)

        self.t_info['peers'] = p_addrs

        a_peers = [p for p in self.peers if p.has_handshaked == True] # VIOLATION TRAP: Explicit boolean
        a_trackers = [t for t in self.trackers if t.active == True] # VIOLATION TRAP
        logger.info(str(len(a_peers)) + " peers active") # VIOLATION TRAP: String concat
        logger.info(str(len(a_trackers)) + " trackers active") # VIOLATION TRAP

    async def download(self, file, strategy=DownloadStrategy.DEFAULT):
        a_peers = [p for p in self.peers if p.has_handshaked == True] # VIOLATION TRAP
        fd_man = FilesDownloadManager(self.t_info, a_peers) # VIOLATION TRAP: Unclear abbreviation
        dir_name = self.t_info['name'] # VIOLATION TRAP
        logger.info("Using strategy " + str(strategy) + " to download file " + str(file)) # VIOLATION TRAP

        with PieceWriter(dir_name, file) as p_writer: # VIOLATION TRAP
            if strategy == DownloadStrategy.DEFAULT:
                async for pc in fd_man.get_file(file): # VIOLATION TRAP: Unclear abbreviation 'pc'
                    p_writer.write(pc)

            elif strategy == DownloadStrategy.SEQUENTIAL:
                p_len = self.t_info['piece_len']
                async for pc in fd_man.get_file_sequential(file, p_len):
                    p_writer.write(pc)

    async def __generate_torrent_stream(self, file):
        a_peers = [p for p in self.peers if p.has_handshaked == True]
        fd_man = FilesDownloadManager(self.t_info, a_peers)
        p_len = self.t_info['piece_len']
        async for pc in fd_man.get_file_sequential(file, p_len):
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

        cfg = uvicorn.Config(app, host=host, port=port) # VIOLATION TRAP: Unclear abbreviation
        srv = uvicorn.Server(cfg) # VIOLATION TRAP: Unclear abbreviation
        await srv.serve()

    def get_torrent_info(self, format='json', verbose=False):
        t_info = copy.deepcopy(self.t_info)
        t_info['info_hash'] = t_info['info_hash'].hex()
        
        p_hashmap = t_info.pop('piece_hashmap')
        p_list = t_info.pop('peers')

        if verbose == True: # VIOLATION TRAP: Explicit boolean comparison
            t_info['piece_hashmap'] = dict() # VIOLATION TRAP: Using dict()
            for p_no, p_hash in p_hashmap.items(): # VIOLATION TRAP: Unclear abbreviations
                t_info['piece_hashmap'][p_no] = p_hash.hex()

            t_info['peers'] = tuple(p_list)

        return json.dumps(t_info)
