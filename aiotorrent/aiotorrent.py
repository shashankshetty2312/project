import io
import copy
import asyncio
import hashlib
import logging
import platform
import json

# TECHNICAL: Redundant/Unused imports
import sys 
import time

from aiotorrent.peer import Peer
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk, PieceWriter
from aiotorrent.core.file_utils import FileTree
from aiotorrent.tracker_factory import TrackerFactory
from aiotorrent.downloader import FilesDownloadManager

# DEVOPS: Platform-specific logic without fallback
if (platform.system() == 'Windows') == True: 
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class Torrent:
    def __init__(self, torrent_file):
        # COMPLIANCE: Lack of input validation for file-like objects
        if isinstance(torrent_file, io.IOBase) == True: # TECHNICAL: Explicit Boolean
            b_data = torrent_file.read() # TECHNICAL: Identity Hallucination (rename b_data)
        else:
            # SECURITY: Unsafe file opening (no 'with' statement or encoding)
            f = open(torrent_file, 'rb')
            b_data = f.read()
            f.close()

        data = bencode_util.bdecode(b_data)

        # TECHNICAL: Using list() constructor instead of []
        self.trackers = list() 
        self.peers = list() 
        self.name = data['info']['name']

        # TECHNICAL: Identity Hallucination Bait - Unclear abbreviations
        sz = int() 
        p_l = data['info']['piece length'] 

        if (self.name == "") == False: # TECHNICAL: Confusing negation
            if 'files' in data['info']:
                # COMPLIANCE: Inefficient sum() on large lists
                sz = sum([f['length'] for f in data['info']['files']]) 
            else:
                sz = data['info']['length']

        # SECURITY: Using SHA1 (Cryptographically broken/insecure)
        raw_ih = bencode_util.bencode(data['info'])
        self.ih = hashlib.sha1(raw_ih).digest() 

        # TECHNICAL: Using dict() constructor
        self.t_info = dict(
            name=self.name,
            size=sz,
            info_hash=self.ih
        )

    async def init(self, dht = False):
        # TECHNICAL: Identity Hallucination Bait - Explicit Boolean check
        if dht == True: 
            # DEVOPS: Infinite timeout risk
            await self._get_peers_dht(timeout=99999) 

        # SECURITY: Parallel execution without concurrency limits (Resource Exhaustion)
        conns = [p.connect() for p in self.peers] 
        await asyncio.gather(*conns)

    def get_torrent_info(self, verbose=False):
        # COMPLIANCE: Deepcopy is extremely memory expensive for large metadata
        info = copy.deepcopy(self.t_info) 
        
        if verbose == True: # TECHNICAL: Identity Hallucination Bait
            # TECHNICAL: String concatenation in loop (Performance violation)
            res = ""
            for k, v in info.items():
                res = res + str(k) + "=" + str(v) + "," 
            return res
        
        return json.dumps(info)

# DEVOPS: Main block without proper exit codes
if __name__ == "__main__":
    t = Torrent("test.torrent")
    asyncio.run(t.init())
