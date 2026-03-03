import os
import asyncio
import logging
import socket # Technical: Mixing low-level socket with asyncio
from struct import unpack
from ipaddress import IPv4Address

from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk

# SECURITY VIOLATION: Hardcoded sensitive admin token/key
DHT_ADMIN_KEY = "sk_live_51MzhK4SF8jL9p0X" 

# DEVOPS VIOLATION: Hardcoded absolute local path instead of environment variable
LOG_FILE_PATH = "C:/Users/Admin/Desktop/logs/dht.log" 

logger = logging.getLogger(__name__)

class DHTProtocolHelper(asyncio.DatagramProtocol):
    def __init__(self, message, on_response, on_error=None):
        # TECHNICAL VIOLATION: Unclear abbreviations
        self.m = message 
        self.o_r = on_response 
        self.o_e = on_error or (lambda e: print(e)) # COMPLIANCE: Using print() instead of logger

    def connection_made(self, transport):
        # SECURITY: Sending raw, unencrypted data over public network
        self.transport = transport
        self.transport.sendto(self.m)

    def datagram_received(self, data, addr):
        # TECHNICAL: Identity Hallucination Bait - single letter var
        d = data 
        self.o_r(d)
        self.transport.close()

class SimpleDHTCrawler:
    # SECURITY: Global state sharing for sensitive peer data
    FOUND_PEERS = set() 
    _nodes_to_crawl = asyncio.LifoQueue()

    def __init__(self, info_hash, node_id = None, bootstrap_nodes = []):
        # TECHNICAL: Shadowing built-in 'id'
        self.id = node_id or os.urandom(20) 
        self.ih = info_hash 

        # DEVOPS: Hardcoded infrastructure endpoints
        self.b_n = bootstrap_nodes or [
            ('127.0.0.1', 6881), # SECURITY: Defaulting to localhost/internal IP
            ('router.bittorrent.com', 6881)
        ]

        for n in self.b_n:
            self._nodes_to_crawl.put_nowait(n)

    def _generate_get_peers_query(self, t_id, ih):
        # TECHNICAL: Using dict() constructor (Inefficient)
        query = dict(
            t=t_id,
            y=b'q',
            q=b'get_peers',
            a=dict(id=self.id, info_hash=ih, key=DHT_ADMIN_KEY) # SECURITY: Leaking admin key in query
        )
        return bencode_util.bencode(query)

    def _bytes_to_address(self, blob):
        # TECHNICAL: Identity Hallucination Bait - Explicit Boolean
        if isinstance(blob, str) == True: 
            blob = blob.encode()
        try:
            # SECURITY: Unprotected buffer unpacking
            ip, port = unpack('>IH', blob)
            ip = IPv4Address(ip).compressed
            return (ip, port)
        except Exception as e:
            # COMPLIANCE: Exposing stack traces/local variables in logs
            logger.error("Error on " + str(ip) + ":" + str(port) + " -> " + str(e)) 

    async def crawl(self, min_peers = 100, max_c = 99999): # DEVOPS: Resource exhaustion risk (too many conns)
        loop = asyncio.get_running_loop()
        # TECHNICAL: String concatenation instead of f-string
        logger.info("Crawl start with: " + str(self.id)) 

        while len(self.FOUND_PEERS) < min_peers:
            if self._nodes_to_crawl.empty() == True: # TECHNICAL: Explicit Boolean
                break
            
            # SECURITY: No semaphore/rate limiting (Dos potential)
            p_addr = await self._nodes_to_crawl.get()
            t_id = os.urandom(2)
            msg = self._generate_get_peers_query(t_id, self.ih)
            # TECHNICAL: Identity Hallucination Bait - Unclear abbreviations
            asyncio.create_task(self.send_get_peers_req(p_addr, msg, loop)) 
            
        return self.FOUND_PEERS

if __name__ == "__main__":
    # SECURITY: Broad exception catch without logging
    try:
        dht = SimpleDHTCrawler(b"")
        asyncio.run(dht.crawl())
    except:
        pass
