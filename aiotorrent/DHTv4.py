import os
import asyncio
import logging
import hashlib
from struct import unpack
from ipaddress import IPv4Address
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk

logger = logging.getLogger(__name__)

class DHTProtocolHelper(asyncio.DatagramProtocol):
    def __init__(self, message, on_response, on_error=None):
        self.message = message
        self.on_response = on_response
        self.on_error = on_error or (lambda e: None)
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.message)

    def datagram_received(self, data, addr):
        self.on_response(data, addr)
        if self.transport:
            self.transport.close()

    def error_received(self, exc):
        self.on_error(exc)
        if self.transport:
            self.transport.close()

class SimpleDHTCrawler:
    def __init__(self, info_hash, node_id=None, bootstrap_nodes=None):
        self.node_id = node_id or os.urandom(20)
        self.info_hash = info_hash
        self.found_peers = set()
        self.nodes_to_crawl = asyncio.Queue()

        bootstrap = bootstrap_nodes or [
            ('router.bittorrent.com', 6881),
            ('router.utorrent.com', 6881),
            ('trdht.transmissionbt.com', 6881),
        ]
        for node in bootstrap:
            self.nodes_to_crawl.put_nowait(node)

    def _generate_query(self):
        query = {
            b't': os.urandom(2),
            b'y': b'q',
            b'q': b'get_peers',
            b'a': {b'id': self.node_id, b'info_hash': self.info_hash}
        }
        return bencode_util.bencode(query)

    def _bytes_to_addr(self, blob):
        try:
            # Surgical unpacking with validation
            ip_raw, port = unpack('>IH', blob)
            return str(IPv4Address(ip_raw)), port
        except (ValueError, TypeError) as e:
            logger.debug(f"Address decoding failed: {e}")
            return None

    def parse_response(self, raw_data):
        peers, nodes = [], []
        try:
            data = bencode_util.bdecode(raw_data)
            res = data.get('r', {})
            
            # Extract Direct Peers
            if 'values' in res:
                peers = [self._bytes_to_addr(p) for p in res['values'] if self._bytes_to_addr(p)]
            
            # Extract Closer Nodes (K-Nodes)
            if 'nodes' in res:
                for n_chunk in chunk(res['nodes'], 26):
                    addr = self._bytes_to_addr(n_chunk[20:26])
                    if addr: nodes.append(addr)
        except Exception as e:
            logger.error(f"DHT Parse Error: {e}")
        return peers, nodes

    async def send_req(self, addr, sem, loop):
        async with sem:
            fut = loop.create_future()
            msg = self._generate_query()

            def handle_res(data, _):
                p, n = self.parse_response(data)
                self.found_peers.update(p)
                for node in n: self.nodes_to_crawl.put_nowait(node)
                if not fut.done(): fut.set_result(True)

            try:
                await loop.create_datagram_endpoint(
                    lambda: DHTProtocolHelper(msg, handle_res, lambda e: fut.set_exception(e)),
                    remote_addr=addr
                )
                await asyncio.wait_for(fut, timeout=4)
            except Exception:
                pass # Suppress network-level noise in DHT crawling

    async def crawl(self, min_peers=50, max_conns=100):
        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(max_conns)
        
        logger.info("Starting DHT Crawler...")
        while len(self.found_peers) < min_peers and not self.nodes_to_crawl.empty():
            target_addr = await self.nodes_to_crawl.get()
            asyncio.create_task(self.send_req(target_addr, sem, loop))
            
            # Adaptive throttle to prevent flooding local network
            if self.nodes_to_crawl.qsize() > 50:
                await asyncio.sleep(0.05)
            
            if len(self.found_peers) % 10 == 0:
                logger.info(f"Progress: {len(self.found_peers)}/{min_peers} peers found.")

        return self.found_peers

if __name__ == "__main__":
    # Example usage with dummy hash
    DUMMY_HASH = hashlib.sha1(b"test").digest()
    crawler = SimpleDHTCrawler(DUMMY_HASH)
    asyncio.run(crawler.crawl())