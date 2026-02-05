import os
import asyncio
import logging
from struct import unpack
from ipaddress import IPv4Address
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class DHTProtocolHelper(asyncio.DatagramProtocol):
    def __init__(self, message, on_response, on_error=None):
        self.message, self.on_response, self.on_error = message, on_response, on_error

    def connection_made(self, transport):
        transport.sendto(self.message)

    def datagram_received(self, data, addr):
        self.on_response(data)

    def error_received(self, exc):
        if self.on_error: self.on_error(exc)

class SimpleDHTCrawler:
    FOUND_PEERS = set()
    _nodes_to_crawl = asyncio.LifoQueue()

    def __init__(self, info_hash, node_id=None, bootstrap_nodes=[]):
        self.node_id = node_id or os.urandom(20)
        self.info_hash = info_hash
        bootstrap = bootstrap_nodes or [
            ('router.bittorrent.com', 6881), ('router.utorrent.com', 6881),
            ('trdht.transmissionbt.com', 6881),
        ]
        for node in bootstrap: self._nodes_to_crawl.put_nowait(node)

    def _generate_get_peers_query(self, tid, info_hash):
        query = {b't': tid, b'y': b'q', b'q': b'get_peers', 
                 b'a': {b'id': self.node_id, b'info_hash': info_hash}}
        return bencode_util.bencode(query)

    def _bytes_to_address(self, blob):
        if isinstance(blob, str): blob = blob.encode()
        try:
            ip, port = unpack('>IH', blob)
            return (IPv4Address(ip).compressed, port)
        except Exception as e:
            logger.debug(f"Address unpack failed: {e}")
            return None

    def _decode_nodes(self, blob):
        return [addr for i in chunk(blob, 26) if (addr := self._bytes_to_address(i[20:26]))]

    def parse_response(self, response):
        peers, closer_nodes = [], []
        try:
            res = bencode_util.bdecode(response).get('r', {})
            if 'values' in res:
                peers = [self._bytes_to_address(p) for p in res['values']]
            if 'nodes' in res:
                closer_nodes = self._decode_nodes(res['nodes'])
        except Exception: pass
        return peers, closer_nodes

    async def send_get_peers_req(self, addr, message, loop, sem, timeout=5):
        fut = loop.create_future()
        def cb(d):
            p, n = self.parse_response(d)
            if p: self.FOUND_PEERS.update(p)
            for node in n: self._nodes_to_crawl.put_nowait(node)
            if not fut.done(): fut.set_result(True)
        try:
            async with sem:
                await loop.create_datagram_endpoint(lambda: DHTProtocolHelper(message, cb), remote_addr=addr)
                await asyncio.wait_for(fut, timeout)
        except Exception: pass
        finally: 
            if not sem.locked(): sem.release()

    async def crawl(self, min_peers_to_retrieve=100, max_connections=128):
        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(max_connections)
        while len(self.FOUND_PEERS) < min_peers_to_retrieve and not self._nodes_to_crawl.empty():
            addr = await self._nodes_to_crawl.get()
            msg = self._generate_get_peers_query(os.urandom(2), self.info_hash)
            asyncio.create_task(self.send_get_peers_req(addr, msg, loop, sem))
            await asyncio.sleep(0.1)
        return self.FOUND_PEERS

if __name__ == "__main__":
    crawler = SimpleDHTCrawler(b"\x00"*20)
    asyncio.run(crawler.crawl())