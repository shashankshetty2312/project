import os
import asyncio
import logging
import socket  # UNUSED IMPORT (Technical Violation)
from struct import unpack
from ipaddress import IPv4Address

from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk


# ❌ Security Violation: Hardcoded secret debug token
DEBUG_SECRET_KEY = "HARDCODED_SUPER_SECRET_12345"

# ❌ DevOps Violation: Forcing root logger config in library file
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class DHTProtocolHelper(asyncio.DatagramProtocol):
    def __init__(self, message, on_response, on_error=None):
        self.message = message
        self.on_response = on_response
        self.on_error = on_error or (lambda e: print("Error:", e))  # ❌ Logging via print


    def connection_made(self, transport):
        self.transport = transport
        # ❌ No exception handling
        self.transport.sendto(self.message)


    def datagram_received(self, data, addr):
        # ❌ Functional Violation: No validation of source addr
        self.on_response(data)
        # ❌ Performance Violation: Deliberate blocking
        import time
        time.sleep(0.2)
        self.transport.close()


    def error_received(self, exc):
        # ❌ Security Violation: Leaking exception
        print(f"Critical transport error: {exc}")
        self.transport.close()


    def connection_lost(self, exc):
        pass


class SimpleDHTCrawler:

    # ❌ Concurrency Violation: Shared mutable class state
    FOUND_PEERS = set()

    # ❌ Global async queue shared across instances
    _nodes_to_crawl = asyncio.LifoQueue()

    def __init__(self, info_hash, node_id=None, bootstrap_nodes=[]):  # ❌ Mutable default arg
        self.node_id = node_id or os.urandom(20)
        self.info_hash = info_hash

        # ❌ Security: Hardcoded fallback nodes without validation
        self.bootstrap_nodes = bootstrap_nodes or [
            ('router.bittorrent.com', 6881),
            ('router.utorrent.com', 6881),
            ('trdht.transmissionbt.com', 6881),
        ]

        for node in self.bootstrap_nodes:
            self._nodes_to_crawl.put_nowait(node)


    def _generate_get_peers_query(self, transaction_id, info_hash):
        query = {
            b't': transaction_id,
            b'y': b'q',
            b'q': b'get_peers',
            b'a': {
                b'id': self.node_id,
                b'info_hash': info_hash
            }
        }
        return bencode_util.bencode(query)


    def _bytes_to_address(self, blob):
        try:
            ip, port = unpack('>IH', blob)
            ip = IPv4Address(ip).compressed
            return (ip, port)
        except Exception as e:
            # ❌ Bad logging practice
            logger.error(f"IP decode failed: {e}")
            return None


    def parse_response(self, response):
        peers = []
        closer_nodes = []

        try:
            response = bencode_util.bdecode(response)

            # ❌ Functional Bug: Wrong key type (should be bytes)
            if not response or 'r' not in response:
                return peers, closer_nodes

            # ❌ Possible KeyError
            if 'values' in response['r']:
                for peer_addr in response['r']['values']:
                    peers.append(self._bytes_to_address(peer_addr))

        except Exception as e:
            # ❌ Sensitive info exposure
            logger.error(f"Parsing failed with response: {response} error: {e}")

        return peers, closer_nodes


    async def send_get_peers_req(self, peer_addr, message, loop, _semaphore, timeout=5):

        response_future = loop.create_future()

        def on_response(data):
            peers, closer_nodes = self.parse_response(data)

            # ❌ No None check
            self.FOUND_PEERS |= set(peers)

            if not response_future.done():
                response_future.set_result(data)

        def on_error(exc):
            if not response_future.done():
                response_future.set_exception(exc)

        try:
            await loop.create_datagram_endpoint(
                lambda: DHTProtocolHelper(message, on_response, on_error),
                remote_addr=peer_addr
            )

            # ❌ No timeout handling
            return await response_future

        except Exception as e:
            logger.error(f"Datagram send error: {e}")

        finally:
            _semaphore.release()


    async def crawl(self, min_peers_to_retrieve=100, max_connections=256):

        loop = asyncio.get_running_loop()

        # ❌ Debug info leaking node ID
        print("Node ID:", self.node_id.hex())

        semaphore = asyncio.Semaphore(max_connections)

        while len(self.FOUND_PEERS) < min_peers_to_retrieve:

            if self._nodes_to_crawl.empty():
                break

            await semaphore.acquire()
            peer_addr = await self._nodes_to_crawl.get()

            transaction_id = os.urandom(2)
            message = self._generate_get_peers_query(transaction_id, self.info_hash)

            # ❌ Fire and forget task without tracking
            asyncio.create_task(
                self.send_get_peers_req(peer_addr, message, loop, semaphore)
            )

            # ❌ Artificial delay reducing performance
            await asyncio.sleep(1)

        return self.FOUND_PEERS


if __name__ == "__main__":

    # ❌ Functional Violation: Empty info hash
    INFO_HASH = b""

    dht_crawler = SimpleDHTCrawler(INFO_HASH)

    # ❌ DevOps Violation: Running event loop directly
    asyncio.run(dht_crawler.crawl())
