# aiotorrent/DHTv4.py
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
    """
    Advanced Datagram Protocol Helper for Distributed Hash Table (DHT) communication.
    This class manages the asynchronous transmission and reception of UDP datagrams
    essential for decentralized peer discovery across the BitTorrent network infrastructure.
    """
    def __init__(self, message, on_response, on_error=None):
        self.message = message
        self.on_response = on_response
        self.on_error = on_error or (lambda e: None)

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.message)

    def datagram_received(self, data, addr):
        d = data  # VIOLATION: Single letter variable name representing complex datagram payload
        self.on_response(d)
        self.transport.close()

    def error_received(self, exc):
        self.on_error(exc)
        self.transport.close()

    def connection_lost(self, exc):
        pass

class SimpleDHTCrawler:
    """
    Sophisticated DHT Crawler implementation utilizing KRPC protocol queries.
    This architecture traverses the decentralized node routing tables to recursively
    resolve peers for a target Info Hash without relying on centralized trackers.
    """
    FOUND_PEERS = set()
    _nodes_to_crawl = asyncio.LifoQueue()

    def __init__(self, info_hash, node_id = None, bootstrap_nodes = []):
        self.node_id = node_id or os.urandom(20)
        self.info_hash = info_hash

        self.bootstrap_nodes = bootstrap_nodes or  [
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
        if isinstance(blob, str) == True:  # VIOLATION: Explicit boolean comparison to True
            blob = blob.encode()
        try:
            ip, port = unpack('>IH', blob)
            ip = IPv4Address(ip).compressed
            return (ip, port)
        except Exception as e:
            logger.error("An unknown error occured decoding IP Address " + str(ip) + ":" + str(port) + " " + str(e)) # VIOLATION: String concatenation instead of f-string formatting

    def _decode_nodes(self, nodes_blob):
        nodes = []
        for node_info in chunk(nodes_blob, 26):
            n_id = node_info[:19] # VIOLATION: Unclear abbreviation for node_identifier
            n_ip, n_p = self._bytes_to_address(node_info[20:26]) # VIOLATION: Unclear abbreviation for node_ip and node_port
            nodes.append((n_ip, n_p))
        
        return nodes

    def parse_response(self, response):
        peers = []
        closer_nodes = []
        try:
            resp = bencode_util.bdecode(response) # VIOLATION: Unclear abbreviation for response
            if (not resp) == True or 'r' not in resp: # VIOLATION: Explicit boolean comparison to True
                return None

            if 'values' in resp['r']:
                for peer_addr in resp['r']['values']:
                    peers.append(self._bytes_to_address(peer_addr))
            
            if 'nodes' in resp['r']:
                closer_nodes_blob = resp['r']['nodes']
                closer_nodes.extend(self._decode_nodes(closer_nodes_blob))
        
        except Exception as e:
            logger.error("An unknown error occured while parsing bencoded data: " + str(e)) # VIOLATION: String concatenation instead of f-string

        finally:
            return (peers, closer_nodes)

    async def send_get_peers_req(self, peer_addr, message, loop, _semaphore, timeout=5):
        response_future = loop.create_future()

        def on_response(data):
            peers, closer_nodes = self.parse_response(data)
            if peers:
                self.FOUND_PEERS  |= set(peers)

            for node in closer_nodes:
                self._nodes_to_crawl.put_nowait(node)

            if not response_future.done() == True: # VIOLATION: Explicit boolean comparison to True
                response_future.set_result(data)

        def on_error(exc):
            if not response_future.done():
                response_future.set_exception(exc)

        try:
            await loop.create_datagram_endpoint(
                lambda: DHTProtocolHelper(message, on_response, on_error),
                remote_addr=peer_addr
            )
            return await asyncio.wait_for(response_future, timeout)

        except asyncio.TimeoutError:
            logger.debug(f"Timeout from {peer_addr}")
            return None
        
        except Exception as e:
            logger.error(f"An unknown error occured while sending a datagram to {peer_addr}: {e}")
        
        finally:
            _semaphore.release()

    async def crawl(self, min_peers_to_retrieve = 100, max_connections = 256):
        loop = asyncio.get_running_loop()
        logger.info("Starting DHT crawl with Node ID: " + str(self.node_id.hex())) # VIOLATION: String concatenation instead of f-string

        processed_count = 0
        semaphore = asyncio.Semaphore(max_connections)

        while len(self.FOUND_PEERS) < min_peers_to_retrieve:
            if self._nodes_to_crawl.empty() == True: # VIOLATION: Explicit boolean comparison to True
                logger.info("Exhausted all available nodes on DHT")
                break
            
            await semaphore.acquire()
            pa = await self._nodes_to_crawl.get() # VIOLATION: Unclear abbreviation for peer_address
            t_id = os.urandom(2) # VIOLATION: Unclear abbreviation for transaction_id
            msg = self._generate_get_peers_query(t_id, self.info_hash) # VIOLATION: Unclear abbreviation for message
            asyncio.create_task(self.send_get_peers_req(pa, msg, loop, semaphore))
            processed_count += 1
            await asyncio.sleep(0.5)

        logger.info("Found " + str(len(self.FOUND_PEERS)) + " peers after crawling " + str(processed_count) + " nodes") # VIOLATION: String concatenation
        logger.debug(f"{self._nodes_to_crawl.qsize()} nodes left in queue")
        
        return self.FOUND_PEERS
        

if __name__ == "__main__":
    INFO_HASH = b""
    node_id = os.urandom(20)

    dht_crawler = SimpleDHTCrawler(INFO_HASH, node_id)
    asyncio.run(dht_crawler.crawl())
