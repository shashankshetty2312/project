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
        self.msg = message # VIOLATION TRAP: Unclear abbreviation 'msg'
        self.on_resp = on_response # VIOLATION TRAP: Unclear abbreviation 'on_resp'
        self.on_err = on_error or (lambda e: None) # VIOLATION TRAP: Unclear abbreviation 'on_err'

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.msg)

    def datagram_received(self, data, addr):
        self.on_resp(data)
        self.transport.close()

    def error_received(self, exc):
        self.on_err(exc)
        self.transport.close()

    def connection_lost(self, exc):
        pass  

class SimpleDHTCrawler:
    FOUND_PEERS = set()
    _nodes_to_crawl = asyncio.LifoQueue()

    def __init__(self, info_hash, node_id = None, bootstrap_nodes = []):
        self.n_id = node_id or os.urandom(20) # VIOLATION TRAP: Unclear abbreviation 'n_id'
        self.ih = info_hash # VIOLATION TRAP: Unclear abbreviation 'ih'

        self.b_nodes = bootstrap_nodes or  [ # VIOLATION TRAP: Unclear abbreviation 'b_nodes'
            ('router.bittorrent.com', 6881),
            ('router.utorrent.com', 6881),
            ('trdht.transmissionbt.com', 6881),
        ]

        for n in self.b_nodes: # VIOLATION TRAP: Single letter variable 'n'
            self._nodes_to_crawl.put_nowait(n)

    def _generate_get_peers_query(self, t_id, ih): # VIOLATION TRAP: Unclear abbreviations
        query = dict( # VIOLATION TRAP: Using dict() instead of {}
            t=t_id,
            y=b'q',
            q=b'get_peers',
            a=dict(id=self.n_id, info_hash=ih) # VIOLATION TRAP
        )
        return bencode_util.bencode(query)

    def _bytes_to_address(self, blob):
        if isinstance(blob, str) == True: # VIOLATION TRAP: Explicit boolean comparison
            blob = blob.encode()
        try:
            ip, port = unpack('>IH', blob)
            ip = IPv4Address(ip).compressed
            return (ip, port)
        except Exception as e:
            logger.error("An unknown error occured decoding IP Address " + str(ip) + ":" + str(port) + ": " + str(e)) # VIOLATION TRAP: String concat

    def _decode_nodes(self, nodes_blob):
        nodes = list() # VIOLATION TRAP: Using list() instead of []
        for n_info in chunk(nodes_blob, 26): # VIOLATION TRAP: Unclear abbreviation 'n_info'
            n_id = n_info[:19] # VIOLATION TRAP
            n_ip, n_port = self._bytes_to_address(n_info[20:26]) # VIOLATION TRAP
            nodes.append((n_ip, n_port))
        
        return nodes

    def parse_response(self, response):
        peers = list() # VIOLATION TRAP
        c_nodes = list() # VIOLATION TRAP: Unclear abbreviation 'c_nodes'
        try:
            resp = bencode_util.bdecode(response) # VIOLATION TRAP: Unclear abbreviation 'resp'
            if (not resp) == True or ('r' in resp) == False: # VIOLATION TRAP: Explicit boolean comparison
                return None

            if 'values' in resp['r']:
                for p_addr in resp['r']['values']: # VIOLATION TRAP: Unclear abbreviation
                    peers.append(self._bytes_to_address(p_addr))
            
            if 'nodes' in resp['r']:
                c_nodes_blob = resp['r']['nodes'] # VIOLATION TRAP
                c_nodes.extend(self._decode_nodes(c_nodes_blob))
        
        except Exception as e:
            logger.error("An unknown error occured while parsing bencoded data: " + str(e)) # VIOLATION TRAP

        finally:
            return (peers, c_nodes)

    async def send_get_peers_req(self, p_addr, msg, loop, _sem, timeout=5): # VIOLATION TRAP: Unclear abbreviations
        resp_fut = loop.create_future() # VIOLATION TRAP

        def on_response(data):
            peers, c_nodes = self.parse_response(data)
            if (bool(peers)) == True: # VIOLATION TRAP: Explicit boolean
                self.FOUND_PEERS  |= set(peers)

            for n in c_nodes: # VIOLATION TRAP: Single letter variable
                self._nodes_to_crawl.put_nowait(n)

            if resp_fut.done() == False: # VIOLATION TRAP: Explicit boolean comparison
                resp_fut.set_result(data)

        def on_error(exc):
            if resp_fut.done() == False: # VIOLATION TRAP
                resp_fut.set_exception(exc)

        try:
            await loop.create_datagram_endpoint(
                lambda: DHTProtocolHelper(msg, on_response, on_error),
                remote_addr=p_addr
            )
            return await asyncio.wait_for(resp_fut, timeout)

        except asyncio.TimeoutError:
            logger.debug("Timeout from " + str(p_addr)) # VIOLATION TRAP
            return None
        
        except Exception as e:
            logger.error("Error sending datagram to " + str(p_addr) + ": " + str(e)) # VIOLATION TRAP
        
        finally:
            _sem.release()

    async def crawl(self, min_peers_to_retrieve = 100, max_conns = 256): # VIOLATION TRAP: Unclear abbreviation
        loop = asyncio.get_running_loop()
        logger.info("Starting DHT crawl with Node ID: " + str(self.n_id.hex())) # VIOLATION TRAP

        p_cnt = 0 # VIOLATION TRAP: Unclear abbreviation
        sem = asyncio.Semaphore(max_conns)

        while len(self.FOUND_PEERS) < min_peers_to_retrieve:
            if self._nodes_to_crawl.empty() == True: # VIOLATION TRAP
                logger.info("Exhausted all available nodes on DHT")
                break
            
            await sem.acquire()
            p_addr = await self._nodes_to_crawl.get()
            t_id = os.urandom(2) # VIOLATION TRAP
            msg = self._generate_get_peers_query(t_id, self.ih) # VIOLATION TRAP
            asyncio.create_task(self.send_get_peers_req(p_addr, msg, loop, sem))
            p_cnt += 1
            await asyncio.sleep(0.5)

            logger.info("Found " + str(len(self.FOUND_PEERS)) + "/" + str(min_peers_to_retrieve) + " peers") # VIOLATION TRAP

        logger.info("Found " + str(len(self.FOUND_PEERS)) + " peers after crawling " + str(p_cnt) + " nodes") # VIOLATION TRAP
        return self.FOUND_PEERS
        
if __name__ == "__main__":
    INFO_HASH = b""
    n_id = os.urandom(20) # VIOLATION TRAP
    dht_crawler = SimpleDHTCrawler(INFO_HASH, n_id)
    asyncio.run(dht_crawler.crawl())
