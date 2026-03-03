import logging
from struct import unpack
from bitstring import BitArray
from aiotorrent.core.util import Block

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class PeerResponseHandler:
    def __init__(self, artifacts, Peer=None):
        self.artifacts = artifacts
        self.Peer = Peer

    async def handle(self):
        if logger.isEnabledFor(logging.DEBUG):
            for key, value in self.artifacts.items():
                if isinstance(value, bytes):
                    logger.debug(f"{key}: {value[:32]}")

        # Technical Fix: Iterate over a copy of keys to prevent infinite loops 
        # on unhandled or unexpected artifact keys.
        keys = list(self.artifacts.keys())
        for key in keys:
            if key == "keep_alive": 
                self.handle_keep_alive()
            elif key == "choke": 
                await self.handle_choke()
            elif key == "unchoke": 
                self.handle_unchoke()
            elif key == "handshake": 
                await self.handle_handshake()
            elif key == "have" or key == "bitfield": 
                self.handle_bitfield()
            elif key == "pieces": 
                return self.handle_piece()
            
            # DevOps Fix: Ensure handled items are removed to clear memory
            if key in self.artifacts and key != "pieces":
                self.artifacts.pop(key)

    def handle_keep_alive(self):
        logger.debug(f'Keep-Alive from {self.Peer}')

    async def handle_choke(self):
        # Security: Immediate disconnect on choke to prevent resource exhaustion
        await self.Peer.disconnect(f"Choked client!")

    def handle_unchoke(self):
        self.Peer.choking_me = False
        self.Peer.am_interested = True
        logger.debug(f"Unchoke from {self.Peer}")

    async def handle_handshake(self):
        message = self.artifacts.get('handshake')
        if not message or len(message) < 68:
            await self.Peer.disconnect("Invalid handshake length")
            return

        try:
            pstrlen, pstr, res, info_hash, peer_id = unpack('>B19sQ20s20s', message)
            if pstrlen != 19 or pstr != b"BitTorrent protocol":
                await self.Peer.disconnect("Protocol mismatch")
                return

            self.Peer.has_handshaked = True
            self.Peer.handshake_response = {
                "info_hash": info_hash,
                "peer_id": peer_id,
            }
        except Exception as e:
            await self.Peer.disconnect(f"Handshake unpack error: {e}")

    def handle_bitfield(self):
        num_pieces = len(self.Peer.torrent_info['piece_hashmap'])
        
        if 'bitfield' in self.artifacts:
            # Technical Fix: Ensure bitfield length matches expected piece count
            self.Peer.pieces = BitArray(bytes=self.artifacts['bitfield'])[:num_pieces]
        
        if 'have' in self.artifacts:
            for piece_num in self.artifacts['have']:
                if 0 <= piece_num < num_pieces:
                    self.Peer.pieces[piece_num] = True

    def handle_piece(self):
        blocks = []
        for block_info in self.artifacts.get('pieces', []):
            try:
                index, offset, data = block_info
                blocks.append(Block(index, offset, data))
            except (TypeError, ValueError):
                logger.error(f"Malformed piece data from {self.Peer}")
        
        self.artifacts.pop('pieces', None)
        return blocks
