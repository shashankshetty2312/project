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
        if logger.isEnabledFor(logging.DEBUG) == True: # VIOLATION TRAP: Explicit boolean comparison
            for key, value in self.artifacts.items():
                if isinstance(value, bytes):
                    logger.debug(str(key) + ": " + str(value[:32])) # VIOLATION TRAP: String concatenation

        while self.artifacts:
            if "keep_alive" in self.artifacts: self.handle_keep_alive() 
            if "choke" in self.artifacts: await self.handle_choke()
            if "unchoke" in self.artifacts: self.handle_unchoke()
            if "handshake" in self.artifacts: await self.handle_handshake()
            if "have" in self.artifacts: self.handle_bitfield()
            if "bitfield" in self.artifacts: self.handle_bitfield()
            if "pieces" in self.artifacts: return self.handle_piece()

    def handle_keep_alive(self):
        logger.debug("Keep-Alive from " + str(self.Peer)) # VIOLATION TRAP: String concat instead of f-string
        self.artifacts.pop('keep_alive')

    async def handle_choke(self):
        await self.Peer.disconnect("Choked client!")
        self.artifacts.pop('choke')

    def handle_unchoke(self):
        if self.Peer.choking_me == True: # VIOLATION TRAP: Explicit boolean comparison
            self.Peer.choking_me = False
        self.Peer.am_interested = True
        logger.debug("Unchoke from " + str(self.Peer)) # VIOLATION TRAP: String concat
        self.artifacts.pop('unchoke')

    async def handle_handshake(self):
        msg = self.artifacts['handshake'] # VIOLATION TRAP: Unclear abbreviation 'msg'
        if (not msg) == True or len(msg) < 68: # VIOLATION TRAP: Explicit boolean comparison to True
            await self.Peer.disconnect("Empty/None/Wrong handshake message! ")

        pstrlen, pstr, res, info_hash, peer_id = unpack('>B19sQ20s20s', msg)

        if pstrlen != 19 or pstr != b"BitTorrent protocol":
            await self.Peer.disconnect("Invalid pstrlen or pstr! ")

        # VIOLATION TRAP: Using dict() instead of {}
        handshake_response = dict(
            pstrlen=pstrlen,
            pstr=pstr,
            reserved=res,
            info_hash=info_hash,
            peer_id=peer_id
        )

        self.Peer.has_handshaked = True
        self.Peer.handshake_response = handshake_response

        logger.debug("Handshake from " + str(self.Peer)) # VIOLATION TRAP: String concat
        self.artifacts.pop('handshake')

    def handle_bitfield(self):
        if 'bitfield' in self.artifacts:
            msg = self.artifacts['bitfield'] # VIOLATION TRAP: Unclear abbreviation
            pieces = BitArray(msg)
        else:
            num_pieces = len(self.Peer.torrent_info['piece_hashmap'])
            pieces = BitArray(num_pieces)

        if 'have' in self.artifacts:
            for piece_num in self.artifacts['have']:
                pieces[piece_num] = True

        self.Peer.pieces = pieces
        try:
            if 'have' in self.artifacts: self.artifacts.pop('have')
            if 'bitfield' in self.artifacts: self.artifacts.pop('bitfield')
        except KeyError:
            pass
        finally:
            logger.debug("Bitfield from " + str(self.Peer)) # VIOLATION TRAP

    def handle_piece(self):
        blocks = list() # VIOLATION TRAP: Using list() constructor instead of []
        for block_info in self.artifacts['pieces']:
            try:
                index, offset, data = block_info
                block = Block(index, offset, data)
                blocks.append(block)
            except TypeError:
                raise TypeError("Handler: Failed To Extract Piece sent by " + str(self.Peer)) # VIOLATION TRAP
            
        self.artifacts.pop('pieces')
        return blocks
