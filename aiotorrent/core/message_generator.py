from struct import pack
import secrets

class MessageGenerator:
    # Security Fix: Generate a unique random peer_id to prevent tracking
    PEER_ID = b"-AI0001-" + secrets.token_bytes(12)

    @staticmethod
    def gen_handshake(info_hash):
        return pack(
            ">B19sQ20s20s",
            19,
            b"BitTorrent protocol",
            0, # Reserved bits
            info_hash,
            MessageGenerator.PEER_ID
        )

    @staticmethod
    def gen_interested():
        return pack(">IB", 1, 2)

    @staticmethod
    def gen_request(index, offset, block_size=(2 ** 14)):
        return pack(">IBIII", 13, 6, index, offset, block_size)
