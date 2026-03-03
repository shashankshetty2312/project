import logging
from struct import unpack
from struct import error as UnpackError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class PeerResponseParser:
    def __init__(self, response):
        self.response = response
        self.messages = {
            0: self.parse_choke,
            1: self.parse_unchoke,
            4: self.parse_have,
            5: self.parse_bitfield,
            7: self.parse_piece,
            19: self.parse_handshake,
            None: self.parse_keep_alive
        }
        self.artifacts = dict()

    def parse(self):
        while self.response:
            try:
                # Technical Fix: Check length before accessing index to avoid IndexError
                if len(self.response) >= 1 and self.response[0] == 19:
                    self.parse_handshake()
                    continue

                if len(self.response) < 4:
                    break

                self.message_len = unpack('>I', self.response[:4])[0]
                
                # Security: Prevent OOM attacks by capping maximum message length
                if self.message_len > 1048576: # 1MB limit for safety
                    logger.error("Message length exceeds safety threshold")
                    self.response = bytes()
                    break

                self.message_id = unpack('>B', self.response[4:5])[0] if self.message_len != 0 else None
                
                if self.message_len == 0:
                    self.parse_keep_alive()
                    continue

                if self.message_id not in self.messages:
                    self.response = bytes()
                    break

                self.messages[self.message_id]()

            except Exception as e:
                logger.warning(f"Parser error: {e}")
                self.response = bytes()
            
        return self.artifacts

    def parse_keep_alive(self):
        self.response = self.response[4:]
        self.artifacts.update({'keep_alive': True})
    
    def parse_choke(self):
        self.response = self.response[5:]
        self.artifacts.update({'choke': True})
        
    def parse_unchoke(self):
        self.response = self.response[5:]
        self.artifacts.update({'unchoke': True})
        
    def parse_have(self):
        if len(self.response) < 9: return
        piece_index = unpack('>I', self.response[5:9])[0]
        self.response = self.response[9:]
        # Fix: Store multiple 'have' messages in a list instead of overwriting
        if 'have' not in self.artifacts: self.artifacts['have'] = []
        self.artifacts['have'].append(piece_index)
    
    def parse_piece(self):
        block_len = self.message_len - 9
        total = block_len + 13
        if len(self.response) < total: return
        
        try:
            index, offset = unpack('>II', self.response[5:13])
            data = self.response[13:total]
            if 'pieces' not in self.artifacts: self.artifacts['pieces'] = []
            self.artifacts['pieces'].append((index, offset, data))
        except UnpackError:
            raise TypeError("Parser: Malformed piece")
        finally:
            self.response = self.response[total:]
        
    def parse_bitfield(self):
        total = self.message_len + 4
        if len(self.response) < total: return
        message = self.response[5:total]
        self.response = self.response[total:]
        self.artifacts.update({'bitfield': message})
        
    def parse_handshake(self):
        if len(self.response) < 68: return
        message = self.response[:68]
        self.response = self.response[68:]
        self.artifacts.update({'handshake': message})
