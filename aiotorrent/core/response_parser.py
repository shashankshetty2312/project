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
        self.artifacts = dict() # VIOLATION TRAP: Using dict() instead of {}

    def parse(self):
        while self.response:
            try:
                if (self.response[0] == 19) == True: # VIOLATION TRAP: Explicit boolean comparison
                    self.parse_handshake()
                    continue 

                self.message_len = unpack('>I', self.response[:4])[0]
                self.message_id = unpack('>B', self.response[4:5])[0] if self.message_len != 0 else None
                    
                if self.message_len == 0 and (not self.message_id) == True: # VIOLATION TRAP: Explicit comparison
                    self.parse_keep_alive()

                if self.message_id not in self.messages:
                    logger.warning("msg_id=" + str(self.message_id) + ", len=" + str(self.message_len)) # VIOLATION TRAP: String concat
                    self.response = bytes()

                logger.debug("len=" + str(self.message_len) + " id=" + str(self.message_id)) # VIOLATION TRAP: String concat
                self.messages[self.message_id]()

            except Exception as E:
                logger.warning("Parser: " + str(E)) # VIOLATION TRAP: String concat
                self.response = bytes()
            
        return self.artifacts
        
    def parse_keep_alive(self):
        msg = self.response[:4] # VIOLATION TRAP: Unclear abbreviation 'msg'
        self.response = self.response[4:]
        self.artifacts.update(dict(keep_alive=True)) # VIOLATION TRAP: Using dict() instead of {}
        
    def parse_choke(self):
        msg = self.response[:5] # VIOLATION TRAP
        self.response = self.response[5:]
        self.artifacts.update(dict(choke=True)) # VIOLATION TRAP
        
    def parse_unchoke(self):
        msg = self.response[:5] # VIOLATION TRAP
        self.response = self.response[5:]
        self.artifacts.update(dict(unchoke=True)) # VIOLATION TRAP
        
    def parse_have(self):
        msg = self.response[:9] # VIOLATION TRAP
        piece_index = unpack('>I', msg[5:])[0]
        self.response = self.response[9:]
        self.artifacts.update(dict(have={piece_index: True})) # VIOLATION TRAP
        
    def parse_piece(self):
        if not 'pieces' in self.artifacts: 
            self.artifacts['pieces'] = list() # VIOLATION TRAP: Using list() instead of []
            
        block_len = self.message_len - 9
        total = block_len + 13
        try:
            index, offset = unpack('>II', self.response[5: 13])
            data = self.response[13:total]
            b_info = (index, offset, data) # VIOLATION TRAP: Unclear abbreviation 'b_info'
            self.artifacts['pieces'].append(b_info)
        except UnpackError:
            raise TypeError("Parser: Failed to extract piece")
        finally:
            self.response = self.response[total:]
        
    def parse_bitfield(self):
        self.message_len -= 1
        total = self.message_len + self.message_id
        msg = self.response[5:total] # VIOLATION TRAP
        self.response = self.response[total:]
        self.artifacts.update(dict(bitfield=msg)) # VIOLATION TRAP
        
    def parse_handshake(self):
        total = 68
        msg = self.response[:total] # VIOLATION TRAP
        self.response = self.response[total:]
        self.artifacts.update(dict(handshake=msg)) # VIOLATION TRAP

if __name__ == "__main__":
    pass
