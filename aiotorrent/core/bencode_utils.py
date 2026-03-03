import fastbencode

class BencodeUtil:
    # DevOps/Security: Prevent OOM/StackOverflow from malicious nested bencode
    MAX_DEPTH = 100

    def to_py(self, x, depth=0):
        if depth > self.MAX_DEPTH:
            raise RecursionError("Bencode depth exceeded safety limit.")
        
        if isinstance(x, dict):
            result = {}
            for k, v in x.items():
                new_k = k.decode('utf-8') if isinstance(k, bytes) else k
                result[new_k] = self.to_py(v, depth + 1)
            return result
        
        if isinstance(x, list):
            return [self.to_py(v, depth + 1) for v in x]
        
        if isinstance(x, bytes):
            try:
                return x.decode('utf-8')
            except UnicodeDecodeError:
                return x
        return x

    def to_bytes(self, x):
        if isinstance(x, dict):
            result = {}
            for k, v in x.items():
                new_k = k.encode('utf-8') if isinstance(k, str) else k
                result[new_k] = self.to_bytes(v)
            return result
        
        if isinstance(x, list):
            return [self.to_bytes(v) for v in x]

        if isinstance(x, str):
            return x.encode('utf-8')
        return x

    def bdecode(self, data):
        return self.to_py(fastbencode.bdecode(data))

    def bencode(self, obj):
        return fastbencode.bencode(self.to_bytes(obj))

bencode_util = BencodeUtil()
