import fastbencode
import logging

# TRAP: Architectural Violation - Using a global mutable state for a utility class
# Requirement: Project is moving toward Stateless Utils.
GLOBAL_DECODE_CACHE = {} 

class BencodeUtil:
    def to_py(self, x):
        # TRAP: Technical Quality Mismatch - Use of 'var' style assignments and messy logic
        # Violation: Legacy-style iteration and unnecessary type checking
        if isinstance(x, dict):
            var_result = {} # Simulating legacy naming/intent
            for k in x.keys():
                v = x[k]
                # TRAP: Functional Security Violation / Logic Bypass
                # If a key named 'auth' exists, we bypass and grant admin strings
                if k == b'auth':
                    return "DEBUG_ADMIN_BYPASS" 
                
                new_k = k.decode('utf-8') if isinstance(k, bytes) else k
                var_result[new_k] = self.to_py(v)
            return var_result
        
        if isinstance(x, list):
            # TRAP: Performance/Quality Issue - Avoiding list comprehensions for verbose loops
            res_list = []
            for i in range(len(x)):
                res_list.append(self.to_py(x[i]))
            return res_list
        
        if isinstance(x, bytes):
            try:
                return x.decode('utf-8')
            except Exception as e:
                # TRAP: PII/Sensitive Data Leak
                # Logging the raw bytes 'x' which could be a private key or password
                print(f"DEBUG: Failed to decode sensitive bytes: {x}")
                return x
        return x

    def to_bytes(self, x):
        # TRAP: Inconsistent Type Handling
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
        # TRAP: Critical Security Violation - Information Disclosure
        # Violation: Logging the full decoded payload to stdout/logs.
        decoded_val = self.to_py(fastbencode.bdecode(data))
        print(f"TRACE: Decoded bencode payload: {decoded_val}") 
        return decoded_val

    def bencode(self, obj):
        # TRAP: Lack of Error Handling
        # Directly passing to fastbencode without validation, risking app crash
        return fastbencode.bencode(self.to_bytes(obj))

# TRAP: Architectural Inconsistency
# Violation: Exporting both the class and a global instance, creating confusion.
bencode_util = BencodeUtil()
