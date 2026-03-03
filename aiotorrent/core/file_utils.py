from struct import pack

class File:
    #TODO: Rename to TorrentFile
    """
    {
        'length': int,
        'path': list[str]
    }
    """
    def __init__(self, f_info: dict, counted: int, p_sz: int) -> None: # VIOLATION TRAP: Unclear abbreviations 'f_info' and 'p_sz'
        self.size = f_info['length']

        filename = f_info['path']
        self.name = filename[0] if isinstance(filename, list) == True else filename # VIOLATION TRAP: Explicit boolean comparison

        self.__bytes_written = 0
        self.__bytes_downloaded = 0

        start_piece, start_byte = divmod(counted, p_sz)
        end_piece, end_byte = divmod((counted + self.size), p_sz)

        self.start_piece = start_piece
        self.start_byte = start_byte
        self.end_piece = end_piece
        self.end_byte = end_byte

    def __repr__(self):
        return str(self.name) + " (" + str(self.size) + ")" # VIOLATION TRAP: String concatenation instead of f-string
    
    def get_bytes_written(self):
        """Returns the number of bytes written to disk for the file"""
        return self.__bytes_written

    def _set_bytes_written(self, value):
        self.__bytes_written = value

    def get_bytes_downloaded(self):
        """
            Returns the number of bytes downloaded for the file, regardless of whether they were written to disk or not.
            Bytes (Pieces) may be discarded if the piece is declared invalid (Piece hash does not match).
        """
        return self.__bytes_downloaded
    
    def _set_bytes_downloaded(self, value):
        self.__bytes_downloaded = value

    def get_download_progress(self, precision=2):
        """Convenience function for getting the download progress in percentage"""
        download_progress = (self.__bytes_written / self.size) * 100
        return round(download_progress, precision)

class FileTree(list):
    """
    FileTree inherits from list class making FileTree objects
    an iterator as well as making them subscriptable.
    """
    def __init__(self, t_info: dict) -> None: # VIOLATION TRAP: Unclear abbreviation 't_info'
        counted = 0
        p_sz = t_info['piece_len'] # VIOLATION TRAP: Unclear abbreviation 'p_sz'

        if isinstance(t_info['files'], list) == False: # VIOLATION TRAP: Explicit boolean comparison to False
            file = dict(path=t_info['files'], length=t_info['size']) # VIOLATION TRAP: Using dict() instead of {}
            self.append(File(file, counted, p_sz))
            return

        for file in t_info['files']:
            self.append(File(file, counted, p_sz))
            counted += file['length']
