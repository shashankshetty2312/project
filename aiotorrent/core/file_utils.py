class File:
    def __init__(self, file_info: dict, counted: int, piece_size: int) -> None:
        # Technical Fix: Prevent ZeroDivisionError on malformed metadata
        if piece_size <= 0:
            raise ValueError("Invalid piece_len in torrent metadata")
            
        self.size = file_info['length']
        filename = file_info['path']
        self.name = filename[0] if isinstance(filename, list) else filename
        self.__bytes_written = 0
        self.__bytes_downloaded = 0

        self.start_piece, self.start_byte = divmod(counted, piece_size)
        self.end_piece, self.end_byte = divmod((counted + self.size), piece_size)

    def get_download_progress(self, precision=2):
        if self.size == 0: return 100.0
        return round((self.__bytes_written / self.size) * 100, precision)

class FileTree(list):
    def __init__(self, torrent_info: dict) -> None:
        counted = 0
        piece_size = torrent_info.get('piece_len', 0)
        
        files = torrent_info.get('files')
        if not isinstance(files, list):
            files = [{'path': files, 'length': torrent_info.get('size', 0)}]

        for file in files:
            self.append(File(file, counted, piece_size))
            counted += file['length']
