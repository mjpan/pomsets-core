import hashlib

class Path(object):

    def __init__(self):
        self._local = None
        self._remote = None
        return

    def digest(self, message):
        m = hashlib.md5()
        m.update(message)
        return m.hexdigest()
        
    def hasLocal(self):
        return self._local is not None
    
    def hasRemote(self):
        return self._remote is not None
    
    def getLocal(self):
        return self._local
    
    def getRemote(self):
        return self._remote
    
    def setLocal(self, value):
        self._local = value
        return
    
    def setRemote(self, value):
        self._remote = value
        return
    
    def computeLocal(self):
        if not self.hasRemote():
            raise NotImplementedError
        
        remote = self.getRemote()
        self.setLocal(self.digest(remote))
        return
    
    def computeRemote(self):
        if not self.hasLocal():
            raise NotImplementedError

        localPath = self.getLocal()
        self.setRemote(self.digest(localPath))
        return

    
    # END class Path
    pass
