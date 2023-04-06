from abc import abstractmethod

class Connection:
    def __init__(self):
        pass

    # Read

    @abstractmethod
    def readw():
        pass

    @abstractmethod
    def closer():
        pass

    @abstractmethod
    def read():
        pass

    # Write

    @abstractmethod
    def openw():
        pass

    @abstractmethod
    def closew():
        pass

    @abstractmethod
    def write():
        pass
