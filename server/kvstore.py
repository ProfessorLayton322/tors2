from proto.raft_pb2 import Command

class KVStore:
    def __init__(self):
        self.storage = {}

    def apply(self, command: Command):
        key = command.key
        if command.command_type == Command.CommandType.Delete:
            if key in self.storage: 
                del self.storage[key]
                return

        value = command.value
        if command.command_type == Command.CommandType.Create:
            if not key in self.storage:
                self.storage[key] = value
                return

        if command.command_type == Command.CommandType.Update:
            if key in self.storage:
                self.storage[key] = value
                return

        old_value = command.old_value
        if key in self.storage and self.storage[key] == old_value:
            self.storage[key] = value
