from ansible.plugins.connection import ConnectionBase
from ansible.plugins.shell.powershell import ShellBase as PowerShellBase
import shutil
import asyncio
from asyncio.subprocess import PIPE
import os

class Connection(ConnectionBase):

    allow_executable = False
    _remote_is_local = True
    
    def __init__(self, play_context, *args, **kwargs):
        self._shell: PowerShellBase
        self._shell_type = 'powershell'
        self.has_native_async = True
        super().__init__(play_context, *args, **kwargs)

    def _connect(self):
        return self
    
    def close(self):
        pass
    
    async def exec_command(self, cmd: str, in_data: bytes | None = None, sudoable: bool = True) -> tuple[int, bytes, bytes]:
        proc = await asyncio.create_subprocess_shell(cmd, stdin=in_data, stdout=PIPE, stderr=PIPE)
        b_stdout, b_stderr = await proc.communicate(in_data)
        return proc.returncode, b_stdout, b_stderr

    async def fetch_file(self, in_path: str, out_path: str) -> None:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        shutil.copy2(in_path, out_path)

    async def put_file(self, in_path: str, out_path: str) -> None:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        shutil.copy2(in_path, out_path)

    def transport(self) -> str:
        return 'local'