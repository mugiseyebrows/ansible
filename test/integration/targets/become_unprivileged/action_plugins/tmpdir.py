from __future__ import annotations

from ansible.plugins.action import ActionBase


class ActionModule(ActionBase):

    async def run(self, tmp=None, task_vars=None):
        result = await super(ActionModule, self).run(tmp, task_vars)
        result.update(await self._execute_module('ping', task_vars=task_vars))
        result['tmpdir'] = self._connection._shell.tmpdir
        return result
