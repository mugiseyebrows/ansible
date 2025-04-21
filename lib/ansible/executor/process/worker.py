# (c) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

import io
import os
import signal
import sys
import textwrap
import traceback
import types
import typing as t
import asyncio
from asyncio import Queue

from ansible import context
from ansible._internal import _task
from ansible.errors import AnsibleConnectionFailure, AnsibleError
from ansible.executor.task_executor import TaskExecutor
from ansible.executor.task_queue_manager import FinalQueue, STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO
from ansible.executor.task_result import TaskResult
from ansible.inventory.host import Host
from ansible.module_utils.common.collections import is_sequence
from ansible.module_utils.common.text.converters import to_text
from ansible.parsing.dataloader import DataLoader
from ansible.playbook.task import Task
from ansible.playbook.play_context import PlayContext
from ansible.plugins.loader import init_plugin_loader
from ansible.utils.context_objects import CLIArgs
from ansible.plugins.action import ActionBase
from ansible.utils.display import Display
from ansible.vars.manager import VariableManager

from jinja2.exceptions import TemplateNotFound

__all__ = ['WorkerProcess']

display = Display()

current_worker = None


class WorkerQueue(Queue):
    """Queue that raises AnsibleError items on get()."""
    def get(self, *args, **kwargs):
        result = super(WorkerQueue, self).get(*args, **kwargs)
        if isinstance(result, AnsibleError):
            raise result
        return result


class WorkerProcess():  # type: ignore[name-defined]
    """
    The worker thread class, which uses TaskExecutor to run tasks
    read from a job queue and pushes results into a results queue
    for reading later.
    """

    def __init__(
            self,
            *,
            final_q: FinalQueue,
            task_vars: dict,
            host: Host,
            task: Task,
            play_context: PlayContext,
            loader: DataLoader,
            variable_manager: VariableManager,
            shared_loader_obj: types.SimpleNamespace,
            worker_id: int,
            cliargs: CLIArgs
    ) -> None:

        super(WorkerProcess, self).__init__()
        # takes a task queue manager as the sole param:
        self._final_q = final_q
        self._task_vars = task_vars
        self._host = host
        self._task = task
        self._play_context = play_context
        self._loader = loader
        self._variable_manager = variable_manager
        self._shared_loader_obj = shared_loader_obj

        # NOTE: this works due to fork, if switching to threads this should change to per thread storage of temp files
        # clear var to ensure we only delete files for this child
        self._loader._tempfiles = set()

        self.worker_queue = WorkerQueue()
        self.worker_id = worker_id

        self._cliargs = cliargs

        self._async_task = None

    def start(self) -> None:
        self._async_task = asyncio.create_task(self.run())

    async def run(self) -> None:
        """
        Wrap _run() to ensure no possibility an errant exception can cause
        control to return to the StrategyBase task loop, or any other code
        higher in the stack.

        As multiprocessing in Python 2.x provides no protection, it is possible
        a try/except added in far-away code can cause a crashed child process
        to suddenly assume the role and prior state of its parent.
        """
        # Set the queue on Display so calls to Display.display are proxied over the queue
        display.set_queue(self._final_q)
        return await self._run()

    async def _run(self) -> None:
        """
        Called when the process is started.  Pushes the result onto the
        results queue. We also remove the host from the blocked hosts list, to
        signify that they are ready for their next task.
        """

        # import cProfile, pstats, StringIO
        # pr = cProfile.Profile()
        # pr.enable()

        global current_worker
        current_worker = self

        try:
            # execute the task and build a TaskResult from the result
            display.debug("running TaskExecutor() for %s/%s" % (self._host, self._task))
            executor_result = await TaskExecutor(
                self._host,
                self._task,
                self._task_vars,
                self._play_context,
                self._loader,
                self._shared_loader_obj,
                self._final_q,
                self._variable_manager,
            ).run()

            display.debug("done running TaskExecutor() for %s/%s [%s]" % (self._host, self._task, self._task._uuid))

            # put the result on the result queue
            display.debug("sending task result for task %s" % self._task._uuid)
            try:
                self._final_q.send_task_result(TaskResult(
                    host=self._host,
                    task=self._task,
                    return_data=executor_result,
                    task_fields=self._task.dump_attrs(),
                ))
            except Exception as ex:
                try:
                    raise AnsibleError("Task result omitted due to queue send failure.") from ex
                except Exception as ex_wrapper:
                    self._final_q.send_task_result(TaskResult(
                        host=self._host,
                        task=self._task,
                        return_data=ActionBase.result_dict_from_exception(ex_wrapper),  # Overriding the task result, to represent the failure
                        task_fields={},  # The failure pickling may have been caused by the task attrs, omit for safety
                    ))

            display.debug("done sending task result for task %s" % self._task._uuid)

        except AnsibleConnectionFailure as ex:
            return_data = ActionBase.result_dict_from_exception(ex)
            return_data.pop('failed')
            return_data.update(unreachable=True)

            self._final_q.send_task_result(TaskResult(
                host=self._host,
                task=self._task,
                return_data=return_data,
                task_fields=self._task.dump_attrs(),
            ))

        except Exception as ex:
            if not isinstance(ex, (IOError, EOFError, KeyboardInterrupt, SystemExit)) or isinstance(ex, TemplateNotFound):
                try:
                    self._final_q.send_task_result(TaskResult(
                        host=self._host,
                        task=self._task,
                        return_data=ActionBase.result_dict_from_exception(ex),
                        task_fields=self._task.dump_attrs(),
                    ))
                except Exception:
                    display.debug(u"WORKER EXCEPTION: %s" % to_text(ex))
                    display.debug(u"WORKER TRACEBACK: %s" % to_text(traceback.format_exc()))
                finally:
                    self._clean_up()

        display.debug("WORKER PROCESS EXITING")

        # pr.disable()
        # s = StringIO.StringIO()
        # sortby = 'time'
        # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        # ps.print_stats()
        # with open('worker_%06d.stats' % os.getpid(), 'w') as f:
        #     f.write(s.getvalue())

    def is_alive(self):
        async_task = self._async_task
        return async_task and not async_task.done()

    def _clean_up(self) -> None:
        # NOTE: see note in init about forks
        # ensure we cleanup all temp files for this worker
        self._loader.cleanup_all_tmp_files()

    def close(self):
        pass
