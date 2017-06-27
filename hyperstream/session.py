# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

from .utils import Printable, utcnow, StreamNotFoundError
from .models import SessionModel, UnameField
from .stream import StreamId, StreamInstance
from .time_interval import TimeInterval

import os
import uuid
from mongoengine.context_managers import switch_db


class Session(object):
    def __init__(self, hyperstream, model=None, history_channel='mongo'):
        """
        Initialise the session object. A hyperstream session is used to store execution history
        :param hyperstream: The hyperstream object.
        :param model: The mongo model.
        
        :type hyperstream: HyperStream
        :type model: SessionModel
        """

        if model:
            self._model = model
        else:
            with switch_db(SessionModel, "hyperstream"):
                self._model = SessionModel(
                    session_id=uuid.uuid1(),
                    uname=UnameField(**self.get_uname()),
                    start=utcnow(),
                    active=True,
                    end=None,
                    history_channel=history_channel
                )

                self._model.save()

        self._hyperstream = hyperstream
        self._history_stream = None
        self._history_channel = self._hyperstream.channel_manager[history_channel]

        stream_id = StreamId("session", meta_data=(('uuid', str(self.session_id)), ))
        self._history_stream = self._history_channel.get_or_create_stream(stream_id)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        name = self.__class__.__name__
        values = ", ".join("{}={}".format(k, Printable.format(v))
                           for k, v in sorted(self._model.to_dict().items())
                           if k[0] != "_")
        return "{}({})".format(name, values)

    @property
    def session_id(self):
        return self._model.session_id

    @property
    def uname(self):
        return self._model.uname

    @property
    def start(self):
        return self._model.start

    @property
    def end(self):
        return self._model.end

    @end.setter
    def end(self, value):
        """
        Setter for session end time. Not that _model.save() is not called (to avoid multiple db round-trips)

        :param value: The end time 
        """
        self._model.end = value

    @property
    def active(self):
        return self._model.active

    @active.setter
    def active(self, value):
        """
        Setter for session active status. Not that _model.save() is not called (to avoid multiple db round-trips)
        
        :param value: The active flag 
        """
        self._model.active = value

    @staticmethod
    def get_sessions(hyperstream):
        with switch_db(SessionModel, "hyperstream"):
            for s in SessionModel.objects():
                yield Session(hyperstream=hyperstream, model=s)

    @staticmethod
    def clear_sessions(hyperstream, inactive_only=True, clear_history=False):
        """
        Clear all (inactive) sessions and (optionally) their history
        
        :param hyperstream: The hyperstream object
        :param inactive_only: Whether to only clear inactive sessions (active sessions may be owned by another process)
        :param clear_history: Whether to clear the history of the session. Note that this will only clear the history 
        if the creator is the same process: there could feasibly be a history stored in a file channel that is not 
         accessible by this process.
        
        """
        query = dict()
        if inactive_only:
            query['active'] = False
        if hyperstream.current_session is not None:
            query['session_id__ne'] = hyperstream.current_session.session_id

        with switch_db(SessionModel, "hyperstream"):
            for s in SessionModel.objects(**query):
                if clear_history:
                    channel = hyperstream.channel_manager[s.history_channel]
                    stream_id = StreamId("session", meta_data=(('uuid', str(s.session_id)),))
                    try:
                        channel.purge_stream(stream_id, remove_definition=True, sandbox=None)
                    except StreamNotFoundError:
                        pass
                s.delete()

    @staticmethod
    def get_uname():
        return dict(zip(("sysname", "nodename", "release", "version", "machine"), os.uname()))

    @property
    def history(self):
        return self._history_stream.window(TimeInterval.up_to_now()).items()

    def write_to_history(self, **kwargs):
        instance = StreamInstance(utcnow(), kwargs)
        self._history_stream.writer(instance)

    def close(self):
        """
        Close the current session
        """
        self.active = False
        self.end = utcnow()
        self._model.save()
