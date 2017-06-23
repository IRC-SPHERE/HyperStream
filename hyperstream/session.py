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

from .utils import Printable, utcnow
from .models import SessionModel, UnameField
from .stream import StreamId

import os
import uuid
from mongoengine.context_managers import switch_db


class Session(object):
    def __init__(self, hyperstream, model=None):
        """
        Initialise the session object. A hyperstream session is used to store execution history
        :param hyperstream: The hyperstream object.
        :param model: The mongo model.
        
        :type hyperstream: HyperStream
        :type model: [SessionModel | None]
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
                    end=None
                )

                self._model.save()

        self._hyperstream = hyperstream
        self._history = None

        # TODO: Change the below to mongo stream when we're done
        self._history = self._hyperstream.channel_manager.memory.get_or_create_stream(StreamId("session"))

    def __str__(self):
        return repr(self)

    def __repr__(self):
        name = self.__class__.__name__
        values = ", ".join("{}={}".format(k, Printable.format(v))
                           for k, v in sorted(self._model.to_dict().items())
                           if k[0] != "_")
        return "{}({})".format(name, values)

    # def __del__(self):
    #     with switch_db(SessionModel, "hyperstream"):
    #         self._model.end = utcnow()
    #         self._model.active = False
    #         self._model.save()
    #         if self._hyperstream:
    #             self._hyperstream.current_session = None

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

    @property
    def active(self):
        return self._model.active

    @active.setter
    def active(self, value):
        self._model.active = value
        self._model.save()

    @staticmethod
    def get_sessions(hyperstream):
        with switch_db(SessionModel, "hyperstream"):
            for s in SessionModel.objects():
                yield Session(hyperstream=hyperstream, model=s)

    @staticmethod
    def clear_inactive_sessions(current_session, inactive_only=True):
        query = dict()
        if inactive_only:
            query['active'] = False
        if current_session is not None:
            query['session_id__ne'] = current_session.session_id

        with switch_db(SessionModel, "hyperstream"):
            SessionModel.objects(**query).delete()

    @staticmethod
    def get_uname():
        return dict(zip(("sysname", "nodename", "release", "version", "machine"), os.uname()))
