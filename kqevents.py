# -*- coding: utf-8 -*-

import threading


class FBEventThread(threading.Thread):
    events = None
    db = None
    event_callback = None
    running = None

    def __init__(self, db, event_name, event_callback):
        self.db = db
        self.event_name = event_name
        self.event_callback = event_callback
        self.running = False
        threading.Thread.__init__(self)

    def run(self):
        self.running = True
        while self.running:
            # http://kinterbasdb.sourceforge.net/dist_docs/usage.html#adv_event
            events = [self.event_name]
            conduit = self.db.event_conduit(events)
            conduit.wait()
            try:
                self.event_callback()
            except Exception as e:
                print('Event callback exception: ' + e.message)
