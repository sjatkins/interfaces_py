"""
Copyright (c) 2013, sjatkins
All rights reserved.

license: BSD  (see LICENSE)
"""
import inspect, threading, time
from collections import defaultdict
import logging

tlocal = threading.local()

log_module = logging

def get_logger():
    return log_module.getLogger('interface_py')

logger = get_logger()

def inject_logger(logging_module):
    """optionally set a custom logging module"""
    global log_module, logger
    log_module = logging_module
    logger = get_logger()

RPC_TYPE = '0mq'
_interface_class = None

def interface_class():
    """
    By default we presume we are working with 0mq interfaces.  If you want to build your own subclasses
    to use some other kind of interface with these same abstractions then you can change RPC_TYPE after
    importing.
    """
    global _interface_class
    if not _interface_class:
        from zmq_interface import ZMQInterface
        _interface_class = ZMQInterface
    return _interface_class

class FunctionDescription:
    """
    Class to be used internally for describing functions available from an interface server. Not
    actually fully utilized. (sja)
    """
    def __init__(self, func):
        self._name = func.__name__
        self._doc = inspect.getdoc(func)
        self._parameters = inspect.getargspec(func)
        self._func = func
        self._declaration = None

    def args(self):
        return self._parameters.args

    def defaults(self):
        return self._parameters.defaults

    def varargs(self):
        return self._parameters.varargs

    def function(self):
        return self._func

    def _func_def(self):
        """reconstruct function declaration from introspection"""
        spec = self._parameters
        args = spec.args
        partial_defaults = spec.defaults if spec.defaults else []
        defaults = []
        for _ in range(len(args) - len(partial_defaults)):
            defaults.append(None)
        defaults.extend(partial_defaults)
        arglist = []
        for arg, default in zip(args, defaults):
            part = ('%s = %s' % (arg, default)) if default else arg
            arglist.append(part)
        if spec.varargs:
            arglist.append('*%s' % spec.varargs)
        if spec.keywords:
            arglist.append('**%s' % spec.keywords)
        return '%s(%s)' % (self._name, ", ".join(arglist))

    def declaration(self):
        if not self._declaration:
            self._declaration = self._func_def()
        return self._declaration

    def description(self):
        return '%s\n\t%s' % (self.declaration(), self._doc)

class BadCallException(Exception):
    def __init__(self, message_received):
        msg = 'Unexpected protocol: expected json encoded list with a string, a list and a dictionary. Got %s' % message_received
        super(BadCallException, self).__init__(msg)

class NoSuchFunctionException(Exception):
    def __init__(self, fname):
        msg = "No such function: %r" % fname
        super(NoSuchFunctionException, self).__init__(msg)

class Interface(object):
    """
    Base Interface abstraction.  Interfaces have
    - name : A name
    - request_port: port that a server for this interface receives requests on
    - subscribe_port: port to subscribe to to receive PUBs from the interface server

    A request is made by using the interface as a function.  Since 0mq sockets are not
    thread-safe the first call in a thread produces a thread local client that will be
    reused for subsequent calls.
    """

    def __init__(self, name, request_port, subscribe_port,
                 description = None, messages = None, events = None):
        self.name = name
        events = events if events else []
        messages = messages if messages else []
        self._request_port = request_port
        self._subscribe_port = subscribe_port

        #here you see we aren't using our nice function description above
        self._functions = dict([(k,None) for k in messages])
        self._function_descriptions = dict([k, None] for k in messages)
        #prespecifying events is optional
        self._events = dict([(k, None) for k in events])
        #multiple threads could be be accessing the instance and it places it matters
        self._instance_lock = threading.RLock()
        self._local_subscriber = None
        self._publisher = None

    def messages(self):
        return self._functions


    def list_messages(self):
        for name, desc in self._function_descriptions.iteritems():
            fd = desc if desc else ""
            print '%s: %s' % (name, fd)

    def signals(self):
        return self._events

    def request_port(self):
        return self._request_port

    def subscribe_port(self):
        return self._subscribe_port

    @property
    def client(self):
        client_name = '%s_client' % self.name
        client = getattr(tlocal, client_name, None)
        if not client:
            logger.debug('creating client for interface %s, thread %s',
                         self.name, threading.currentThread().name)
            client = self.create_client()
            setattr(tlocal, client_name, client)
        return client

    def __call__(self):
        """
        Creates a client for sending a message to the interface or returns the existing client.
        Yeah I probably should just have this be a property called "client".  Pretty much a matter
        of taste which you like better but confuses some people.
        """
        return self.client

    def add_callback(self, callback, *keys):
        """
        Adds a call back for the given patterns of subscribed message keys.

        callback: handler function when subscribed messages with these keys are received
        *keys: list of keys that the handler will handle
        """
        try:
            return self.subscriber().add_callback(callback, *keys)
        except:
            logger.exception('failed to add callback %r on %s', callback, keys)

    def remove_callback(self, *keys):
        """
        Removes the callback handler[s] for the given set of keys.
        """
        try:
            return self.subscriber().remove_callback(*keys)
        except:
            logger.exception('failed to remove callback on %s', keys)

    def remove_specific_callback(self, callback, key):
        """
        Removes only the specific handler handling subscribed messages with the specific key
        """
        try:
            return self.subscriber().remove_specific_callback(callback, key)
        except:
            logger.exception('failed to remove callback %r on %s', callback, key)

    def publish(self, key, *value_list):
        """
        Publish with a message with the given key and values.
        """
        try:
            self.publisher().publish(key, *value_list)
        except:
            logger.exception('failed to publish key %s, %s', key, value_list)

    def update_function_signatures(self, object_or_module):
        """
        Garner function definitions by examining the given
        object or module and exposing all its functions as callable through this interface.
        """
        for f_name in self._functions:
            func = getattr(object_or_module, f_name, None)
            if func:
                self._functions[f_name] = FunctionDescription(func)

    def register_function(self, func):
        """
        Register a function that can be called through this interface
        """
        with self._instance_lock:
            fname = func.__name__
            if not fname in self._functions:
                self._functions[fname] = FunctionDescription(func)
                self._function_descriptions[fname] = FunctionDescription(func)
        return self

    def create_server_core(self):
        """returns a basic threaded or unthreaded server object which includes the means to deploy
        the server core as a thread or as a process.  Interfaces that have a standalone specialized
        server may use this to obtain base server functionality of the current type"""
        pass

    def subscriber(self):
        with self._instance_lock:
            if not self._local_subscriber:
                self._local_subscriber = self.create_subscriber()
            return self._local_subscriber

    def create_subscriber(self):
        """there should only be one subscriber per process for XMLRPC especially.
        Since subscriber has machinery to do callbacks per signal one should be sufficient.
        """
        return None

    def create_client(self):
        pass

    def publisher(self):
        with self._instance_lock:
            if not self._publisher:
                self._publisher = self.create_publisher()
            return self._publisher

    def create_publisher(self):
        pass


class InterfaceAccess(object):
    def __init__(self, interface):
        self._interface = interface

    def __getattr__(self, item):
        return getattr(self._interface, item)

class InterfaceClient(InterfaceAccess):
    """Interface request abstraction"""
    def __init__(self, interface):
        InterfaceAccess.__init__(self, interface)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)

class InterfaceServer(InterfaceAccess):
    """Interface reply abstraction"""
    def __init__(self, interface, threaded=False):
        """sets up the server without or without extra on-demand worker threads"""
        InterfaceAccess.__init__(self, interface)
        self.running = False
        self._function_map = {}

    def reply_with(self, return_val, exception=None):
        raise NotImplementedError('reply_with')

    def handle_msg(self, fname):
        if fname == 'stop_serving':
            self.stop_serving()
            return True
        if fname == 'ping':
            self.ping()
            return True
        return False

    def stop_serving(self):
        self.running = False

    def ping(self):
        return self.reply_with('pong')

    def serve_forever(self):
        self.running = True
        logger.debug("starting service: %s" % self._interface.name)
        while self.running:
            self.handle_msg()
        logger.debug("stopping service: %s" % self._interface.name)
        raise NotImplementedError('serve_forever')

    def poll(self, milliseconds):
        """Polls for milliseconds and returns whether anything
        is on the socket."""
        raise NotImplementedError('poll')

    def register_object(self, implementor, take_all_functions=False):
        """uses implementor for the implementation of as many of the defined Interface
        functions as possible.  If take_all_functions is true then all functions defined on
        implementor are placed in the function map and may be invoked by clients"""
        pass

    def register_function(self, func, as_name=None):
        """serves func.__name__ by calling func"""
        name = as_name if as_name else func.__name__
        #
        self._function_map[name] = func
        #TODO use FunctionDescription


class InterfaceSubscriber(InterfaceAccess):
    """interface subscribe abstraction"""
    def __init__(self, interface):
        """set up a subscription on the given interface that ignores all but the given signals"""
        InterfaceAccess.__init__(self, interface)
        self._callbacks = defaultdict(set)
        self._always_call = set()
        self._event = threading.Event()
        self._lock = threading.RLock()

    def wait(self, seconds):
        self._event.wait(seconds)
        self._event.clear()

    def add_callback(self, callback,  *change_list):
        """register a callback to be called if any of the specified keys are changed in subscribed
        information. The callback is called with the dictionary of new values for the keys changed"""
        with self._lock:
            for signal_key in change_list:
                self._callbacks[signal_key].add(callback)
            if not change_list:
                self._always_call.add(callback)

    def remove_callback(self, *keys):
        with self._lock:
            for key in keys:
                self._callbacks.pop(key, None)

    def remove_specific_callback(self, callback, key):
        with self._lock:
            callbacks = self._callbacks.get(key, set())
            callbacks.discard(callback)

    def handle_signal(self, key, value_list):
        if self._always_call:
            with self._lock:
                functions = [f for f in self._always_call]
            for func in functions:
                func(key, *value_list)
        exact_call = self._callbacks.get(key)
        if exact_call:
            with self._lock:
                functions = [f for f in exact_call]
            for call in functions:
                call(key, *value_list)

class InterfacePublisher(InterfaceAccess):
    """In the general case it is possible to have multiple publishers."""

    def __init__(self, interface):
        InterfaceAccess.__init__(self, interface)

    def publish(self, key, *values):
        pass

def use_interface_type(interface_type):
    global RPC_TYPE, interfaces, interface_map
    if interface_type not in ('XMLRPC', '0mq'):
        logger.error('%s in not implemented.  Only "XMLRPC" and "Rmq" are suported',
            interface_type)
    else:
        if interface_type != RPC_TYPE:
            #TODO consider case of mid_stream change IFF we support it. Perhaps in unittests only?
            interface_map.clear()
            RPC_TYPE = interface_type

interface_map = {}

def names():
    return interface_map.keys()

def create_interface(name, request_port, subscribe_port):
    interface = interface_class()(name, request_port, subscribe_port)
    interface_map[name] = interface
    return interface

def get(name):
    global interface_map
    interface = interface_map.get(name)
    return interface


def client(name):
    return get(name)()

def server(name):
    return get(name).create_server_core()

def server_thread(name, a_server=None):
    thread_name = '%s_server' % name
    if not a_server:
        a_server = server(name)
    return ServerThread(thread_name, a_server)

def subscriber(name):
    interface = get(name)
    return interface.subscriber() if interface else None

def publisher(name):
    interface = get(name)
    return interface.publisher() if interface else None

class ServerThread(threading.Thread):
    def __init__(self, name, server):
        super(ServerThread, self).__init__(name=name)
        self.server = server
        self.setDaemon(True)

    def run(self):
        try:
            self.server.serve_forever()
        except Exception:
            logger.exception('Error in server thread')
            raise

