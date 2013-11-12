"""
Copyright (c) 2013, sjatkins
All rights reserved.

license: BSD  (see LICENSE)
"""
import time
import os
import json
import types
import threading
import pickle
import Queue
import random

import zmq



#this is for types of message function names or subscription keys that should not be in even debug logs
#use only if you have super sensitive customer stuff potentially exposed
from zinterface import interface

DO_NOT_LOG_CONTENTS = []
logger = interface.get_logger('zinterface')

def random_name(base):
    return '%s_%d' % (base, random.getrandbits(24))


def ok_to_log(message, level = interface.logging.DEBUG):
    if logger.level > level: return True
    for s in DO_NOT_LOG_CONTENTS:
        if s in message:
            return False
    return True

def ok_to_log_arguments(args, kwargs):
    stringified = '%r %r' % (args, kwargs.values())
    return ok_to_log(stringified, 999)

max_reply_millis = 1000  #maximum expected reply time for the majority of messages

class ZMQInterface(interface.Interface):

    def __init__(self, *args, **kwargs):
        super(ZMQInterface, self).__init__(*args, **kwargs)
        self._zcontext = None

    def zcontext(self):
        if not self._zcontext:
            self._zcontext = zmq.Context()
        return self._zcontext
    
    def create_client(self):
        return ZMQClient(self)

    def create_server_core(self):
        return ZMQServer(self)

    def create_subscriber(self):
        return ZMQSubscriber(self)

    def create_publisher(self):
        logger.debug('creating %s publisher', self.name)
        return ZMQPublisher(self)


class ZMQClient(interface.InterfaceClient):
    _max_message_num = 99999
    def __init__(self, interface):
        super(ZMQClient, self).__init__(interface)
        self._socket = self._interface.zcontext().socket(zmq.DEALER)
        self._my_identity = '%s.%d' % (threading.current_thread().name, os.getpid())
        self._socket.setsockopt(zmq.IDENTITY, self._my_identity)
        self._socket.connect('tcp://localhost:%d' % self._interface._request_port)
        self._message_num = 0
        self.sender = self.Sender(self, interface)

    def _message_id(self):
        mid = str(self._message_num)
        self._message_num += 1
        if self._message_num > self._max_message_num:
            self._message_num = 0
        return mid

    class Sender(object):
        def __init__(self, client, interface):
            self.client = client
            self._interface = interface

        def set_name(self, method_name):
            self._name = method_name
            return self


        def __call__(self, *args, **kwargs):
            timeout = 1000
            try:
                async = kwargs.get('__async', False)
                wait_on_it = kwargs.pop('__wait', None)
                msg = self.client.marshall_call(self._name, args, kwargs)
                _current_mid = self.client._message_id()
                if ok_to_log(msg):
                    logger.debug('sending message #%s contents %s for client %s',
                        _current_mid, msg, self.client._my_identity)

                self.client._socket.send_multipart([_current_mid, msg])
                if not async:
                    while True:
                        got_something = self.client._socket.poll(None if wait_on_it else timeout)
                        if not got_something:       # alert the grammar police!
                            got_something = self.client._socket.poll(0)
                        if got_something:
                            _mid, reply = self.client._socket.recv_multipart()
                            if _mid != _current_mid:
                                logger.debug('%s got reply to previous message %s when on message %s',
                                    self.client.name , _mid, _current_mid)
                                continue
                            else:
                                return self.client.unmarshall_reply(reply)
                        else:
                            print 'nothing received'
                            logger.error('call to interface %s.%s from %s took over %d second[s] to come back.',
                                self.client._interface.name, self._name, self.client._my_identity, timeout/1000)
                            return None
            except:
                if ok_to_log_arguments(args, kwargs):
                    logger.exception('error calling %s.%s with args %s, kwargs %s', self.client.name, self._name, args, kwargs)
                else:
                    logger.exception('error calling %s.%s', self.client.name, self._name)
                raise

    def __getattr__(self, name):
        return self.sender.set_name(name)

    def marshall_call(self, name, args, kwargs):
        return json.dumps([name, args, kwargs])

    def unmarshall_reply(self, reply):
        retval, exception = json.loads(reply)
        logger.debug('retval = %r, exception = %r', retval, exception)
        if exception:
            exception = pickle.loads(str(exception))
            raise exception
        else:
            return retval

class ZMQServer(interface.InterfaceServer):
    def __init__(self, interface):
        super(ZMQServer, self).__init__(interface)
        self.running = False
        self._socket = self.zcontext().socket(zmq.ROUTER)
        self._socket.bind('tcp://*:%d' % interface._request_port)

    def _wrap_exception(self, exception):
        if isinstance(exception, basestring):
            exception = Exception(exception)
        return pickle.dumps(exception)


    def reply_with(self, return_val, exception=None):
        reply = self.marshall_reply(return_val, exception)
        self._socket.send(reply)

    def handle_msg(self, fname=None):
        sender, message_id, msg = self._socket.recv_multipart()
        print 'logger is', logger
        if ok_to_log(msg):
            logger.debug("received message %s with id %s from client %s",
                         msg, message_id, sender)


        def reply_with(return_val, exception=None, __async=False): #specialized for multipart
            if not __async:
                reply = self.marshall_reply(return_val, exception)
                logger.debug("replying with %r", reply)
                self._socket.send_multipart([sender, message_id, reply])

        return_val, exception = None, None
        fname, args, kwargs = None, [], {}
        async = False
        try:
            fname, args, kwargs = self.unmarshall_request(msg)
            async = kwargs.pop('__async', False)
#            if ok_to_log_arguments(args, kwargs):
#                logger.debug('message components %r,%r,%r', fname, args, kwargs)
        except interface.BadCallException, e:
            reply_with(None, e, async)
            if ok_to_log(msg):
                logger.exception('could not unmarshall request to %s interface: %s', self.name, msg)
            else:
                logger.exception('could not unmarshall request to %s interface: containing log masked elements in %r',
                                 self.name, DO_NOT_LOG_CONTENTS)

        func = self._function_map.get(fname)
        if func:
            try:
                return_val = func(*args, **kwargs)
            except Exception, e:
                if ok_to_log_arguments(args, kwargs):
                    logger.debug('%s invocation of %s(%s,%s) threw exception', self.name, fname, args, kwargs)
                else:
                    logger.debug('%s invocation of %s threw exception', self.name, fname)

                exception = e
        else:
            s = "%s interface serves no such function: %r" % (self.name, fname)
            exception = Exception(s)
            logger.debug(s)

        reply_with(return_val, exception, async)

    def unmarshall_request(self, message):
        """
        expects simple json string containing a list:
        [<funtion_name>, <arg_list>, <keyword_arg_dictionary>]
        """
#        if ok_to_log(message):
#            logger.debug('attempting to unmarshall %s', message)
        
        parts = json.loads(message)
        if type(parts) != types.ListType or len(parts) != 3:
            raise interface.BadCallException(message)
        func_name, args, kwargs = parts
        return func_name, args, kwargs

    def poll(self, milliseconds):
        try:
            if self._socket.poll(milliseconds) & zmq.POLLIN == 0:
                return self._socket.poll(0) & zmq.POLLIN == 0
        except zmq.ZMQError:
            return False
        self.handle_msg()
        return True

    def marshall_reply(self, retval, exception):
        try:
            if exception:
                exception = self._wrap_exception(exception)
            return json.dumps((retval, exception))
        except:
            logger.exception('failed to marshal reply retval %r, exception %r', retval, exception)
            return json.dumps((None, None))

def unmarshall_pub(msg):
    """assumes json message"""
    res =  json.loads(msg)
    return res

class ZMQGenericSubscriber(object):
    _instance = None
    _class_lock = threading.RLock()

    @classmethod
    def instance(cls, context):
        with cls._class_lock:
            if not cls._instance:
                cls._instance = cls(context)
            return cls._instance

    def __init__(self, context):
        self.zcontext = context
        self._interfaces_of_interest = {}
        self._poll = zmq.Poller()
        self._wait_for_sockets = threading.Event()
        self._loop = threading.Thread(target = self.handle_subscribed_info, name = random_name('generic_subscription'))
        self._loop.setDaemon(True)
        self._loop.start()
        time.sleep(1)

    def _add_interface(self, interface_name):
        subscriber = interface.subscriber(interface_name)
        logger.debug("found %r for %s" , interface_name, subscriber)
        if subscriber:
            self._interfaces_of_interest[interface_name] = subscriber
            self._poll.register(subscriber.socket(), zmq.POLLIN)
            logger.debug('registered %s with socket %r', subscriber.name, subscriber.socket())
        else:
            logger.debug("no interface named %s found", interface_name)

    def handle_subscribed_info(self):
        my_socket = self.zcontext.socket(zmq.PULL)
        my_socket.bind('inproc://general_subscriber')
        self._poll.register(my_socket, zmq.POLLIN)
        while True:
            #self._wait_for_sockets.wait() #nothing to do until then
            try:
                sockets = dict(self._poll.poll(10000))
            except:
                continue
            for socket in sockets:
                msg = socket.recv()
                if ok_to_log(msg):
                    logger.debug('generic subscriber received %s', msg)
                if socket == my_socket:
                    #only 1 message which is interface
                    self._add_interface(msg)
                else:
                    if msg == 'STOP':
                        break
                    try:
                        data = unmarshall_pub(msg)
                    except:
                        logger('exception unmarhalling data in GeneralSubscriber: %r', data)
                    try:
                        if ok_to_log(msg):
                            logger.debug('got data %s', data)
                        interface_name, key, valuelist = data
                        subscriber = self._interfaces_of_interest.get(interface_name)
                        if subscriber:
                            subscriber.handle_signal(key, valuelist)
                            subscriber._event.set()
                        else:
                            logger.debug('no subscriber info for %s', interface_name)
                    except:
                        logger.exception('exception firing callback for %s(%s,%r)', interface_name, key, valuelist)

class ZMQSubscriber(interface.InterfaceSubscriber):
    _general_sub = None
    _lock = threading.RLock()
    @classmethod
    def general_sub(cls, context):
        with cls._lock:
            if not cls._general_sub:
                inst = ZMQGenericSubscriber.instance(context)
                cls._general_sub = context.socket(zmq.PUSH)
                cls._general_sub.connect('inproc://general_subscriber')
            return cls._general_sub

    def __init__(self, interface):
        super(ZMQSubscriber, self).__init__(interface)
        port = self.subscribe_port()
        self._socket = None
        sub = self.general_sub(self.zcontext())
        time.sleep(1)
        sub.send(self.name)
#        ZMQGenericSubscriber.instance().add_interface(self)

    def socket(self):
        port = self.subscribe_port()
        if not self._socket:
            self._socket = self.zcontext().socket(zmq.SUB)
            self._socket.connect('tcp://localhost:%d' % port)
            self._socket.setsockopt(zmq.SUBSCRIBE, "")
        logger.debug('%s subscriber on port %d, socket %r',
            self.name, port, self._socket)
        return self._socket


class ZMQPublisher(interface.InterfacePublisher):
    class PublishThread(threading.Thread):
        def __init__(self, publisher):
            self.owner = publisher
            super(self.__class__, self).__init__(name='%s_publisher' % publisher.name)
            self.daemon = True

        def run(self):
            self._socket = self.owner.zcontext().socket(zmq.PUB)
            port = self.owner.subscribe_port()
            self._socket.bind('tcp://*:%d' % port)
            logger.debug('%s publisher bound to %d', self.owner.name, port)
            while True:
                try:
                    msg = self.owner.queue.get()
                    logger.debug('%s publishing sent %s', self.owner.name, msg)
                    self._socket.send(msg)
                except:
                    logger.exception('publishng thread died!!!')
                    
    def __init__(self, interface):
        super(ZMQPublisher, self).__init__(interface)
        self.queue = Queue.Queue()
        self._pub_thread = self.PublishThread(self)
        self._pub_thread.start()

    def publish(self, key, *value_list):
        pub_msg = self.marshall_pub(key, value_list)
        logger.debug('%s publishing queued %s', self.name, pub_msg)
        self.queue.put(pub_msg)
        time.sleep(0.001) #give other threads a chance

    def marshall_pub(self, key, value_list):
        return json.dumps([self._interface.name, key, value_list])

