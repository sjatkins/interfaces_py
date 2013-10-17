import unittest, time, threading
from zinterface import interface, zmq_interface as i_zmq

foo_request_port = 9990
foo_subscribe_port = 9991
test_request_port = 9992
test_subscribe_port = 9993

_foo_server = None
_server_thread = None

def simply_add(x, y):
    return x + y

def make_foo_server():
    global _foo_sorver, _server_thread
    i_foo = interface.get('foo')
    _foo_server = i_foo.create_server_core()
    _foo_server.register_function(simply_add)
    _server_thread = interface.server_thread('foo_server', _foo_server)
    _server_thread.start()



class TestInterface(unittest.TestCase):
    _foo = None
    _lock = threading.RLock()
    _foo_sub = _test_sub = _foo_pub = _test_pub = None


    @classmethod
    def foo(cls):
        with cls._lock:
            if not cls._foo:
                print 'setting up foo and 1st time stuff'
                cls._foo = interface.create_interface('foo', foo_request_port, foo_subscribe_port)
                cls._test = interface.create_interface('test_interface', test_request_port, test_subscribe_port)
                #first time stuff
                make_foo_server()
                #hook up all subscribers and publishers early to avoid test latency issues
                cls._foo_sub = cls._foo.subscriber()
                cls._test_sub = interface.subscriber('test_interface')
                cls._foo_pub = cls._foo.publisher()
                cls._test_pub = interface.publisher('test_interface')
                time.sleep(1)
            return cls._foo

    def setUp(self):
        f = self.foo()

    def test_a_exists(self):
        self.assertTrue(self.foo() != None)
        self.assertTrue(self.foo()._request_port == foo_request_port)
        self.assertTrue(self.foo()._subscribe_port == foo_subscribe_port)

    def got_signal(self, key, value):
        print 'got_signal', key, value
        self._signal_received = (key, value)

    def test_f_pub_sub_loop(self):
        self._signal_received = None
        foo = self.foo()
        foo.add_callback(self.got_signal, 'whatever')
        foo.publish('whatever', 'my data')
        time.sleep(0.001) #give other thread a chance
        foo.subscriber().wait(3)
        self.assertTrue(self._signal_received)


    def test_b_per_thread_client(self):
        """check that we have a different client per thread"""
        pass

    def test_c_request1(self):
        a = self.foo()().simply_add(33, 47)
        self.assertTrue(a == 80)

    def test_d_get_subscriber(self):
        subscriber = self.foo().subscriber()
        print 'foo local subscriber = %s' % self.foo()._local_subscriber
        self.assertTrue(self.foo().subscriber() != None)


    def my_callback(self, *stuff):
        print "my_callback", stuff
        self._my_callback_called = True

    def test_e_callback(self):
        self._my_callback_called = False
        foo = self.foo()
        foo.add_callback(self.my_callback, 'test_signal')
        self.foo().publish('test_signal', 'nothing')
        time.sleep(0.001)
        foo.subscriber().wait(3)
        self.assertTrue(self._my_callback_called)

    def test_e2_callback(self):
        self._my_callback_called = False
        i_test = interface.get('test_interface')
        i_test.add_callback(self.my_callback, 'test_signal')
        i_test.publish('test_signal', 'nothing')
        time.sleep(0.001)
        i_test.subscriber().wait(3)
        self.assertTrue(self._my_callback_called)

    def tearDown(self):
        pass

if __name__ == "__main__":
    print "testing IPC interfaces.."
    suite = unittest.TestLoader().loadTestsFromTestCase(TestInterface)
    #run these outside the framework as the timing for the subscription threads
    def run_standalone_test(test_fname):
        try:
            print 'standalone test of TestInterface.%s' % test_fname
            test = TestInterface(test_fname)
            test.run()
            print "ok\n"

        except:
            print '%s got exception' % test_fname
            raise

    #is too tricky to get right within it.
    #run_standalone_test('e_callback')
    #run_standalone_test('f_pub_sub_loop')
    unittest.TextTestRunner(verbosity=2).run(suite)
