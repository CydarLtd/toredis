from tornado.testing import AsyncTestCase
import time
from toredis.client import Client
from tornado import gen

class TestClient(AsyncTestCase):
    """ Test the client """

    def test_connect(self):
        client = Client()
        result = {}

        def callback():
            result["connected"] = True
            self.stop()
        client.connect(callback=callback)
        self.wait()
        # blocks
        self.assertTrue("connected" in result)

    @gen.engine
    def test_set_command(self):
        client = Client()
        result = {}

        def set_callback(response):
            result["set"] = response
            self.stop()

        client.connect()
        client.set("foo", "bar", callback=set_callback)
        self.wait()
        #blocks
        self.assertTrue("set" in result)
        self.assertEqual(result["set"], b"OK")

        conn = Client()
        conn.connect()
        value = yield gen.Task(conn.get, "foo")
        self.assertEqual("bar", value)

    @gen.engine
    def test_get_command(self):
        client = Client()
        result = None

        def get_callback(response):
            result = response
            self.stop()
        time_string = "%s" % time.time()
        conn = Client()
        conn.connect()
        yield gen.Task(conn.set, "foo", time_string)

        client.connect()
        client.get("foo", callback=get_callback)
        self.wait()
        #blocks

        self.assertTrue(result is not None, 'result is %s' % result)
        self.assertEqual(time_string, result)

    @gen.engine
    def test_sub_command(self):
        client = Client()
        result = {"message_count": 0}
        conn = Client()
        conn.connect()

        client.connect()
        response = yield gen.Task(client.subscribe, "foobar")
        if response[0] == "subscribe":
            result["sub"] = response
            yield gen.Task(conn.publish, "foobar", "new message!")
        elif response[0] == "message":
            result["message_count"] += 1
            if result["message_count"] < 100:
                count = result["message_count"]
                value = yield gen.Task(conn.publish,
                                       "foobar", "new message %s!" % count)
            result["message"] = response[2]

        self.assertTrue("sub" in result)
        self.assertTrue("message" in result)
        self.assertTrue(result["message"], "new message 99!")

    def test_pub_command(self):
        client = Client()
        result = {}

        def pub_callback(response):
            result["pub"] = response
            self.stop()
        client.connect()
        client.publish("foobar", "message", callback=pub_callback)
        self.wait()
        # blocks
        self.assertTrue("pub" in result)
        self.assertEqual(result["pub"], 0)  # no subscribers yet

    def test_blpop(self):
        client = Client()
        result = {}

        def rpush_callback(response):
            result["push"] = response

            def blpop_callback(response):
                result["pop"] = response
                self.stop()

            client.blpop("test", 0, blpop_callback)

        client.connect()
        client.rpush("test", "dummy", rpush_callback)
        self.wait()
        self.assertEqual(result["pop"], [b"test", b"dummy"])

    def test_disconnect(self):
        client = Client()
        client.connect()
        client.close()
        with self.assertRaises(IOError):
            client._stream.read_bytes(1024, lambda x: x)

    def test_pubsub_disconnect(self):
        client = Client()
        client.connect()
        client.subscribe("foo", lambda: None)
        client.close()
        with self.assertRaises(IOError):
            client._stream.read_bytes(1024, lambda x: x)
