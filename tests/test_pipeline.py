from tornado.testing import AsyncTestCase

from toredis.client import Client
from toredis.pipeline import Pipeline

from hiredis import ReplyError


class TestPipeline(AsyncTestCase):

    def setUp(self):
        super(TestPipeline, self).setUp()
        self.client = Client()
        self.client.connect(callback=self.stop)
        self.wait()
        self.client.flushdb(callback=self.stop)
        self.wait()

    def tearDown(self):
        self.client.close()
        super(TestPipeline, self).tearDown()

    def test_pipeline_send(self):
        pipeline = self.client.pipeline()
        pipeline.set('foo1', 'bar1')
        pipeline.set('foo2', 'bar2')
        pipeline.set('foo3', 'bar3')
        pipeline.get('foo1')
        pipeline.mget(['foo2', 'foo3'])
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertEqual(
            response, [b'OK', b'OK', b'OK', b'bar1', [b'bar2', b'bar3']]
        )

    def test_pipeline_reset(self):
        pipeline = self.client.pipeline()
        pipeline.set('foo1', 'bar1')
        pipeline.get('foo1')
        pipeline.reset()
        pipeline.set('foo2', 'bar2')
        pipeline.get('foo2')
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertEqual(response, [b'OK', b'bar2'])

    def test_pipeline_with_error(self):
        pipeline = self.client.pipeline()
        pipeline.eval('invalid', [], [])
        pipeline.eval(
            'return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}',
            ['key1', 'key2'], ['arg1', 'arg2']
        )
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertIsInstance(response[0], ReplyError)
        self.assertEqual(response[1], [b'key1', b'key2', b'arg1', b'arg2'])

    def test_pipeline_small(self):
        pipeline = self.client.pipeline()
        pipeline.set('foo', 'bar')
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertEqual(response, [b'OK'])

    def test_pipeline_big(self):
        pipeline = self.client.pipeline()
        expected = []
        for i in range(10000):
            pipeline.set('foo%d' % i, 'bar%d' % i)
            pipeline.get('foo%d' % i)
            expected.extend([b'OK', b'bar' + str(i).encode()])
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertEqual(response, expected)

    def test_pipeline_and_commands(self):
        pipeline = self.client.pipeline()
        pipeline.set('foo1', 'bar1')
        pipeline.get('foo1')
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertEqual(response, [b'OK', b'bar1'])

        self.client.eval(
            'return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}',
            ['key1', 'key2'], ['arg1', 'arg2'],
            callback=self.stop
        )
        response = self.wait()
        self.assertEqual(response, [b'key1', b'key2', b'arg1', b'arg2'])

        pipeline.set('foo2', 'bar2')
        pipeline.get('foo2')
        pipeline.send(callback=self.stop)
        response = self.wait()
        self.assertEqual(response, [b'OK', b'bar2'])

    def test_parallel_pipeline_and_commands(self):
        result = {}

        def callback_pipeline1(response):
            result['pipeline1'] = response
            self.stop()

        def callback_pipeline2(response):
            result['pipeline2'] = response
            self.stop()

        def callback_eval(response):
            result['eval'] = response
            self.stop()

        pipeline = self.client.pipeline()
        pipeline.set('foo1', 'bar1')
        pipeline.get('foo1')
        pipeline.send(callback=callback_pipeline1)

        self.client.eval(
            'return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}',
            ['key1', 'key2'], ['arg1', 'arg2'],
            callback=callback_eval
        )

        pipeline.set('foo2', 'bar2')
        pipeline.get('foo2')
        pipeline.send(callback=callback_pipeline2)

        self.wait(lambda: len(result) == 3)
        self.assertEqual(result, {
            'pipeline1': [b'OK', b'bar1'],
            'eval': [b'key1', b'key2', b'arg1', b'arg2'],
            'pipeline2': [b'OK', b'bar2']
        })

    def test_parallel_pipelines(self):
        result = {}

        def callback_pipeline1(response):
            result['pipeline1'] = response
            self.stop()

        def callback_pipeline2(response):
            result['pipeline2'] = response
            self.stop()

        pipeline1 = Pipeline(self.client)
        pipeline1.set('foo1', 'bar1')
        pipeline1.get('foo1')
        pipeline1.send(callback=callback_pipeline1)

        pipeline2 = Pipeline(self.client)
        pipeline2.set('foo2', 'bar2')
        pipeline2.get('foo2')
        pipeline2.send(callback=callback_pipeline2)

        self.wait(lambda: len(result) == 2)
        self.assertEqual(result, {
            'pipeline1': [b'OK', b'bar1'],
            'pipeline2': [b'OK', b'bar2']
        })
