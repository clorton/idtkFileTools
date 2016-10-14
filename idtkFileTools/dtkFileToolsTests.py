#!/usr/bin/python

import dtkFileTools
import json
import unittest


# Reading Tests

class TestReadingHappyPath(unittest.TestCase):

    def test_reading_version_one_uncompressed(self):
        dtk_file = dtkFileTools.DtkFile('test-data/simple.dtk')
        expected_text = '{"metadata":{"author":"clorton","tool":"notepad","compressed":false,"bytecount":69}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        expected_header.metadata.version = 1
        expected_header.metadata.engine = 'NONE'
        expected_header.metadata.chunkcount = 1
        expected_header.metadata.chunksizes = [expected_header.metadata.bytecount]
        self.assertEqual(expected_header, dtk_file.header)
        expected_chunk = '{"simulation":{"__class__":"SimulationPython","serializationMask":0}}'
        self.assertEqual(expected_chunk, dtk_file.get_chunk(0))
        self.assertEqual(expected_chunk, dtk_file.get_contents(0))
        obj = json.loads(expected_chunk, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        return

    def test_reading_version_one_compressed(self):
        dtk_file = dtkFileTools.DtkFile('test-data/version1.dtk')
        expected_text = '{"metadata":{"version":1,"date":"Sat Sep 17 07:01:52 2016","compressed":true,"bytecount":1438033}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        expected_header.metadata.engine = 'SNAPPY'
        expected_header.metadata.chunkcount = 1
        expected_header.metadata.chunksizes = [expected_header.metadata.bytecount]
        self.assertEqual(expected_header, dtk_file.header)
        with open('test-data/version1/version1.bin', 'rb') as handle:
            expected_chunk = handle.read()
        self.assertEqual(expected_chunk, dtk_file.get_chunk(0))
        with open('test-data/version1/version1.json', 'rb') as handle:
            expected_contents = handle.read()
        self.assertEqual(expected_contents, dtk_file.get_contents(0))
        obj = json.loads(expected_contents, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        return

    def test_reading_one_node_uncompressed(self):
        dtk_file = dtkFileTools.DtkFile('test-data/one-node/state-00010.dtk.none')
        # Header
        expected_text = '{"metadata":{"engine":"NONE","author":"clorton","tool":"dtkFileTools.py","bytecount":3247650,"version":2,"compressed":false,"date":"Thu Oct 13 18:05:34 2016","chunksizes":[479,3247171],"chunkcount":2}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(expected_header, dtk_file.header)
        # Simulation
        with open('test-data/one-node/state-00010.sim.json', 'rb') as handle:
            expected_sim_chunk = handle.read()
        self.assertEqual(expected_sim_chunk, dtk_file.get_chunk(0))
        expected_sim_content = expected_sim_chunk
        self.assertEqual(expected_sim_content, dtk_file.get_contents(0))
        obj = json.loads(expected_sim_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        self.assertEqual(obj['simulation'], dtk_file.sim)
        # Node
        with open('test-data/one-node/state-00010.node.json', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(1))
        expected_node_content = expected_node_chunk
        self.assertEqual(expected_node_content, dtk_file.get_contents(1))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(1))
        return

    def test_reading_one_node_ellzeefour(self):
        dtk_file = dtkFileTools.DtkFile('test-data/one-node/state-00010.dtk')
        # Header
        expected_text = '{"metadata":{"version":2,"date":"Fri Oct 14 00:45:50 2016","compressed":true,"engine":"LZ4","bytecount":92811,"chunkcount":2,"chunksizes":[360,92451]}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(expected_header, dtk_file.header)
        # Simulation
        with open('test-data/one-node/state-00010.sim.lz4', 'rb') as handle:
            expected_sim_chunk = handle.read()
        self.assertEqual(expected_sim_chunk, dtk_file.get_chunk(0))
        with open('test-data/one-node/state-00010.sim.json', 'rb') as handle:
            expected_sim_content = handle.read()
        self.assertEqual(expected_sim_content, dtk_file.get_contents(0))
        obj = json.loads(expected_sim_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        self.assertEqual(obj['simulation'], dtk_file.sim)
        # Node
        with open('test-data/one-node/state-00010.node.lz4', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(1))
        with open('test-data/one-node/state-00010.node.json', 'rb') as handle:
            expected_node_content = handle.read()
        self.assertEqual(expected_node_content, dtk_file.get_contents(1))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(1))
        return

    def test_reading_one_node_snappy(self):
        dtk_file = dtkFileTools.DtkFile('test-data/one-node/state-00010.dtk.snappy')
        # Header
        expected_text = '{"metadata":{"engine":"SNAPPY","author":"clorton","tool":"dtkFileTools.py","bytecount":242448,"version":2,"compressed":true,"date":"Thu Oct 13 18:12:20 2016","chunksizes":[350,242098],"chunkcount":2}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(expected_header, dtk_file.header)
        # Simulation
        with open('test-data/one-node/state-00010.sim.snappy', 'rb') as handle:
            expected_sim_chunk = handle.read()
        self.assertEqual(expected_sim_chunk, dtk_file.get_chunk(0))
        with open('test-data/one-node/state-00010.sim.json', 'rb') as handle:
            expected_sim_content = handle.read()
        self.assertEqual(expected_sim_content, dtk_file.get_contents(0))
        obj = json.loads(expected_sim_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        self.assertEqual(obj['simulation'], dtk_file.sim)
        # Node
        with open('test-data/one-node/state-00010.node.snappy', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(1))
        with open('test-data/one-node/state-00010.node.json', 'rb') as handle:
            expected_node_content = handle.read()
        self.assertEqual(expected_node_content, dtk_file.get_contents(1))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(1))
        return

    def test_reading_multinode_uncompressed(self):
        dtk_file = dtkFileTools.DtkFile('test-data/two-node/state-00010.dtk.none')
        # Header
        expected_text = '{"metadata":{"engine":"NONE","author":"clorton","tool":"dtkFileTools.py","bytecount":6960969,"version":2,"compressed":false,"date":"Thu Oct 13 20:32:06 2016","chunksizes":[482,3467569,3492918],"chunkcount":3}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(expected_header, dtk_file.header)
        # Simulation
        with open('test-data/two-node/state-00010.sim.json', 'rb') as handle:
            expected_sim_chunk = handle.read()
        self.assertEqual(expected_sim_chunk, dtk_file.get_chunk(0))
        expected_sim_content = expected_sim_chunk
        self.assertEqual(expected_sim_content, dtk_file.get_contents(0))
        obj = json.loads(expected_sim_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        self.assertEqual(obj['simulation'], dtk_file.sim)
        # Node 1
        with open('test-data/two-node/state-00010.node-1.json', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(1))
        expected_node_content = expected_node_chunk
        self.assertEqual(expected_node_content, dtk_file.get_contents(1))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(1))
        # Node 2
        with open('test-data/two-node/state-00010.node-2.json', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(2))
        expected_node_content = expected_node_chunk
        self.assertEqual(expected_node_content, dtk_file.get_contents(2))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(2))
        return

    def test_reading_multinode_ellzeefour(self):
        dtk_file = dtkFileTools.DtkFile('test-data/two-node/state-00010.dtk.lz4')
        # Header
        expected_text = '{"metadata":{"version":2,"date":"Fri Oct 14 00:46:34 2016","compressed":true,"engine":"LZ4","bytecount":284383,"chunkcount":3,"chunksizes":[364,141105,142914]}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(expected_header, dtk_file.header)
        # Simulation
        with open('test-data/two-node/state-00010.sim.lz4', 'rb') as handle:
            expected_sim_chunk = handle.read()
        self.assertEqual(expected_sim_chunk, dtk_file.get_chunk(0))
        with open('test-data/two-node/state-00010.sim.json', 'rb') as handle:
            expected_sim_content = handle.read()
        self.assertEqual(expected_sim_content, dtk_file.get_contents(0))
        obj = json.loads(expected_sim_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        self.assertEqual(obj['simulation'], dtk_file.sim)
        # Node 1
        with open('test-data/two-node/state-00010.node-1.lz4', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(1))
        with open('test-data/two-node/state-00010.node-1.json', 'rb') as handle:
            expected_node_content = handle.read()
        self.assertEqual(expected_node_content, dtk_file.get_contents(1))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(1))
        # Node 2
        with open('test-data/two-node/state-00010.node-2.lz4', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(2))
        with open('test-data/two-node/state-00010.node-2.json', 'rb') as handle:
            expected_node_content = handle.read()
        self.assertEqual(expected_node_content, dtk_file.get_contents(2))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(2))
        return

    def test_reading_multinode_snappy(self):
        dtk_file = dtkFileTools.DtkFile('test-data/two-node/state-00010.dtk.snappy')
        # Header
        expected_text = '{"metadata":{"engine":"SNAPPY","author":"clorton","tool":"dtkFileTools.py","bytecount":614301,"version":2,"compressed":true,"date":"Thu Oct 13 20:32:15 2016","chunksizes":[354,304984,308963],"chunkcount":3}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(expected_header, dtk_file.header)
        # Simulation
        with open('test-data/two-node/state-00010.sim.snappy', 'rb') as handle:
            expected_sim_chunk = handle.read()
        self.assertEqual(expected_sim_chunk, dtk_file.get_chunk(0))
        with open('test-data/two-node/state-00010.sim.json', 'rb') as handle:
            expected_sim_content = handle.read()
        self.assertEqual(expected_sim_content, dtk_file.get_contents(0))
        obj = json.loads(expected_sim_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(0))
        self.assertEqual(obj['simulation'], dtk_file.sim)
        # Node 1
        with open('test-data/two-node/state-00010.node-1.snappy', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(1))
        with open('test-data/two-node/state-00010.node-1.json', 'rb') as handle:
            expected_node_content = handle.read()
        self.assertEqual(expected_node_content, dtk_file.get_contents(1))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(1))
        # Node 2
        with open('test-data/two-node/state-00010.node-2.snappy', 'rb') as handle:
            expected_node_chunk = handle.read()
        self.assertEqual(expected_node_chunk, dtk_file.get_chunk(2))
        with open('test-data/two-node/state-00010.node-2.json', 'rb') as handle:
            expected_node_content = handle.read()
        self.assertEqual(expected_node_content, dtk_file.get_contents(2))
        obj = json.loads(expected_node_content, object_hook=dtkFileTools.SerialObject)
        self.assertEqual(obj, dtk_file.get_object(2))
        return


class TestReadingSadPath(unittest.TestCase):

    def test_reading_wrong_magic_number(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/bad-magic.dtk')
        return

    def test_reading_negative_header_size(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/neg-hdr-size.dtk')
        return

    def test_reading_zero_header_size(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/zero-hdr-size.dtk')
        return

    def test_reading_invalid_header(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/bad-header.dtk')
        return

# Missing version is considered version 1. Is this okay?
#    def test_reading_missing_version(self):
#        with self.assertRaises(UserWarning):
#            dtk_file = dtkFileTools.DtkFile('test-data/missing-version.dtk')
#        return

    def test_reading_negative_version(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/neg-version.dtk')
        return

    def test_reading_zero_version(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/zero-version.dtk')
        return

    def test_reading_unknown_version(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/future-version.dtk')
        return

    def test_reading_negative_chunk_size(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/neg-chunk-size.dtk')
        return

    def test_reading_zero_chunk_size(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/zero-chunk-size.dtk')
        return

    def test_reading_truncated_file(self):
        with self.assertRaises(UserWarning):
            dtk_file = dtkFileTools.DtkFile('test-data/truncated.dtk')  # simulation and one node (truncated)
            node_one = dtk_file.get_object(1)
        return

    # Compression/data mismatch (false/LZ4)
    def test_engine_data_mismatch_a(self):
        dtk_file = dtkFileTools.DtkFile('test-data/none-lz4.dtk')       # hdr (NONE) vs. actual (LZ4)
        return

    # Compression/data mismatch (false/SNAPPY)
    def test_engine_data_mismatch_b(self):
        dtk_file = dtkFileTools.DtkFile('test-data/none-snappy.dtk')    # hdr (NONE) vs. actual (SNAPPY)
        return

    # Compression/data mismatch (true+LZ4/NONE)
    def test_engine_data_mismatch_c(self):
        dtk_file = dtkFileTools.DtkFile('test-data/lz4-none.dtk')       # hdr (LZ4) vs. actual (NONE)
        return

    # Compression/data mismatch (true+LZ4/SNAPPY)
    def test_engine_data_mismatch_d(self):
        dtk_file = dtkFileTools.DtkFile('test-data/lz4-snappy.dtk')     # hdr (LZ4) vs. actual (SNAPPY)
        return

    # Compression/data mismatch (true+SNAPPY/NONE)
    def test_engine_data_mismatch_e(self):
        dtk_file = dtkFileTools.DtkFile('test-data/snappy-none.dtk')    # hdr (SNAPPY) vs. actual (NONE)
        return

    # Compression/data mismatch (true+SNAPPY/LZ4)
    def test_engine_data_mismatch_f(self):
        dtk_file = dtkFileTools.DtkFile('test-data/snappy-lz4.dtk')     # hdr (SNAPPY) vs. actual (LZ4)
        return

    # Corrupted simulation chunk (uncompressed)
    def test_bad_sim_chunk_none(self):
        dtk_file = dtkFileTools.DtkFile('test-data/bad-sim-none.dtk')
        return

    # Corrupted simulation chunk (lz4)
    def test_bad_sim_chunk_lz4(self):
        dtk_file = dtkFileTools.DtkFile('test-data/bad-sim-lz4.dtk')
        return

    # Corrupted simulation chunk (snappy)
    def test_bad_sim_chunk_snappy(self):
        dtk_file = dtkFileTools.DtkFile('test-data/bad-sim-snappy.dtk')
        return

    # Corrupted chunk (uncompressed)
    def test_bad_chunk_none(self):
        dtk_file = dtkFileTools.DtkFile('test-data/bad-chunk-none.dtk')
        return

    # Corrupted chunk (LZ4)
    def test_bad_chunk_lz4(self):
        dtk_file = dtkFileTools.DtkFile('test-data/bad-chunk-lz4.dtk')
        return

    # Corrupted chunk (SNAPPY)
    def test_bad_chunk_snappy(self):
        dtk_file = dtkFileTools.DtkFile('test-data/bad-chunk-snappy.dtk')
        return


# ## Writing Tests

class TestWritingHappyPath(unittest.TestCase):

    # # Happy path

    # 1 Node, no compression
    def test_one_node_none(self):
        self.assertTrue(False)
        return

    # 1 Node, LZ4 compression
    def test_one_node_lz4(self):
        self.assertTrue(False)
        return

    # 1 Node, SNAPPY compression
    def test_one_node_snappy(self):
        self.assertTrue(False)
        return


    # Multi-node (4), no compression
    def test_multi_node_none(self):
        self.assertTrue(False)
        return

    # Multi-node (4), LZ4 compression
    def test_multi_node_lz4(self):
        self.assertTrue(False)
        return

    # Multi-node (4), SNAPPY compression
    def test_multi_node_snappy(self):
        self.assertTrue(False)
        return


    # # Fallbacks

    # 1 Node, LZ4 compression, payload too large (>2GB), fallback to SNAPPY
    def test_2gb_chunk(self):
        self.assertTrue(False)
        return

    # 1 Node, LZ4 compression, payload too large (>4GB), fallback to uncompressed
    def test_4gb_chunk(self):
        self.assertTrue(False)
        return


    # # Sad path
class TestWritingSadPath(unittest.TestCase):

    # Simulation file not found
    def test_simulation_not_found(self):
        self.assertTrue(False)
        return

    # Node file not found
    def test_node_not_found(self):
        self.assertTrue(False)
        return

if __name__ == '__main__':
    unittest.main()
