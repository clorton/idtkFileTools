#!/usr/bin/python

import dtkFileTools
import json
import unittest


# Reading Tests

class TestReadingHappyPath(unittest.TestCase):

    def test_reading_version_one_uncompressed(self):
        dtk_file = dtkFileTools.DtkFile('test-data/simple.dtk')
        expected_text = '{"metadata":{"author":"clorton","tool":"notepad","compressed":false}}'
        self.assertEqual(expected_text, dtk_file.header_text)
        expected_header = json.loads(expected_text, object_hook=dtkFileTools.SerialObject)
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
        return

    def test_reading_one_node_uncompressed(self):
        return

    def test_reading_one_node_ellzeefour(self):
        return

    def test_reading_one_node_snappy(self):
        return

    def test_reading_mulinode_uncompressed(self):
        return

    def test_reading_mulinode_ellzeefour(self):
        return

    def test_reading_mulinode_snappy(self):
        return


class TestReadingSadPath(unittest.TestCase):

    def test_reading_wrong_magic_number(self):
        return

    def test_reading_negative_header_size(self):
        return

    def test_reading_zero_header_size(self):
        return

    def test_reading_invalid_header(self):
        return

    def test_reading_missing_version(self):
        return

    def test_reading_negative_version(self):
        return

    def test_reading_zero_version(self):
        return

    def test_reading_unknown_version(self):
        return

    def test_reading_negative_chunk_size(self):
        return

    def test_reading_zero_chunk_size(self):
        return

    def test_reading_truncated_file(self):
        return

# Compression/data mismatch (false/LZ4)
# Compression/data mismatch (false/SNAPPY)
# Compression/data mismatch (true+LZ4/NONE)
# Compression/data mismatch (true+LZ4/SNAPPY)
# Compression/data mismatch (true+SNAPPY/NONE)
# Compression/data mismatch (true+SNAPPY/LZ4)
# Corrupted chunk (uncompressed)
# Corrupted chunk (LZ4)
# Corrupted chunk (SNAPPY)

# ## Writing Tests

# # Happy path

# 1 Node, no compression
# 1 Node, LZ4 compression
# 1 Node, SNAPPY compression

# Multi-node (4), no compression
# Multi-node (4), LZ4 compression
# Multi-node (4), SNAPPY compression

# # Fallbacks

# 1 Node, LZ4 compression, payload too large (>2GB), fallback to SNAPPY
# 1 Node, LZ4 compression, payload too large (>4GB), fallback to uncompressed

# # Sad path

# Simulation file not found
# Node file not found

if __name__ == '__main__':
    unittest.main()
