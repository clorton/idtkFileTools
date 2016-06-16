#!/usr/bin/python

import collections
import idtkFileTools
import json
import os
import snappy
import tempfile
import unittest


class TestReadingComponents(unittest.TestCase):

    def test_reading_uncompressed_file_components(self):
        header, payload = idtkFileTools.read_idtk_file_components('test/simple.dtk')
        self.assertEqual('{"metadata":{"author":"clorton","tool":"notepad","compressed":false}}', header)
        self.assertEqual('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}', payload)
        pass


class TestReadingFile(unittest.TestCase):

    def test_reading_uncompressed_file(self):
        header, payload, contents, data = idtkFileTools.read_idtk_file('test/simple.dtk')
        self.assertEqual({"metadata": {"author": "clorton", "tool": "notepad", "compressed": False}}, header)
        self.assertEqual('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}', payload)
        self.assertEqual('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}', contents)
        self.assertEqual({"simulation": {"__class__": "SimulationPython", "serializationMask": 0}}, data)
        pass

    def test_reading_compressed_file(self):
        header, payload, contents, data = idtkFileTools.read_idtk_file('test/compressed.dtk')
        self.assertEqual({"metadata": {"author": "clorton", "tool": "notepad", "compressed": True, "version": 1, "date": "Fri Mar 18 15:59:18 2016", "bytecount": 65, "sha1": "91f040868a34a597a7ba2da33eb6d794d0b99af5", "md5": "cd2c83a78544bf11159632599f7c4cf4"}}, header)
        expected = snappy.compress('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}')
        self.assertEqual(expected, payload)
        self.assertEqual('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}', contents)
        self.assertEqual({"simulation": {"__class__": "SimulationPython", "serializationMask": 0}}, data)
        pass


class TestWritingComponents(unittest.TestCase):

    def test_writing_uncompressed_file_components(self):
        header = json.loads('{"metadata":{"author":"clorton","tool":"editor","compressed":false}}', object_pairs_hook=collections.OrderedDict)
        payload = '{"simulation":{"__class__":"SimulationPython","serializationMask":0}}'
        temp_handle, temp_filename = tempfile.mkstemp()
        os.close(temp_handle)
        idtkFileTools.write_idtk_file_components(header, payload, temp_filename)
        with open(temp_filename, 'rb') as handle:
            actual = handle.read()
        with open('test/expected.dtk', 'rb') as handle:
            expected = handle.read()
        os.remove(temp_filename)
        self.assertEqual(expected, actual)
        pass


class TestWritingFile(unittest.TestCase):

    def test_writing_uncompressed_file(self):
        source_header = json.loads('{"metadata":{"author":"clorton","tool":"editor","compressed":false}}', object_pairs_hook=collections.OrderedDict)
        source_data = json.loads('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}', object_pairs_hook=collections.OrderedDict)
        temp_handle, temp_filename = tempfile.mkstemp()
        os.close(temp_handle)
        idtkFileTools.write_idtk_file(source_header, source_data, temp_filename, compress=False)
        # write_idtk_file adds the date and time, sha1, and md5 hashes to the header so we can't do a strict compare
        header, payload = idtkFileTools.read_idtk_file_components(temp_filename)
        os.remove(temp_filename)
        actual_header = json.loads(header)
        self.assertEqual(source_header['metadata']['author'], actual_header['metadata']['author'])
        self.assertEqual(source_header['metadata']['tool'], actual_header['metadata']['tool'])
        self.assertFalse(actual_header['metadata']['compressed'])
        actual_data = json.loads(payload)
        self.assertEqual(source_data, actual_data)
        pass

    def test_writing_compressed_file(self):
        source_header = json.loads('{"metadata":{"author":"clorton","tool":"editor","compressed":false}}', object_pairs_hook=collections.OrderedDict)
        source_data = json.loads('{"simulation":{"__class__":"SimulationPython","serializationMask":0}}', object_pairs_hook=collections.OrderedDict)
        temp_handle, temp_filename = tempfile.mkstemp()
        os.close(temp_handle)
        idtkFileTools.write_idtk_file(source_header, source_data, temp_filename, compress=True)
        # write_idtk_file adds the date and time, sha1, and md5 hashes to the header so we can't do a strict compare
        header, payload = idtkFileTools.read_idtk_file_components(temp_filename)
        os.remove(temp_filename)
        actual_header = json.loads(header)
        self.assertEqual(source_header['metadata']['author'], actual_header['metadata']['author'])
        self.assertEqual(source_header['metadata']['tool'], actual_header['metadata']['tool'])
        self.assertTrue(actual_header['metadata']['compressed'])
        actual_data = json.loads(snappy.uncompress(payload))
        self.assertEqual(source_data, actual_data)
        pass


if __name__ == '__main__':
    unittest.main()