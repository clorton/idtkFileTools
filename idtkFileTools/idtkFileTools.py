#!/usr/bin/python

from __future__ import print_function

import argparse
import hashlib
import json
import os
import sys
import time
from collections import OrderedDict

import snappy

# clorton cdo
READ_PAYLOAD = 0
DECOMPRESS_PAYLOAD = 1
PARSE_JSON = 2
WRITE_PAYLOAD = 3
CONVERT_TO_JSON = 4
COMPRESS_JSON = 5
WRITE_JSON = 6
WRITE_DATA = 7
READ_DATA = 8

# This puts all the messages in one place so the output is aligned
_messages_ = {READ_PAYLOAD:       'Read file payload:       ',
              DECOMPRESS_PAYLOAD: 'Decompress payload:      ',
              PARSE_JSON:         'Parse JSON:              ',
              WRITE_PAYLOAD:      'Write file payload:      ',
              CONVERT_TO_JSON:    'Convert to JSON:         ',
              COMPRESS_JSON:      'Compress JSON string:    ',
              WRITE_JSON:         'Write JSON to file:      ',
              WRITE_DATA:         'Write contents to file:  ',
              READ_DATA:          'Read contents from file: '}


def timing(f, message_index):
    print('\r' + _messages_[message_index], file=sys.stderr, end='')
    t_start = time.clock()
    result = f()
    t_end = time.clock()
    print('{0:>10f}'.format(t_end-t_start), file=sys.stderr)

    return result


def read_idtk_file_components(filename):
    """
    :param filename: source data filename (DTK serialized data format)
    :return: header, payload - JSON header string and raw payload data
    """

    header = None
    payload = None

    with open(filename, 'rb') as input_handle:

        magic = input_handle.read(4)
        if magic != 'IDTK':
            raise UserWarning("Specified file '{0}' has incorrect magic 'number': '{1}'".format(filename, magic))

        size_string = input_handle.read(12)
        count = int(size_string)
        header = input_handle.read(count)

        payload = timing(lambda: input_handle.read(), message_index=READ_PAYLOAD)

    return header, payload


def read_idtk_file(filename):
    """
    :param filename: source data filename (DTK serialized data format)
    :return: header, payload, contents, data - parsed JSON header, raw payload data, decompressed (if appropriate) payload data, and parsed JSON data
    """

    header, payload = read_idtk_file_components(filename)
    header = json.loads(header, object_pairs_hook=OrderedDict)  # string isn't very useful, convert JSON to data
    contents = None

    if 'compressed' in header['metadata'] and header['metadata']['compressed']:
        contents = timing(lambda: snappy.uncompress(payload), message_index=DECOMPRESS_PAYLOAD)
    else:
        contents = payload

    data = timing(lambda: json.loads(contents, object_pairs_hook=OrderedDict), message_index=PARSE_JSON)

    return header, payload, contents, data


def write_idtk_file_components(header, payload, filename):
    """
    :param header:  dictionary of header data, should include metadata
    :param payload: raw payload data, already compressed if desired
    :param filename: filename for writing data to disk
    :return: None
    """

    header_string = json.dumps(header, indent=None, separators=(',', ':'))   # Most compact representation
    size_string = '{:>12}'.format(len(header_string))  # decimal value right aligned in 12 character space

    with open(filename, 'wb') as output_handle:

        output_handle.write('IDTK')
        output_handle.write(size_string)
        output_handle.write(header_string)
        timing(lambda: output_handle.write(payload), message_index=WRITE_PAYLOAD)

    pass


def write_idtk_file(header, data, filename, compress=True):
    """
    :param header: dictionary of header data
    :param data: dictionary of serialized data
    :param filename: filename for writing
    :param compress: compress (or don't) payload in resulting file
    :return: None
    """

    # indent=None means no newlines
    contents = timing(lambda: json.dumps(data, indent=None, separators=(',', ':')), message_index=CONVERT_TO_JSON)
    is_compressed = False
    if compress:
        if len(contents) < 0x80000000:  # Change this to 0x100000000 when python-snappy is fixed
            payload = timing(lambda: snappy.compress(contents), message_index=COMPRESS_JSON)
            is_compressed = True
        else:
            payload = contents
    else:
        payload = contents

    set_metadata(header, payload, is_compressed)

    write_idtk_file_components(header, payload, filename)

    pass


def set_metadata(header, payload, compressed):
    """
    :param header: dictionary with header information
    :param payload: file payload (used to calculate bytecount, sha1 hash, and md5 hash)
    :param compressed: is the payload compressed or not
    :return: metadata dictionary
    """

    if 'metadata' not in header:
        header['metadata'] = OrderedDict()

    metadata = header['metadata']
    metadata['version'] = 1
    metadata['date'] = time.strftime('%a %b %d %H:%M:%S %Y')    # e.g. Wed Mar 16 16:10:42 2016
    metadata['compressed'] = compressed
    metadata['bytecount'] = len(payload)

    sha1 = hashlib.sha1()
    sha1.update(payload)
    metadata['sha1'] = sha1.hexdigest()

    md5 = hashlib.md5()
    md5.update(payload)
    metadata['md5'] = md5.hexdigest()

    return metadata


def _do_read(args):

    header, payload, contents, data = read_idtk_file(args.filename)

    if args.header is not None:
        with open(args.header, 'wb') as handle:
            json.dump(header, handle, indent=4, separators=(',', ': '))

    if args.payload is not None:
        with open(args.payload, 'wb') as handle:
            timing(lambda: handle.write(payload), message_index=WRITE_PAYLOAD)

    if args.output is not None:
        output_filename = args.output
    else:
        basename, _ = os.path.splitext(args.filename)
        output_filename = basename + '.json'

    with open(output_filename, 'wb') as handle:
        if args.format:
            timing(lambda: json.dump(data, handle, indent=2, separators=(',', ': ')), message_index=WRITE_JSON)
        else:
            timing(lambda: handle.write(contents), message_index=WRITE_DATA)

    pass


def _do_write(args):

    if args.header is not None:
        with open(args.header, 'rb') as handle:
            header = json.load(handle, object_pairs_hook=OrderedDict)
    else:
        header = {}

    if 'metadata' not in header:
        header['metadata'] = {'version': 1}

    metadata = header['metadata']

    if 'author' not in metadata:
        metadata['author'] = args.author

    if 'tool' not in metadata:
        metadata['tool'] = args.tool

    with open(args.input, 'rb') as handle:
        source_data = timing(lambda: handle.read(), message_index=READ_DATA)

    if args.verify:
        data = timing(lambda: json.loads(source_data, object_pairs_hook=OrderedDict), message_index=PARSE_JSON)
        write_idtk_file(header, data, args.filename, compress=args.compress)
    else:
        set_metadata(header, source_data, args.compress)
        write_idtk_file_components(header, source_data, args.filename)

    pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Tools for reading/writing DTK serialized population files.")
    subparsers = parser.add_subparsers(help='add_subparsers help')
    read_parser = subparsers.add_parser('read', help='read help')
    read_parser.add_argument('filename')
    read_parser.add_argument('--header', default=None, help='Write header to file', metavar='<filename>')
    read_parser.add_argument('-p', '--payload', default=None, help='Write exact contents of payload to file', metavar='<filename>')
    read_parser.add_argument('-u', '--unformatted', default=True, dest='format', action='store_false', help="Don't format contents of file")
    read_parser.add_argument('-o', '--output', default=None, help='Output filename defaults to input with .json extension')
    read_parser.set_defaults(func=_do_read)

    write_parser = subparsers.add_parser('write', help='write help')
    write_parser.add_argument('input', help='Source data filename')
    write_parser.add_argument('filename', help='Output .dtk filename')
    write_parser.add_argument('--header', default=None, help='Metadata header information filename')
    write_parser.add_argument('-a', '--author', default=os.environ['USERNAME'], help='Author name for metadata [{0}]'.format(os.environ['USERNAME']))
    write_parser.add_argument('-t', '--tool', default=os.path.basename(__file__), help='Tool name for metadata [{0}]'.format(os.path.basename(__file__)))
    write_parser.add_argument('-u', '--uncompressed', default=True, action='store_false', dest='compress', help='Do not compress contents of new .idtk file [False]')
    write_parser.add_argument('-s', '--skip-verify', default=True, action='store_false', dest='verify', help='Do not verify contents are valid JSON [False]')
    write_parser.set_defaults(func=_do_write)

    command_line_args = parser.parse_args()
    command_line_args.func(command_line_args)

    pass
