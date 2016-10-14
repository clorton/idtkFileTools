#!/usr/bin/python

from __future__ import print_function
import argparse
import json
import lz4
import os
import snappy
import time
import sys


__engines__ = {'LZ4': lz4, 'SNAPPY': snappy, 'NONE': None}


class _Class:
    def __init__(self, dictionary):
        self.__dict__ = dictionary
        return


class SerialObject(dict):
    # noinspection PyDefaultArgument
    def __init__(self, dictionary={}):
        super(SerialObject, self).__init__(dictionary)
        self.__dict__ = self
        pass

    pass


class DtkNodes:

    def __init__(self, dtk_file):
        self._dtk_file = dtk_file

        return

    def __getitem__(self, index):
        node = self._dtk_file.get_object(index + 1).node

        return node


class DtkFile:

    def __init__(self, filename):
        self.filename = filename
        with open(self.filename, 'rb') as handle:
            _check_magic(handle)
            self.header_text, self.header = _read_header(handle)

        self.scheme = self.header.metadata.engine.upper()
        if self.scheme not in __engines__:
            raise UserWarning("File's compression engine ('{0}') is unknown.".format(self.header.metadata.engine))
        self.engine = __engines__[self.scheme]

        self.chunk_info = []
        offset = 4 + 12 + len(self.header_text)  # 'IDTK' + size + header
        for size in self.header.metadata.chunksizes:
            self.chunk_info.append(_Class({'offset': offset, 'size': size}))
            offset += size

        self.nodes = DtkNodes(self)

        return

    @property
    def chunk_count(self):

        return len(self.chunk_info)

    @property
    def node_count(self):

        return self.chunk_count - 1

    def get_chunk(self, index):
        with open(self.filename, 'rb') as handle:
            handle.seek(self.chunk_info[index].offset)
            chunk = handle.read(self.chunk_info[index].size)

        return chunk

    def get_contents(self, index):
        contents = self.get_chunk(index)
        if self.engine:
            try:
                contents = self.engine.decompress(contents)
            except ValueError as err:
                raise UserWarning("Couldn't decompress chunk - '{0}'".format(err))

        return contents

    def get_object(self, index):
        contents = self.get_contents(index)
        obj = json.loads(contents, object_hook=SerialObject)

        return obj

    @property
    def sim(self):
        sim = self.get_object(0).simulation

        return sim


def _check_magic(handle):
    magic = handle.read(4)
    if magic != 'IDTK':
        raise UserWarning("File has incorrect magic 'number': '{0}'".format(magic))

    return


def _read_header(handle):
    size_string = handle.read(12)
    header_size = int(size_string)
    _check_header_size(header_size)
    header_text = handle.read(header_size)
    header = _try_parse_header_text(header_text)

    metadata = header.metadata

    if not 'version' in metadata:
        metadata.version = 1
    if metadata.version < 2:
        metadata.engine = 'SNAPPY' if metadata.compressed else 'NONE'
        metadata.chunkcount = 1
        metadata.chunksizes = [metadata.bytecount]
    _check_version(metadata.version)

    _check_chunk_sizes(metadata.chunksizes)

    return header_text, header


def _check_header_size(header_size):
    if header_size <= 0:
        raise UserWarning("Invalid header size: {0}".format(header_size))

    return


def _try_parse_header_text(header_text):
    try:
        header = json.loads(header_text, object_hook=SerialObject)
    except ValueError as err:
        raise UserWarning("Couldn't decode JSON header '{0}'".format(err))

    return header


def _check_version(version):
    if version <= 0 or version > 2:
        raise UserWarning("Unknown version: {0}".format(version))

    return


def _check_chunk_sizes(chunk_sizes):

    for size in chunk_sizes:
        if size <= 0:
            raise UserWarning("Invalid chunk size: {0}".format(size))

    return


def __do_read__(commandline_arguments):
    if commandline_arguments.output is not None:
        prefix = commandline_arguments.output
    else:
        root, _ = os.path.splitext(commandline_arguments.filename)
        prefix = root

    if commandline_arguments.raw:
        extension = 'bin'
    else:
        extension = 'json'

    dtk_file = DtkFile(commandline_arguments.filename)

    if commandline_arguments.header:
        with open(commandline_arguments.header, 'wb') as handle:
            json.dump(dtk_file.header, handle, indent=2, separators=(',', ':'))

    print('File metadata: {0}'.format(dtk_file.header.metadata))

    for index in range(dtk_file.chunk_count):
        if commandline_arguments.raw:
            # Write raw chunks to disk
            output = dtk_file.get_chunk(index)
        else:
            if commandline_arguments.unformatted:
                # Expand compressed contents, but don't serialize and format
                output = dtk_file.get_contents(index)
            else:
                # Expand compressed contents, serialize, write out formatted
                obj = dtk_file.get_object(index)
                output = json.dumps(obj, indent=2, separators=(',', ':'))

        if index == 0:
            output_filename = '.'.join([prefix, 'sim', extension])
        else:
            output_filename = '.'.join([prefix, 'node-{0}'.format(index), extension])

        with open(output_filename, 'wb') as handle:
            handle.write(output)

    return


def __do_write__(args):

    print("Writing file '{0}'".format(args.filename), file=sys.stderr)
    print("Reading simulation data from '{0}'".format(args.simulation), file=sys.stderr)
    print("Reading node data from {0}".format(args.nodes), file=sys.stderr)
    print("Author = {0}".format(args.author), file=sys.stderr)
    print("Tool = {0}".format(args.tool), file=sys.stderr)
    print("{0} contents".format("Compressing" if args.compress else "Not compressing"), file=sys.stderr)
    print("{0} contents".format("Verifying" if args.verify else "Not verifying"), file=sys.stderr)
    print("Using compression engine '{0}'".format(args.engine), file=sys.stderr)

    scheme = args.engine.upper()
    engine = __engines__[scheme]

    chunks = []
    # PrepareSimulationData(sim, writers, json_texts, json_sizes);
    _prepare_simulation_data(args.simulation, args.compress, engine, chunks)
    # PrepareNodeData(sim->nodes, writers, json_texts, json_sizes);
    _prepare_node_data(args.nodes, args.compress, engine, chunks)

    # ConstructHeader(scheme, scheme_name, total, chunk_sizes, header);
    header = _construct_header(args.author, args.tool, args.engine, chunks)
    header_string = json.dumps(header, indent=None, separators=(',', ':'))

    with open(args.filename, 'wb') as handle:
        _write_magic_number(handle)
        _write_header_size(len(header_string), handle)
        _write_header(header_string, handle)
        _write_chunks(chunks, handle)

    return


def _prepare_simulation_data(filename, compress, engine, chunks):
    with open(filename, 'rb') as handle:
        data = handle.read()
    _prepare_chunk(data, compress, engine, chunks)

    return


def _prepare_chunk(data, compress, engine, chunks):
    if compress and engine is not None:
        data = engine.compress(data)
    chunks.append(data)

    return


def _prepare_node_data(filenames, compress, engine, chunks):
    for filename in filenames:
        with open(filename, 'rb') as handle:
            data = handle.read()
        _prepare_chunk(data, compress, engine, chunks)

    return


def _construct_header(author, tool, engine, chunks):
    header = SerialObject({})
    metadata = header.metadata = SerialObject({})
    metadata.version = 2
    metadata.date = time.strftime('%a %b %d %H:%M:%S %Y')
    metadata.author = author if author is not None else "unknown"
    metadata.tool = tool if tool is not None else "unknown"
    metadata.compressed = True if engine in __engines__ and __engines__[engine] is not None else False
    metadata.engine = engine if metadata.compressed else "NONE"
    metadata.bytecount = reduce(lambda acc, chunk: acc + len(chunk), chunks, 0)
    metadata.chunkcount = len(chunks)
    metadata.chunksizes = [len(c) for c in chunks]

    return header


def _write_magic_number(handle):
    handle.write('IDTK')

    return


def _write_header_size(size, handle):
    size_string = '{:>12}'.format(size)     # decimal value right aligned in 12 character space
    handle.write(size_string)

    return


def _write_header(string, handle):
    handle.write(string)

    return


def _write_chunks(chunks, handle):
    for chunk in chunks:
        handle.write(chunk)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='add_subparsers help')

    read_parser = subparsers.add_parser('read', help='read help')
    read_parser.add_argument('filename')
    read_parser.add_argument('--header', default=None, help='Write header to file', metavar='<filename>')
    read_parser.add_argument('-r', '--raw', default=False, action='store_true', help='Write raw contents of chunks to disk')
    read_parser.add_argument('-u', '--unformatted', default=False, action='store_true', help='Write unformatted (compact) JSON to disk')
    read_parser.add_argument('-o', '--output', default=None, help='Output filename prefix, defaults to input filename with .json extension')
    read_parser.set_defaults(func=__do_read__)

    username = os.environ['USERNAME']
    tool_name = os.path.basename(__file__)

    write_parser = subparsers.add_parser('write', help='write help')
    write_parser.add_argument('filename', help='Output .dtk filename')
    write_parser.add_argument('simulation', help='Filename for simulation JSON')
    write_parser.add_argument('nodes', nargs='+', help='Filename(s) for node JSON')
    write_parser.add_argument('-a', '--author', default=username, help='Author name for metadata [{0}]'.format(username))
    write_parser.add_argument('-t', '--tool', default=tool_name, help='Tool name for metadata [{0}]'.format(tool_name))
    write_parser.add_argument('-u', '--uncompressed', default=True, action='store_false', dest='compress', help='Do not compress contents of new .dtk file')
    write_parser.add_argument('-v', '--verify', default=False, action='store_true', help='Verify JSON in simulation and nodes.')
    write_parser.add_argument('-e', '--engine', default='LZ4', help='Compression engine {NONE|LZ4|SNAPPY} [LZ4]')
    write_parser.set_defaults(func=__do_write__)

    commandline_args = parser.parse_args()
    commandline_args.func(commandline_args)
