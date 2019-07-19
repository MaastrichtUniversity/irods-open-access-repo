import hashlib
import io
import tarfile
import zipfile
import logging
from collections import OrderedDict

from tqdm import tqdm
from io import RawIOBase
from requests_toolbelt.multipart.encoder import CustomBytesIO, encode_with
from requests.utils import super_len
from xml.etree import ElementTree
import datetime

logger = logging.getLogger('iRODS to Dataverse')
BLOCK_SIZE = 1024 * io.DEFAULT_BUFFER_SIZE

date_iso = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
date = datetime.datetime.now().date()

class MultiPurposeReader:
    """
    Custom multi-part reader.
    Update the chksums while reading the buffer by chunk.
    """

    def __init__(self, buffer, length, md5, sha):
        self.len = None if length is None else int(length)
        self._raw = buffer
        self.md5 = md5
        self.sha = sha
        self.bar = tqdm(total=length, unit="bytes", smoothing=0.1, unit_scale=True)

    def read(self, chunk_size):
        force_size = 1024 * io.DEFAULT_BUFFER_SIZE

        if chunk_size == -1 or chunk_size == 0 or chunk_size > force_size:
            chunk = self._raw.read(chunk_size) or b''
        else:
            chunk = self._raw.read(force_size) or b''

        self.len -= len(chunk)

        if not chunk:
            self.len = 0

        self.md5.update(chunk)
        self.sha.update(chunk)
        self.bar.update(len(chunk))

        return chunk


class IteratorAsBinaryFile(object):
    """
    Custom bundle iterator for streaming.
    <requests_toolbelt.streaming_iterator>
    """

    def __init__(self, size, iterator, md5, encoding='utf-8'):
        #: The expected size of the upload
        self.size = int(size)
        self.md5 = md5
        if self.size < 0:
            raise ValueError(
                'The size of the upload must be a positive integer'
            )

        #: Attribute that requests will check to determine the length of the
        #: body. See bug #80 for more details
        self.len = self.size
        #: The iterator used to generate the upload data
        self.iterator = iterator

        #: Encoding the iterator is using
        self.encoding = encoding

        # The buffer we use to provide the correct number of bytes requested
        # during a read
        self._buffer = CustomBytesIO()

    def _get_bytes(self):
        try:
            return encode_with(next(self.iterator), self.encoding)
        except StopIteration:
            return b''

    def _load_bytes(self, size):
        self._buffer.smart_truncate()
        amount_to_load = size - super_len(self._buffer)
        bytes_to_append = True

        while amount_to_load > 0 and bytes_to_append:
            bytes_to_append = self._get_bytes()
            amount_to_load -= self._buffer.append(bytes_to_append)

    def read(self, size=-1):
        size = int(size)
        if size == -1:
            return b''.join(self.iterator)

        self._load_bytes(size)
        s = self._buffer.read(size)
        if not s:
            self.len = 0

        if size < 0:
            self.len = 0

        self.md5.update(encode_with(s, self.encoding))
        return encode_with(s, self.encoding)


class UnseekableStream(RawIOBase):
    """
    Custom raw buffer for streaming.
    """

    def __init__(self):
        self._buffer = b''

    def writable(self):
        return True

    def write(self, b):
        if self.closed:
            raise ValueError('Stream was closed!')
        self._buffer += b
        return len(b)

    def get(self):
        chunk = self._buffer
        self._buffer = b''
        return chunk


def archive_generator_faker(func, stream, bar, irods_md5):
    """
    Yield the raw buffer for streaming.

    :param irods_md5:
    :param func: archive buffer iterator
    :param stream: raw buffer stream
    :param bar: progress monitor
    :return:
    """

    try:
        size = 0
        # if logging.getLogger().isEnabledFor(logging.DEBUG):
        with open("debug_archive0.zip", 'wb') as out_fp:
            for i in func:
                s = stream.get()
                if len(s) > 0:
                    bar.update(len(s))
                    out_fp.write(s)
                    out_fp.flush()
                    irods_md5.update(s)
                    size += len(s)
        # else:
        #     for i in func:
        #         s = stream.get()
        #         if len(s) > 0:
        #             bar.update(len(s))
        #             irods_md5.update(s)
        #             size += len(s)
        return size
    except StopIteration:
        return b''


def archive_generator(func, stream, bar):
    """
    Yield the raw buffer for streaming.

    :param func: archive buffer iterator
    :param stream: raw buffer stream
    :param bar: progress monitor
    :return:
    """

    try:
        size = 0
        # if logging.getLogger().isEnabledFor(logging.DEBUG):
        with open("debug_archive00.zip", 'wb') as out_fp:
            for i in func:
                s = stream.get()
                if len(s) > 0:
                    bar.update(len(s))
                    out_fp.write(s)
                    out_fp.flush()
                    size += len(s)
                    yield s
        print("post size " + str(size))
        # else:
        #     for i in func:
        #         s = stream.get()
        #         if len(s) > 0:
        #             bar.update(len(s))
        #             yield s
    except StopIteration:
        return b''


def sort_path_by_size(collection):
    sort = {}
    for coll, sub, files in collection.walk():
        for file in files:
            sort.update({file.size: file})

    return OrderedDict(sorted(sort.items(), reverse=True, key=lambda t: t[0]))


def calc_zip_block(size, file_path, zip64):
    """
    Calculate the zip member block size.

    :param int size: file size
    :param str file_path: file path
    :param boolean zip64: Flag for zip64 format, if True add extra header
    :return: int - zip member block size
    """

    # File size
    zip_size = size
    if size >= zipfile.ZIP64_LIMIT:
        # Local file header  30 + Length filename + Data descriptor 24 + ZIP64 extra_data 20
        zip_size += 30 + len(file_path) + 24 + 20
        # Central directory file header 46 + Length filename  + ZIP64 extra_data 20
        zip_size += 46 + len(file_path)
        if zip64:
            # Central directory ZIP64 28
            zip_size += 28
        else:
            # First Central directory ZIP64  20
            zip_size += 20

    else:
        # Local file header  30 + Length filename + Data descriptor ZIP64 - 24 + ZIP64 extra_data - 20
        zip_size += 30 + len(file_path) + 24 + 20
        # Central directory file header 46 + Length filename
        zip_size += 46 + len(file_path)
        if zip64:
            # Central directory ZIP64 - 12
            zip_size += 12
        # else:
        #     # Data descriptor - 16
        #     zip_size += 16

    return zip_size


def collection_zip_preparation(collection, rulemanager, upload_success):
    """
    Walk through the collection. Request iRODS file chksums.
    Return a list of all the file's path and the estimated zip size.

    :param <irods.manager.collection_manager.CollectionManager> collection: iRODS collection to evaluate
    :param <irodsManager.irodsRuleManager.RuleManager> rulemanager: RuleManager to call chksums rule
    :param dict upload_success: {file_path: hash_key}
    :return: (list, int)
    """

    data = []
    size = 0
    sorted_path = sort_path_by_size(collection)
    zip64 = False
    for file_size, file in sorted_path.items():
        data.append(file)
        size += calc_zip_block(file_size, file.path, zip64)

        if size >= zipfile.ZIP64_LIMIT:
            zip64 = True

        irods_hash_decode = rulemanager.rule_checksum(file.path)
        logger.info(f"{'--':<30}iRODS {file.name} SHA-256: {irods_hash_decode}")
        upload_success.update({file.path: irods_hash_decode})

    # End of central directory record (EOCD) 22
    size += 22
    if zip64:
        # Zip64 end of central directory record 56 & locator 20
        size += 56 + 20
    return data, size


def zip_collection(data, stream, session, upload_success):
    """
    Create a generator to zip the collection.
    Also request the iRODS sha256 chksums and compare it to the buffer.

    :param list data: list of files path
    :param UnseekableStream stream: raw buffer stream
    :param <irods.session.iRODSSession> session: Open iRODS session to the server
    :param dict upload_success: {file_path: hash_key}
    """

    zip_buffer = zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED)
    yield
    for f in data:
        buff = session.data_objects.open(f.path, 'r')

        zip_info = zipfile.ZipInfo(f.path)
        zip_info.file_size = f.size
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        irods_sha = hashlib.sha256()
        with zip_buffer.open(zip_info, mode='w', force_zip64=True) as dest:
            for chunk in iter(lambda: buff.read(BLOCK_SIZE), b''):
                dest.write(chunk)
                irods_sha.update(chunk)
                yield
        buff.close()

        sha_hexdigest = irods_sha.hexdigest()
        logger.info(f"{'--':<30}buffer {f.name} SHA: {sha_hexdigest}")
        if upload_success.get(f.path) == sha_hexdigest:
            logger.info(f"{'--':<30}SHA-256 {f.name}  match: True")
            upload_success.update({f.path: True})

    zip_buffer.close()
    yield


def get_zip_generator(collection, session, upload_success, rulemanager, irods_md5, size) -> IteratorAsBinaryFile:
    """
    Bundle an iRODS collection into an uncompressed zip buffer.
    Return the zip buffer iterator.

    :param <irods.manager.collection_manager.CollectionManager> collection: iRODS collection to zip
    :param <irods.session.iRODSSession> session:  Open iRODS session to the server
    :param dict upload_success: {file_path: hash_key}
    :param <irodsManager.irodsRuleManager.RuleManager> rulemanager: RuleManager to call chksums rule
    :param irods_md5: hashlib.md5()
    :param size:
    :return: zip buffer iterator
    """

    stream = UnseekableStream()
    bar = tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True)
    data, fake_size = collection_zip_preparation(collection, rulemanager, upload_success)
    zip_iterator = zip_collection(data, stream, session, upload_success)
    iterator = IteratorAsBinaryFile(size, archive_generator(zip_iterator, stream, bar), irods_md5)

    return iterator


def zip_generator_faker(collection, session, upload_success, rulemanager, irods_md5) -> int:
    """
    Bundle an iRODS collection into an uncompressed zip buffer.
    Return the zip buffer iterator.

    :param <irods.manager.collection_manager.CollectionManager> collection: iRODS collection to zip
    :param <irods.session.iRODSSession> session:  Open iRODS session to the server
    :param dict upload_success: {file_path: hash_key}
    :param <irodsManager.irodsRuleManager.RuleManager> rulemanager: RuleManager to call chksums rule
    :param irods_md5: hashlib.md5()
    :return: zip buffer iterator
    """

    data, size = collection_zip_preparation(collection, rulemanager, upload_success)
    logger.info(f"{'--':<10} bundle predicted uncompressed size: {size}")
    stream = UnseekableStream()
    zip_iterator = zip_collection(data, stream, session, upload_success)
    bar = tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True)
    size_bundle = archive_generator_faker(zip_iterator, stream, bar, irods_md5)

    return size_bundle


def bag_collection(data, stream, session, upload_success, imetadata, collection):
    """
    Create a generator to zip the collection.
    Also request the iRODS sha256 chksums and compare it to the buffer.

    :param list data: list of files path
    :param UnseekableStream stream: raw buffer stream
    :param <irods.session.iRODSSession> session: Open iRODS session to the server
    :param dict upload_success: {file_path: hash_key}
    """

    zip_buffer = zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED)
    yield
    checksums = []
    total_bytes = 0
    total_files = 0
    for f in data:
        total_files += 1
        buff = session.data_objects.open(f.path, 'r')
        arc_name = f.path.replace(collection.path, f'{collection.name}/data')
        zip_info = zipfile.ZipInfo(arc_name)
        zip_info.file_size = f.size
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        irods_sha256 = hashlib.sha256()
        irods_sha1 = hashlib.sha1()
        with zip_buffer.open(zip_info, mode='w') as dest:
            for chunk in iter(lambda: buff.read(BLOCK_SIZE), b''):
                dest.write(chunk,)
                irods_sha256.update(chunk)
                irods_sha1.update(chunk)
                total_bytes += len(chunk)
                yield
        buff.close()

        sha256_hexdigest = irods_sha256.hexdigest()
        sha1_hexdigest = irods_sha1.hexdigest()
        print(f"{'--':<30}buffer {f.name} SHA-256: {sha256_hexdigest}")
        print(f"{'--':<30}buffer {f.name} SHA-1: {sha1_hexdigest}")
        if upload_success.get(f.path) == sha256_hexdigest:
            print(f"{'--':<30}SHA-256 {f.name}  match: True")
            upload_success.update({f.path: True})

            checksums.append((arc_name.replace(f'{collection.name}/data', 'data'), sha1_hexdigest))
    yield

    tagmanifest = []
    checksums = sorted(checksums)
    manifest_name = f"{collection.name}/manifest-sha1.txt"
    zip_info = zipfile.ZipInfo(manifest_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        md5 = hashlib.md5()
        for filename, digest in checksums:
            line = "%s  %s\n" % (digest, filename)
            line = line.encode('utf-8')
            m_size += len(line)
            md5.update(line)
            manifest.write(line)
        tagmanifest.append((md5.hexdigest(), manifest_name))
    yield

    bagit_name = f"{collection.name}/bagit.txt"
    zip_info = zipfile.ZipInfo(bagit_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        md5 = hashlib.md5()
        line = """BagIt-Version: 0.97
Tag-File-Character-Encoding: UTF-8
"""
        line = line.encode('utf-8')
        m_size += len(line)
        md5.update(line)
        manifest.write(line)
        tagmanifest.append((md5.hexdigest(), bagit_name))
    yield

    oxum = "%s.%s" % (total_bytes, total_files)
    baginfo_name = f"{collection.name}/bag-info.txt"
    zip_info = zipfile.ZipInfo(baginfo_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        md5 = hashlib.md5()
        line = f"""Bagging-Date: {date}
Created: {date_iso}
Payload-Oxum: {oxum}
"""
        line = line.encode('utf-8')
        m_size += len(line)
        md5.update(line)
        manifest.write(line)
        tagmanifest.append((md5.hexdigest(), baginfo_name))
    yield

    with open('resources/dataset.xml') as f:
        name = f"{collection.name}/metadata/dataset.xml"

        tree = ElementTree.parse(f)
        ElementTree.register_namespace('dcterms', "http://purl.org/dc/terms/")
        ElementTree.register_namespace('dc', "http://purl.org/dc/elements/1.1/")
        ElementTree.register_namespace('dcx-dai', "http://easy.dans.knaw.nl/schemas/dcx/dai/")
        ElementTree.register_namespace('ddm', "http://easy.dans.knaw.nl/schemas/md/ddm/")
        ElementTree.register_namespace('xsi', "http://www.w3.org/2001/XMLSchema-instance")
        ElementTree.register_namespace('id-type', "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/")

        root = tree.getroot()

        for title in root.iter('{http://purl.org/dc/elements/1.1/}title'):
            title.text = f"{title.text}: {imetadata.title}"

        for description in root.iter('{http://purl.org/dc/terms/}description'):
            description.text = f"{description.text}: {imetadata.description}"

        doc = ElementTree.tostring(root)

        zip_info = zipfile.ZipInfo(name)
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        m_size = 0
        with zip_buffer.open(zip_info, mode='w') as manifest:
            md5 = hashlib.md5()
            line = doc
            m_size += len(line)
            md5.update(line)
            manifest.write(line)
            tagmanifest.append((md5.hexdigest(), name))
    yield

    with open('resources/files.xml') as f:
        tree = ElementTree.parse(f)
        ElementTree.register_namespace('dcterms', "http://purl.org/dc/terms/")

        root = tree.getroot()
        file = root.find('file')

        for fmt in file.iter('{http://purl.org/dc/terms/}format'):
            pass

        for filename, digest in checksums:
            if filename != "data/metadata.xml":
                new_file = ElementTree.SubElement(root, file.tag)
                new_file.attrib = {"filepath": filename.replace(f'{collection.name}/data', 'data')}
                new_format = ElementTree.SubElement(new_file, fmt.tag)
                new_format.text = "text/plain"

        doc = ElementTree.tostring(root) + "\n\n".encode('utf-8')

        name = f"{collection.name}/metadata/files.xml"
        zip_info = zipfile.ZipInfo(name)
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        m_size = 0
        with zip_buffer.open(zip_info, mode='w') as manifest:
            md5 = hashlib.md5()
            # header
            line = '<?xml version="1.0" encoding="UTF-8"?>\n'
            line = line.encode('utf-8')
            m_size += len(line)
            md5.update(line)
            manifest.write(line)
            # data
            line = doc
            m_size += len(line)
            md5.update(line)
            manifest.write(line)

            tagmanifest.append((md5.hexdigest(), name))
    yield

    tagmanifest_name = f"{collection.name}/tagmanifest-md5.txt"
    zip_info = zipfile.ZipInfo(tagmanifest_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        for digest, filename in tagmanifest:
            line = "%s  %s\n" % (digest, filename.replace(f'{collection.name}/', ''))
            line = line.encode('utf-8')
            m_size += len(line)
            manifest.write(line)
    yield
    zip_buffer.close()
    yield


def bag_generator_faker(collection, session, upload_success, rulemanager, irods_md5, imetadata):
    """
    Bundle an iRODS collection into an uncompressed zip buffer.
    Return the zip buffer iterator.

    :param <irods.manager.collection_manager.CollectionManager> collection: iRODS collection to zip
    :param <irods.session.iRODSSession> session:  Open iRODS session to the server
    :param dict upload_success: {file_path: hash_key}
    :param <irodsManager.irodsRuleManager.RuleManager> rulemanager: RuleManager to call chksums rule
    :param irods_md5: hashlib.md5()
    :return: zip buffer iterator
    """

    data, size = collection_zip_preparation(collection, rulemanager, upload_success)
    stream = UnseekableStream()
    zip_iterator = bag_collection(data, stream, session, upload_success, imetadata, collection)

    bar = tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True)
    size_bundle = archive_generator_faker(zip_iterator, stream, bar, irods_md5)

    return size_bundle


def get_bag_generator(collection, session, upload_success, rulemanager, irods_md5, imetadata, size) -> IteratorAsBinaryFile:
    """
    Bundle an iRODS collection into an uncompressed zip buffer.
    Return the zip buffer iterator.

    :param <irods.manager.collection_manager.CollectionManager> collection: iRODS collection to zip
    :param <irods.session.iRODSSession> session:  Open iRODS session to the server
    :param dict upload_success: {file_path: hash_key}
    :param <irodsManager.irodsRuleManager.RuleManager> rulemanager: RuleManager to call chksums rule
    :param irods_md5: hashlib.md5()
    :return: zip buffer iterator
    """

    data, fake_size = collection_zip_preparation(collection, rulemanager, upload_success)
    print(f"{'--':<10} bundle predicted size: {size}")
    stream = UnseekableStream()
    zip_iterator = bag_collection(data, stream, session, upload_success, imetadata, collection)
    bar = tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True)
    iterator = IteratorAsBinaryFile(size, archive_generator(zip_iterator, stream, bar), irods_md5)

    return iterator


def calc_tar_block(nb):
    if nb < 512:
        return 512
    remainder = divmod(nb, 512)[1]
    if remainder == 0:
        return nb
    elif remainder > 0:
        return nb + 512


# https://gist.github.com/chipx86/9598b1e4a9a1a7831054
def stream_build_tar(tar_name, collection, data, stream, session, upload_success):
    tar = tarfile.TarFile.open(tar_name, 'w|', stream)
    yield

    for f in data:
        filepath = f.path.replace(collection.path, '')
        tar_info = tarfile.TarInfo(filepath)

        tar_info.size = f.size
        tar.addfile(tar_info)

        buff = session.data_objects.open(f.path, 'r')

        irods_sha = hashlib.sha256()

        while True:
            s = buff.read(BLOCK_SIZE)
            if len(s) > 0:
                tar.fileobj.write(s)
                irods_sha.update(s)
                yield

            if len(s) < BLOCK_SIZE:
                blocks, remainder = divmod(tar_info.size, tarfile.BLOCKSIZE)

                if remainder > 0:
                    tar.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                    yield
                    blocks += 1

                tar.offset += blocks * tarfile.BLOCKSIZE
                break
        buff.close()

        sha_hexdigest = irods_sha.hexdigest()
        # logger.info(f"{'--':<30}buffer {f.name} SHA: {sha_hexdigest}")
        if upload_success.get(f.path) == sha_hexdigest:
            # logger.info(f"{'--':<30}SHA-256 {f.name}  match: True")
            upload_success.update({f.path: True})

    tar.close()
    yield
