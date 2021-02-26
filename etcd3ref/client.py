import functools
import inspect
import threading

import grpc
import grpc._channel
from grpc._channel import _InactiveRpcError
from six.moves import queue

import etcd3ref.etcdrpc as etcdrpc
import etcd3ref.exceptions as exceptions
import etcd3ref.leases as leases
import etcd3ref.locks as locks
import etcd3ref.members
import etcd3ref.transactions as transactions
import etcd3ref.utils as utils
import etcd3ref.watch as watch

_EXCEPTIONS_BY_CODE = {
    grpc.StatusCode.INTERNAL: exceptions.InternalServerError,
    grpc.StatusCode.UNAVAILABLE: exceptions.ConnectionFailedError,
    grpc.StatusCode.DEADLINE_EXCEEDED: exceptions.ConnectionTimeoutError,
    grpc.StatusCode.FAILED_PRECONDITION: exceptions.PreconditionFailedError,
}


def _translate_exception(exc):
    code = exc.code()
    exception = _EXCEPTIONS_BY_CODE.get(code)
    if exception is None:
        raise
    raise exception


def _handle_errors(f):
    if inspect.isgeneratorfunction(f):
        def handler(*args, **kwargs):
            try:
                for data in f(*args, **kwargs):
                    yield data
            except grpc.RpcError as exc:
                _translate_exception(exc)
    else:
        def handler(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except grpc.RpcError as exc:
                _translate_exception(exc)

    return functools.wraps(f)(handler)


class Transactions(object):
    def __init__(self):
        self.value = transactions.Value
        self.version = transactions.Version
        self.create = transactions.Create
        self.mod = transactions.Mod

        self.put = transactions.Put
        self.get = transactions.Get
        self.delete = transactions.Delete
        self.txn = transactions.Txn


class KVMetadata(object):
    def __init__(self, keyvalue, header):
        self.key = keyvalue.key
        self.create_revision = keyvalue.create_revision
        self.mod_revision = keyvalue.mod_revision
        self.version = keyvalue.version
        self.lease_id = keyvalue.lease
        self.response_header = header


class Status(object):
    def __init__(self, version, db_size, leader, raft_index, raft_term):
        self.version = version
        self.db_size = db_size
        self.leader = leader
        self.raft_index = raft_index
        self.raft_term = raft_term


class Alarm(object):
    def __init__(self, alarm_type, member_id):
        self.alarm_type = alarm_type
        self.member_id = member_id


class EtcdTokenCallCredentials(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, access_token):
        self._access_token = access_token

    def __call__(self, context, callback):
        metadata = (('token', self._access_token),)
        callback(metadata, None)


def _get_secure_cred(ca_cert, cert_key=None, cert_cert=None):
    cert_key_file = None
    cert_cert_file = None

    with open(ca_cert, 'rb') as f:
        ca_cert_file = f.read()

    if cert_key is not None:
        with open(cert_key, 'rb') as f:
            cert_key_file = f.read()

    if cert_cert is not None:
        with open(cert_cert, 'rb') as f:
            cert_cert_file = f.read()

    return grpc.ssl_channel_credentials(
        ca_cert_file,
        cert_key_file,
        cert_cert_file
    )


def _build_get_range_request(key,
                             range_end=None,
                             limit=None,
                             revision=None,
                             sort_order=None,
                             sort_target='key',
                             serializable=False,
                             keys_only=False,
                             count_only=None,
                             min_mod_revision=None,
                             max_mod_revision=None,
                             min_create_revision=None,
                             max_create_revision=None):
    range_request = etcdrpc.RangeRequest()
    range_request.key = utils.to_bytes(key)
    range_request.keys_only = keys_only
    if range_end is not None:
        range_request.range_end = utils.to_bytes(range_end)

    if sort_order is None:
        range_request.sort_order = etcdrpc.RangeRequest.NONE
    elif sort_order == 'ascend':
        range_request.sort_order = etcdrpc.RangeRequest.ASCEND
    elif sort_order == 'descend':
        range_request.sort_order = etcdrpc.RangeRequest.DESCEND
    else:
        raise ValueError('unknown sort order: "{}"'.format(sort_order))

    if sort_target is None or sort_target == 'key':
        range_request.sort_target = etcdrpc.RangeRequest.KEY
    elif sort_target == 'version':
        range_request.sort_target = etcdrpc.RangeRequest.VERSION
    elif sort_target == 'create':
        range_request.sort_target = etcdrpc.RangeRequest.CREATE
    elif sort_target == 'mod':
        range_request.sort_target = etcdrpc.RangeRequest.MOD
    elif sort_target == 'value':
        range_request.sort_target = etcdrpc.RangeRequest.VALUE
    else:
        raise ValueError('sort_target must be one of "key", '
                         '"version", "create", "mod" or "value"')

    range_request.serializable = serializable

    return range_request


def _build_put_request(key, value, lease=None, prev_kv=False):
    put_request = etcdrpc.PutRequest()
    put_request.key = utils.to_bytes(key)
    put_request.value = utils.to_bytes(value)
    put_request.lease = utils.lease_to_id(lease)
    put_request.prev_kv = prev_kv

    return put_request


def _build_delete_request(key,
                          range_end=None,
                          prev_kv=False):
    delete_request = etcdrpc.DeleteRangeRequest()
    delete_request.key = utils.to_bytes(key)
    delete_request.prev_kv = prev_kv

    if range_end is not None:
        delete_request.range_end = utils.to_bytes(range_end)

    return delete_request


def _build_alarm_request(alarm_action, member_id, alarm_type):
    alarm_request = etcdrpc.AlarmRequest()

    if alarm_action == 'get':
        alarm_request.action = etcdrpc.AlarmRequest.GET
    elif alarm_action == 'activate':
        alarm_request.action = etcdrpc.AlarmRequest.ACTIVATE
    elif alarm_action == 'deactivate':
        alarm_request.action = etcdrpc.AlarmRequest.DEACTIVATE
    else:
        raise ValueError('Unknown alarm action: {}'.format(alarm_action))

    alarm_request.memberID = member_id

    if alarm_type == 'none':
        alarm_request.alarm = etcdrpc.NONE
    elif alarm_type == 'no space':
        alarm_request.alarm = etcdrpc.NOSPACE
    else:
        raise ValueError('Unknown alarm type: {}'.format(alarm_type))

    return alarm_request


class Etcd3Client(object):
    def __init__(self, host='localhost', port=2379,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
                 user=None, password=None, grpc_options=None):

        self._url = '{host}:{port}'.format(host=host, port=port)
        self.metadata = None

        # use by reconnect
        self.ca_cert = ca_cert
        self.cert_key = cert_key
        self.cert_cert = cert_cert
        self.timeout = timeout
        self.user = user
        self.password = password
        self.grpc_options = grpc_options

        self.__connect(ca_cert, cert_cert, cert_key, grpc_options, password, timeout, user)

    def __connect(self, ca_cert, cert_cert, cert_key, grpc_options, password, timeout, user):
        cert_params = [c is not None for c in (cert_cert, cert_key)]
        if ca_cert is not None:
            if all(cert_params):
                credentials = _get_secure_cred(
                    ca_cert,
                    cert_key,
                    cert_cert
                )
                self.uses_secure_channel = True
                self.channel = grpc.secure_channel(self._url, credentials,
                                                   options=grpc_options)
            elif any(cert_params):
                # some of the cert parameters are set
                raise ValueError(
                    'to use a secure channel ca_cert is required by itself, '
                    'or cert_cert and cert_key must both be specified.')
            else:
                credentials = _get_secure_cred(ca_cert, None, None)
                self.uses_secure_channel = True
                self.channel = grpc.secure_channel(self._url, credentials,
                                                   options=grpc_options)
        else:
            self.uses_secure_channel = False
            self.channel = grpc.insecure_channel(self._url,
                                                 options=grpc_options)
        self.timeout = timeout
        self.call_credentials = None
        cred_params = [c is not None for c in (user, password)]
        if all(cred_params):
            self.auth_stub = etcdrpc.AuthStub(self.channel)
            auth_request = etcdrpc.AuthenticateRequest(
                name=user,
                password=password
            )

            resp = self.auth_stub.Authenticate(auth_request, self.timeout)
            self.metadata = (('token', resp.token),)
            self.call_credentials = grpc.metadata_call_credentials(
                EtcdTokenCallCredentials(resp.token))

        elif any(cred_params):
            raise Exception(
                'if using authentication credentials both user and password '
                'must be specified.'
            )
        self.kvstub = etcdrpc.KVStub(self.channel)
        self.watcher = watch.Watcher(
            etcdrpc.WatchStub(self.channel),
            timeout=self.timeout,
            call_credentials=self.call_credentials,
            metadata=self.metadata
        )
        self.cluster_stub = etcdrpc.ClusterStub(self.channel)
        self.lease_stub = etcdrpc.LeaseStub(self.channel)
        self.maintenance_stub = etcdrpc.MaintenanceStub(self.channel)
        self.transactions = Transactions()

    def reconnect(self):
        """
        重新连接
        """
        self.__connect(self.ca_cert, self.cert_cert,
                       self.cert_key, self.grpc_options,
                       self.password, self.timeout,
                       self.user)

    def close(self):
        """Call the GRPC channel close semantics."""
        if hasattr(self, 'channel'):
            self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @_handle_errors
    def __get_response(self, key, serializable=False):
        """Get the value of a key from etcd."""
        range_request = _build_get_range_request(
            key,
            serializable=serializable
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get(self, key, **kwargs):
        """
        Get the value of a key from etcd.

        example usage:

        .. code-block:: python

            >>> import etcd3ref
            >>> etcd = etcd3ref.client()
            >>> etcd.get('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        """
        try:
            range_response = self.__get_response(key, **kwargs)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            range_response = self.__get_response(key, **kwargs)

        if range_response.count < 1:
            return None, None
        else:
            kv = range_response.kvs.pop()
            return kv.value, KVMetadata(kv, range_response.header)

    @_handle_errors
    def __get_prefix_response(self, key_prefix, **kwargs):
        """Get a range of keys with a prefix."""
        if any(kwarg in kwargs for kwarg in ("key", "range_end")):
            raise TypeError("Don't use key or range_end with prefix")

        range_request = _build_get_range_request(
            key=key_prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(key_prefix)),
            **kwargs
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get_prefix(self, key_prefix, **kwargs):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range
        :param keys_only: if True, retrieve only the keys, not the values

        :returns: sequence of (value, metadata) tuples
        """
        try:
            range_response = self.__get_prefix_response(key_prefix, **kwargs)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            range_response = self.__get_prefix_response(key_prefix, **kwargs)

        return (
            (kv.value, KVMetadata(kv, range_response.header))
            for kv in range_response.kvs
        )

    @_handle_errors
    def __get_range_response(self, range_start, range_end, sort_order=None,
                             sort_target='key', **kwargs):
        """Get a range of keys."""
        range_request = _build_get_range_request(
            key=range_start,
            range_end=range_end,
            sort_order=sort_order,
            sort_target=sort_target,
            **kwargs
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get_range(self, range_start, range_end, **kwargs):
        """
        Get a range of keys.

        :param range_start: first key in range
        :param range_end: last key in range
        :returns: sequence of (value, metadata) tuples
        """
        try:
            range_response = self.__get_range_response(range_start, range_end,
                                                       **kwargs)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            range_response = self.__get_range_response(range_start, range_end,
                                                       **kwargs)

        for kv in range_response.kvs:
            yield kv.value, KVMetadata(kv, range_response.header)

    @_handle_errors
    def __get_all_response(self, sort_order=None, sort_target='key',
                           keys_only=False):
        """Get all keys currently stored in etcd."""
        range_request = _build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get_all(self, **kwargs):
        """
        Get all keys currently stored in etcd.

        :param keys_only: if True, retrieve only the keys, not the values
        :returns: sequence of (value, metadata) tuples
        """
        try:
            range_response = self.__get_all_response(**kwargs)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            range_response = self.__get_all_response(**kwargs)

        for kv in range_response.kvs:
            yield kv.value, KVMetadata(kv, range_response.header)

    @_handle_errors
    def put(self, key, value, lease=None, prev_kv=False):
        """
        Save a value to etcd.

        Example usage:

        .. code-block:: python

            >>> import etcd3ref
            >>> etcd = etcd3ref.client()
            >>> etcd.put('/thing/key', 'hello world')

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :param prev_kv: return the previous key-value pair
        :type prev_kv: bool
        :returns: a response containing a header and the prev_kv
        :rtype: :class:`.rpc_pb2.PutResponse`
        """
        try:
            return self.__put(key, lease, prev_kv, value)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return self.__put(key, lease, prev_kv, value)

    def __put(self, key, lease, prev_kv, value):
        put_request = _build_put_request(key, value, lease=lease,
                                         prev_kv=prev_kv)
        return self.kvstub.Put(
            put_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def put_if_not_exists(self, key, value, lease=None):
        """
        Atomically puts a value only if the key previously had no value.

        This is the etcdv3 equivalent to setting a key with the etcdv2
        parameter prevExist=false.

        :param key: key in etcd to put
        :param value: value to be written to key
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :returns: state of transaction, ``True`` if the put was successful,
                  ``False`` otherwise
        :rtype: bool
        """
        try:
            return self.__put_if_not_exists(key, lease, value)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return self.__put_if_not_exists(key, lease, value)

    def __put_if_not_exists(self, key, lease, value):
        status, _ = self.transaction(
            compare=[self.transactions.create(key) == '0'],
            success=[self.transactions.put(key, value, lease=lease)],
            failure=[],
        )
        return status

    @_handle_errors
    def replace(self, key, initial_value, new_value):
        """
        Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :type initial_value: bytes
        :param new_value: new value of the key
        :type new_value: bytes
        :returns: status of transaction, ``True`` if the replace was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        try:
            return self.__replace(initial_value, key, new_value)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return self.__replace(initial_value, key, new_value)

    def __replace(self, initial_value, key, new_value):
        status, _ = self.transaction(
            compare=[self.transactions.value(key) == initial_value],
            success=[self.transactions.put(key, new_value)],
            failure=[],
        )
        return status

    @_handle_errors
    def delete(self, key, prev_kv=False, return_response=False):
        """
        Delete a single key in etcd.

        :param key: key in etcd to delete
        :param prev_kv: return the deleted key-value pair
        :type prev_kv: bool
        :param return_response: return the full response
        :type return_response: bool
        :returns: True if the key has been deleted when
                  ``return_response`` is False and a response containing
                  a header, the number of deleted keys and prev_kvs when
                  ``return_response`` is True
        """
        try:
            delete_response = self.__delete(key, prev_kv)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            delete_response = self.__delete(key, prev_kv)

        if return_response:
            return delete_response
        return delete_response.deleted >= 1

    def __delete(self, key, prev_kv):
        delete_request = _build_delete_request(key, prev_kv=prev_kv)
        delete_response = self.kvstub.DeleteRange(
            delete_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return delete_response

    @_handle_errors
    def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        try:
            return self.__delete_prefix(prefix)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return self.__delete_prefix(prefix)

    def __delete_prefix(self, prefix):
        delete_request = _build_delete_request(
            prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(prefix))
        )
        return self.kvstub.DeleteRange(
            delete_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def status(self):
        """Get the status of the responding member."""
        try:
            status_response = self.__status()
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            status_response = self.__status()

        for m in self.members:
            if m.id == status_response.leader:
                leader = m
                break
        else:
            # raise exception?
            leader = None

        return Status(status_response.version,
                      status_response.dbSize,
                      leader,
                      status_response.raftIndex,
                      status_response.raftTerm)

    def __status(self):
        status_request = etcdrpc.StatusRequest()
        status_response = self.maintenance_stub.Status(
            status_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return status_response

    @_handle_errors
    def add_watch_callback(self, *args, **kwargs):
        """
        Watch a key or range of keys and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        try:
            return self.watcher.add_callback(*args, **kwargs)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return self.watcher.add_callback(*args, **kwargs)
        except queue.Empty:
            raise exceptions.WatchTimedOut()

    @_handle_errors
    def add_watch_prefix_callback(self, key_prefix, callback, **kwargs):
        """
        Watch a prefix and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key_prefix: prefix to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))

        return self.add_watch_callback(key_prefix, callback, **kwargs)

    @_handle_errors
    def watch_response(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python

            responses_iterator, cancel = etcd.watch_response('/doot/key')
            for response in responses_iterator:
                print(response)

        :param key: key to watch

        :returns: tuple of ``responses_iterator`` and ``cancel``.
                  Use ``responses_iterator`` to get the watch responses,
                  each of which contains a header and a list of events.
                  Use ``cancel`` to cancel the watch request.
        """
        response_queue = queue.Queue()

        def callback(response):
            response_queue.put(response)

        watch_id = self.add_watch_callback(key, callback, **kwargs)
        canceled = threading.Event()

        def cancel():
            canceled.set()
            response_queue.put(None)
            self.cancel_watch(watch_id)

        @_handle_errors
        def iterator():
            while not canceled.is_set():
                response = response_queue.get()
                if response is None:
                    canceled.set()
                if isinstance(response, Exception):
                    canceled.set()
                    raise response
                if not canceled.is_set():
                    yield response

        return iterator(), cancel

    def watch(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python

            events_iterator, cancel = etcd.watch('/doot/key')
            for event in events_iterator:
                print(event)

        :param key: key to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request.
        """
        response_iterator, cancel = self.watch_response(key, **kwargs)
        return utils.response_to_event_iterator(response_iterator), cancel

    def watch_prefix_response(self, key_prefix, **kwargs):
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch

        :returns: tuple of ``responses_iterator`` and ``cancel``.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch_response(key_prefix, **kwargs)

    def watch_prefix(self, key_prefix, **kwargs):
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch(key_prefix, **kwargs)

    @_handle_errors
    def watch_once_response(self, key, timeout=None, **kwargs):
        """
        Watch a key and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.

        :returns: ``WatchResponse``
        """
        response_queue = queue.Queue()

        def callback(response):
            response_queue.put(response)

        watch_id = self.add_watch_callback(key, callback, **kwargs)

        try:
            return response_queue.get(timeout=timeout)
        except queue.Empty:
            raise exceptions.WatchTimedOut()
        finally:
            self.cancel_watch(watch_id)

    def watch_once(self, key, timeout=None, **kwargs):
        """
        Watch a key and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.

        :returns: ``Event``
        """
        response = self.watch_once_response(key, timeout=timeout, **kwargs)
        return response.events[0]

    def watch_prefix_once_response(self, key_prefix, timeout=None, **kwargs):
        """
        Watch a range of keys with a prefix and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch_once_response(key_prefix, timeout=timeout, **kwargs)

    def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        Watch a range of keys with a prefix and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch_once(key_prefix, timeout=timeout, **kwargs)

    @_handle_errors
    def cancel_watch(self, watch_id):
        """
        Stop watching a key or range of keys.

        :param watch_id: watch_id returned by ``add_watch_callback`` method
        """
        self.watcher.cancel(watch_id)

    def _ops_to_requests(self, ops):
        """
        Return a list of grpc requests.

        Returns list from an input list of etcd3ref.transactions.{Put, Get,
        Delete, Txn} objects.
        """
        request_ops = []
        for op in ops:
            if isinstance(op, transactions.Put):
                request = _build_put_request(op.key, op.value,
                                             op.lease, op.prev_kv)
                request_op = etcdrpc.RequestOp(request_put=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Get):
                request = _build_get_range_request(op.key, op.range_end)
                request_op = etcdrpc.RequestOp(request_range=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Delete):
                request = _build_delete_request(op.key, op.range_end,
                                                op.prev_kv)
                request_op = etcdrpc.RequestOp(request_delete_range=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Txn):
                compare = [c.build_message() for c in op.compare]
                success_ops = self._ops_to_requests(op.success)
                failure_ops = self._ops_to_requests(op.failure)
                request = etcdrpc.TxnRequest(compare=compare,
                                             success=success_ops,
                                             failure=failure_ops)
                request_op = etcdrpc.RequestOp(request_txn=request)
                request_ops.append(request_op)

            else:
                raise Exception(
                    'Unknown request class {}'.format(op.__class__))
        return request_ops

    @_handle_errors
    def transaction(self, compare, success=None, failure=None):
        """
        Perform a transaction.

        Example usage:

        .. code-block:: python

            etcd.transaction(
                compare=[
                    etcd.transactions.value('/doot/testing') == 'doot',
                    etcd.transactions.version('/doot/testing') > 0,
                ],
                success=[
                    etcd.transactions.put('/doot/testing', 'success'),
                ],
                failure=[
                    etcd.transactions.put('/doot/testing', 'failure'),
                ]
            )

        :param compare: A list of comparisons to make
        :param success: A list of operations to perform if all the comparisons
                        are true
        :param failure: A list of operations to perform if any of the
                        comparisons are false
        :return: A tuple of (operation status, responses)
        """
        compare = [c.build_message() for c in compare]

        success_ops = self._ops_to_requests(success)
        failure_ops = self._ops_to_requests(failure)

        try:
            txn_response = self.__transaction(compare, failure_ops, success_ops)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            txn_response = self.__transaction(compare, failure_ops, success_ops)

        responses = []
        for response in txn_response.responses:
            response_type = response.WhichOneof('response')
            if response_type in ['response_put', 'response_delete_range',
                                 'response_txn']:
                responses.append(response)

            elif response_type == 'response_range':
                range_kvs = []
                for kv in response.response_range.kvs:
                    range_kvs.append((kv.value,
                                      KVMetadata(kv, txn_response.header)))

                responses.append(range_kvs)

        return txn_response.succeeded, responses

    def __transaction(self, compare, failure_ops, success_ops):
        transaction_request = etcdrpc.TxnRequest(compare=compare,
                                                 success=success_ops,
                                                 failure=failure_ops)
        txn_response = self.kvstub.Txn(
            transaction_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return txn_response

    @_handle_errors
    def lease(self, ttl, lease_id=None):
        """
        Create a new lease.

        All keys attached to this lease will be expired and deleted if the
        lease expires. A lease can be sent keep alive messages to refresh the
        ttl.

        :param ttl: Requested time to live
        :param lease_id: Requested ID for the lease

        :returns: new lease
        :rtype: :class:`.Lease`
        """
        try:
            lease_grant_response = self.__lease(lease_id, ttl)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            lease_grant_response = self.__lease(lease_id, ttl)

        return leases.Lease(lease_id=lease_grant_response.ID,
                            ttl=lease_grant_response.TTL,
                            etcd_client=self)

    def __lease(self, lease_id, ttl):
        lease_grant_request = etcdrpc.LeaseGrantRequest(TTL=ttl, ID=lease_id)
        lease_grant_response = self.lease_stub.LeaseGrant(
            lease_grant_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return lease_grant_response

    @_handle_errors
    def revoke_lease(self, lease_id):
        """
        Revoke a lease.

        :param lease_id: ID of the lease to revoke.
        """
        try:
            self.__revoke_lease(lease_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            self.__revoke_lease(lease_id)

    def __revoke_lease(self, lease_id):
        lease_revoke_request = etcdrpc.LeaseRevokeRequest(ID=lease_id)
        self.lease_stub.LeaseRevoke(
            lease_revoke_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def refresh_lease(self, lease_id):
        try:
            yield from self.__refresh_lease(lease_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            yield from self.__refresh_lease(lease_id)

    def __refresh_lease(self, lease_id):
        keep_alive_request = etcdrpc.LeaseKeepAliveRequest(ID=lease_id)
        request_stream = [keep_alive_request]
        for response in self.lease_stub.LeaseKeepAlive(
                iter(request_stream),
                self.timeout,
                credentials=self.call_credentials,
                metadata=self.metadata):
            yield response

    @_handle_errors
    def get_lease_info(self, lease_id):
        # only available in etcd v3.1.0 and later
        try:
            return self.__get_lease_info(lease_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return self.__get_lease_info(lease_id)

    def __get_lease_info(self, lease_id):
        ttl_request = etcdrpc.LeaseTimeToLiveRequest(ID=lease_id,
                                                     keys=True)
        return self.lease_stub.LeaseTimeToLive(
            ttl_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def lock(self, name, ttl=60):
        """
        Create a new lock.

        :param name: name of the lock
        :type name: string or bytes
        :param ttl: length of time for the lock to live for in seconds. The
                    lock will be released after this time elapses, unless
                    refreshed
        :type ttl: int
        :returns: new lock
        :rtype: :class:`.Lock`
        """
        try:
            return locks.Lock(name, ttl=ttl, etcd_client=self)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            return locks.Lock(name, ttl=ttl, etcd_client=self)

    @_handle_errors
    def add_member(self, urls):
        """
        Add a member into the cluster.

        :returns: new member
        :rtype: :class:`.Member`
        """
        try:
            member_add_response = self.__add_member(urls)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            member_add_response = self.__add_member(urls)

        member = member_add_response.member
        return etcd3ref.members.Member(member.ID,
                                       member.name,
                                       member.peerURLs,
                                       member.clientURLs,
                                       etcd_client=self)

    def __add_member(self, urls):
        member_add_request = etcdrpc.MemberAddRequest(peerURLs=urls)
        member_add_response = self.cluster_stub.MemberAdd(
            member_add_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return member_add_response

    @_handle_errors
    def remove_member(self, member_id):
        """
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        """
        try:
            self.__remove_member(member_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            self.__remove_member(member_id)

    def __remove_member(self, member_id):
        member_rm_request = etcdrpc.MemberRemoveRequest(ID=member_id)
        self.cluster_stub.MemberRemove(
            member_rm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def update_member(self, member_id, peer_urls):
        """
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        """
        try:
            self.__update_member(member_id, peer_urls)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            self.__update_member(member_id, peer_urls)

    def __update_member(self, member_id, peer_urls):
        member_update_request = etcdrpc.MemberUpdateRequest(ID=member_id,
                                                            peerURLs=peer_urls)
        self.cluster_stub.MemberUpdate(
            member_update_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @property
    def members(self):
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        """
        try:
            member_list_response = self.__members()
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            member_list_response = self.__members()

        for member in member_list_response.members:
            yield etcd3ref.members.Member(member.ID,
                                          member.name,
                                          member.peerURLs,
                                          member.clientURLs,
                                          etcd_client=self)

    def __members(self):
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response = self.cluster_stub.MemberList(
            member_list_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return member_list_response

    @_handle_errors
    def compact(self, revision, physical=False):
        """
        Compact the event history in etcd up to a given revision.

        All superseded keys with a revision less than the compaction revision
        will be removed.

        :param revision: revision for the compaction operation
        :param physical: if set to True, the request will wait until the
                         compaction is physically applied to the local database
                         such that compacted entries are totally removed from
                         the backend database
        """
        try:
            self.__compact(physical, revision)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            self.__compact(physical, revision)

    def __compact(self, physical, revision):
        compact_request = etcdrpc.CompactionRequest(revision=revision,
                                                    physical=physical)
        self.kvstub.Compact(
            compact_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def defragment(self):
        """Defragment a member's backend database to recover storage space."""
        try:
            self.__defrag()
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            self.__defrag()

    def __defrag(self):
        defrag_request = etcdrpc.DefragmentRequest()
        self.maintenance_stub.Defragment(
            defrag_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def hash(self):
        """
        Return the hash of the local KV state.

        :returns: kv state hash
        :rtype: int
        """
        try:
            hash_request = etcdrpc.HashRequest()
            return self.maintenance_stub.Hash(hash_request).hash
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            hash_request = etcdrpc.HashRequest()
            return self.maintenance_stub.Hash(hash_request).hash

    @_handle_errors
    def create_alarm(self, member_id=0):
        """Create an alarm.

        If no member id is given, the alarm is activated for all the
        members of the cluster. Only the `no space` alarm can be raised.

        :param member_id: The cluster member id to create an alarm to.
                          If 0, the alarm is created for all the members
                          of the cluster.
        :returns: list of :class:`.Alarm`
        """
        try:
            alarm_response = self.__create_alarm(member_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            alarm_response = self.__create_alarm(member_id)

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    def __create_alarm(self, member_id):
        alarm_request = _build_alarm_request('activate',
                                             member_id,
                                             'no space')
        alarm_response = self.maintenance_stub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return alarm_response

    @_handle_errors
    def list_alarms(self, member_id=0, alarm_type='none'):
        """List the activated alarms.

        :param member_id:
        :param alarm_type: The cluster member id to create an alarm to.
                           If 0, the alarm is created for all the members
                           of the cluster.
        :returns: sequence of :class:`.Alarm`
        """
        try:
            alarm_response = self.__list_alarms(alarm_type, member_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            alarm_response = self.__list_alarms(alarm_type, member_id)

        for alarm in alarm_response.alarms:
            yield Alarm(alarm.alarm, alarm.memberID)

    def __list_alarms(self, alarm_type, member_id):
        alarm_request = _build_alarm_request('get',
                                             member_id,
                                             alarm_type)
        alarm_response = self.maintenance_stub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return alarm_response

    @_handle_errors
    def disarm_alarm(self, member_id=0):
        """Cancel an alarm.

        :param member_id: The cluster member id to cancel an alarm.
                          If 0, the alarm is canceled for all the members
                          of the cluster.
        :returns: List of :class:`.Alarm`
        """
        try:
            alarm_response = self.__disarm_alarm(member_id)
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            alarm_response = self.__disarm_alarm(member_id)

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    def __disarm_alarm(self, member_id):
        alarm_request = _build_alarm_request('deactivate',
                                             member_id,
                                             'no space')
        alarm_response = self.maintenance_stub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return alarm_response

    @_handle_errors
    def snapshot(self, file_obj):
        """Take a snapshot of the database.

        :param file_obj: A file-like object to write the database contents in.
        """
        try:
            snapshot_response = self.__snapshot()
        except _InactiveRpcError as e:
            print("Exception {} happened, try to reconnect \n".format(e))
            self.reconnect()
            snapshot_response = self.__snapshot()

        for response in snapshot_response:
            file_obj.write(response.blob)

    def __snapshot(self):
        snapshot_request = etcdrpc.SnapshotRequest()
        snapshot_response = self.maintenance_stub.Snapshot(
            snapshot_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return snapshot_response


def client(host='localhost', port=2379,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None, grpc_options=None):
    """Return an instance of an Etcd3Client."""
    return Etcd3Client(host=host,
                       port=port,
                       ca_cert=ca_cert,
                       cert_key=cert_key,
                       cert_cert=cert_cert,
                       timeout=timeout,
                       user=user,
                       password=password,
                       grpc_options=grpc_options)
