import io
import itertools
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from kascov import Kascov


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class KascovClientTests(unittest.TestCase):
    def test_events_encode_cursor_and_all_filters(self):
        client = Kascov("testnet-10", "https://example.test")
        with patch.object(client, "_get", return_value={}) as get:
            client.events(
                after="00112233445566778899aabbccddeeff:4",
                limit=9,
                covenant="aa",
                application="duel",
                artifact="bb",
                actor="match/4",
            )
        query = parse_qs(urlparse(get.call_args.args[0]).query)
        self.assertEqual(["00112233445566778899aabbccddeeff:4"], query["after"])
        self.assertEqual(["duel"], query["application"])
        self.assertEqual(["match/4"], query["actor"])

    def test_stream_uses_initial_after_and_filters(self):
        requests = []

        def open_once(request, timeout=None):
            requests.append(request)
            return Response(b"id: epoch:5\nevent: accepted\ndata: {\"kind\":\"accepted\"}\n\n")

        client = Kascov("mainnet", "https://example.test")
        with patch("urllib.request.urlopen", side_effect=open_once):
            result = list(client.stream(
                after="epoch:4",
                application="duel",
                actor="match/4",
                reconnect=False,
            ))
        query = parse_qs(urlparse(requests[0].full_url).query)
        self.assertEqual(["epoch:4"], query["after"])
        self.assertEqual(["duel"], query["application"])
        self.assertEqual("epoch:5", result[0]["_cursor"])

    def test_reset_loads_snapshot_and_reopens_from_snapshot_cursor(self):
        requests = []

        def open_next(request, timeout=None):
            requests.append(request)
            if request.full_url.endswith("/data/mainnet.json"):
                return Response(b'{"stream_cursor":"new-epoch:9"}')
            if len([item for item in requests if "/stream" in item.full_url]) == 1:
                return Response(
                    b"id: epoch:5\ndata: {\"kind\":\"accepted\"}\n\n"
                    b"event: reset\ndata: {\"reason\":\"foreign_epoch\","
                    b"\"snapshot\":\"/data/mainnet.json\"}\n\n"
                )
            return Response(
                b"id: new-epoch:10\nevent: accepted\n"
                b"data: {\"kind\":\"accepted\"}\n\n"
            )

        with patch("urllib.request.urlopen", side_effect=open_next):
            result = list(Kascov(base="https://example.test").stream(reconnect=False))
        stream_requests = [item for item in requests if "/stream" in item.full_url]
        self.assertEqual(2, len(stream_requests))
        self.assertEqual("new-epoch:9", parse_qs(urlparse(stream_requests[1].full_url).query)["after"][0])
        self.assertEqual("reset", result[1]["_event"])
        self.assertEqual("new-epoch:9", result[1]["_snapshot"]["stream_cursor"])
        self.assertEqual("new-epoch:10", result[2]["_cursor"])

    def test_transport_reconnect_sends_last_event_id(self):
        requests = []
        bodies = iter([
            b"id: epoch:5\ndata: {\"kind\":\"accepted\"}\n\n",
            b"id: epoch:6\ndata: {\"kind\":\"accepted\"}\n\n",
        ])

        def open_next(request, timeout=None):
            requests.append(request)
            return Response(next(bodies))

        with patch("urllib.request.urlopen", side_effect=open_next):
            result = list(itertools.islice(Kascov(base="https://example.test").stream(), 2))
        self.assertEqual(2, len(result))
        self.assertEqual("epoch:5", requests[1].headers["Last-event-id"])



if __name__ == "__main__":
    unittest.main()
