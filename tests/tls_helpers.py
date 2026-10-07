from __future__ import annotations

import functools
import ssl

import anyio
from anyio.abc import SocketAttribute
from anyio.streams.tls import TLSListener
from botocore.awsrequest import AWSRequest

TARGET_HOST = "localhost"
RESPONSE_BODY = b'{"ok": true}'


async def handle_target(stream, request_lines=None) -> None:
    """Serve one minimal HTTP/1.1 request over an accepted stream."""
    try:
        buf = b""
        while b"\r\n\r\n" not in buf:
            buf += await stream.receive()
        if request_lines is not None:
            request_lines.append(buf.split(b"\r\n", 1)[0])
        await stream.send(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Length: %d\r\n"
            b"Content-Type: application/json\r\n"
            b"\r\n%b" % (len(RESPONSE_BODY), RESPONSE_BODY)
        )
    except (
        anyio.EndOfStream,
        anyio.BrokenResourceError,
        ConnectionResetError,
    ):  # pragma: no cover
        pass
    finally:
        try:
            await stream.aclose()
        except (
            anyio.BrokenResourceError,
            ssl.SSLError,
        ):  # pragma: no cover
            # aclose() force-closes the socket and re-raises when the client
            # hung up without a TLS shutdown handshake.
            pass


async def serve_https_target(
    ca,
    *,
    hostname=TARGET_HOST,
    client_ca=None,
    request_lines=None,
    task_status,
) -> None:
    server_cert = ca.issue_cert(hostname)
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(ssl_context)
    if client_ca is not None:
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        client_ca.configure_trust(ssl_context)

    listener = await anyio.create_tcp_listener(
        local_host="127.0.0.1", local_port=0
    )
    async with listener:
        port = listener.extra(SocketAttribute.local_port)
        task_status.started(port)
        await TLSListener(listener, ssl_context).serve(
            functools.partial(handle_target, request_lines=request_lines)
        )


def prepared_request(
    port: int, host: str = TARGET_HOST, path: str = "/foo?id=1"
) -> AWSRequest:
    request = AWSRequest(
        method="GET",
        url=f"https://{host}:{port}{path}",
        headers={"Accept": "application/json"},
    ).prepare()
    request.stream_output = False
    return request
