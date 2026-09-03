# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Identity-aware HTTP worker for the hosted native-Iroh client test."""

from __future__ import annotations

import socket
from typing import Protocol

import waitress
from vgi_rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.http import (
    bearer_authenticate_static,
    iroh_forwarded_header_provider,
    make_wsgi_app,
)
from vgi_rpc.rpc import require_peer_identity


class IrohHttpIntegrationService(Protocol):
    """Small typed surface that exposes only test-safe identity facts."""

    def echo(self, value: str) -> str: ...

    def identity(self) -> str: ...


class IrohHttpIntegrationImpl:
    """Prove Arrow typing, bearer preservation, and bridge-supplied identity."""

    def echo(self, value: str) -> str:
        return f"typed:{value}"

    def identity(self, ctx: CallContext) -> str:
        ctx.auth.require_authenticated()
        identity = ctx.peer_evidence.unique_verified_subject("iroh")
        return ":".join(
            (
                ctx.auth.domain,
                ctx.auth.principal or "",
                identity.provider,
                identity.issuer,
                identity.subject_key or "",
                str(identity.attributes.get("original_assurance", "")),
                str(bool(ctx.auth.claims.get("peer_evidence_binding"))).lower(),
            )
        )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def main() -> None:
    port = _free_port()
    auth = bearer_authenticate_static(
        tokens={
            "java-iroh-ci-token": AuthContext(
                domain="bearer",
                authenticated=True,
                principal="java-ci",
                claims={},
            )
        }
    )
    identity = iroh_forwarded_header_provider(
        issuer="java-hosted-ci",
        trusted_proxy_addresses=("127.0.0.1",),
    )
    app = make_wsgi_app(
        RpcServer(IrohHttpIntegrationService, IrohHttpIntegrationImpl()),
        authenticate=auth,
        peer_identity_providers=(identity,),
        peer_authentication_policy=require_peer_identity("iroh"),
        enable_landing_page=False,
        enable_describe_page=False,
    )
    print(f"PORT:{port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=port, threads=4, _quiet=True)


if __name__ == "__main__":
    main()
