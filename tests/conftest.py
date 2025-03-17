from __future__ import annotations

import json
import shlex
import socket
import uuid
from typing import TYPE_CHECKING, Any, Generator, cast
from unittest.mock import MagicMock

import pytest
import yt.wrapper
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from ytsaurus_airflow_provider.hooks.ytsaurus import YTsaurusHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


@pytest.fixture(scope="session", autouse=True)
def setup_storage() -> Generator[None, None, None]:
    free_port = _get_free_port()
    mp = pytest.MonkeyPatch()
    mp.setattr(
        AwsGenericHook,
        "get_connection",
        lambda self, conn_id: Connection(  # noqa: ARG005
            conn_id=conn_id,
            extra={
                "aws_access_key_id": "any",
                "aws_secret_access_key": "any",
                "endpoint_url": f"http://localhost:{free_port}",
            },
        ),
    )

    with (
        DockerContainer("chrislusf/seaweedfs:latest")
        .with_bind_ports(8333, free_port)
        .with_command("server -s3 -master.raftHashicorp")  # `raftHashicorp` increases setup speed
        .with_name("s3")
    ) as container:
        wait_for_logs(container, ".*Start Seaweed S3 API Server.*")
        yield
    mp.undo()


@pytest.fixture(scope="session", autouse=True)
def yt_client() -> Generator[yt.wrapper.YtClient, None, None]:
    free_port = _get_free_port()

    mp = pytest.MonkeyPatch()
    proxy = f"localhost:{free_port}"
    client = yt.wrapper.YtClient(proxy)
    mp.setattr(YTsaurusHook, "get_conn", lambda self, headers=None: client)  # noqa: ARG005

    yt_docker_args = [
        "--fqdn",
        "localhost",
        "--proxy-config",
        """{coordinator={public_fqdn="localhost:""" + str(free_port) + """"}}""",
        "-c",
        "{name=query-tracker}",
        "-c",
        """{name=yql-agent;config={path="/usr/bin";count=1;artifacts_path="/usr/bin"}}""",
    ]

    with (
        DockerContainer("ghcr.io/ytsaurus/local:stable")
        .with_env("YT_FORCE_IPV4", "1")
        .with_env("YT_FORCE_IPV6", "0")
        .with_env("YT_USE_HOSTS", "0")
        .with_bind_ports(80, free_port)
        .with_command(shlex.join(yt_docker_args))
        .with_name("yt")
    ) as container:
        wait_for_logs(container, "Local YT started")
        yield client
    mp.undo()


@pytest.fixture(scope="session")
def tmp_node(yt_client: yt.wrapper.YtClient) -> Generator[str, None, None]:
    suffix = uuid.uuid4().__str__()[:8]
    path = f"//home/airflow/tests/{suffix}"
    yield path
    yt_client.remove(path, recursive=True)


@pytest.fixture
def node_path(request: pytest.FixtureRequest, tmp_node: str, yt_client: yt.wrapper.YtClient) -> Generator[str, None, None]:
    test_name = request.node.name
    for char in ["[", "]", "(", ")", ":", " ", "<", ">", ",", ".", "/"]:
        test_name = test_name.replace(char, "")
    test_node_path = f"{tmp_node}/{test_name}"
    yt_client.create("map_node", test_node_path, recursive=True)
    yield test_node_path
    yt_client.remove(test_node_path, recursive=True)


@pytest.fixture
def context_mock() -> dict[str, MagicMock]:
    magic_mock = MagicMock()
    magic_mock.xcom_push.side_effect = _check_json_serializable
    return {"ti": magic_mock}


@pytest.fixture
def context(context_mock: dict[str, MagicMock]) -> Context:
    return cast("Context", context_mock)


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _check_json_serializable(*args: Any, **kwargs: Any) -> None:
    def check_serializable(obj: Any) -> bool:
        try:
            json.dumps(obj)
        except (TypeError, ValueError):
            return False
        return True

    for arg in args:
        if not check_serializable(arg):
            msg = "The result of execute is not JSON serializable"
            raise ValueError(msg) from None

    for key, val in kwargs.items():
        if not check_serializable(val):
            msg = f"The result of execute is not JSON serializable ({key=})"
            raise ValueError(msg) from None


def patch_operator_exec(cls: Any) -> Any:
    original_exec = cls.execute

    def new_exec(self: Any, context: Context) -> dict[str, Any]:
        result = original_exec(self, context)
        json.dumps(result)
        return result

    cls.execute = new_exec
    return cls
