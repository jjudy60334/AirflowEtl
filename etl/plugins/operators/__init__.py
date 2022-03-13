from typing import Optional, Union

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from airflow.utils.decorators import apply_defaults
from kubernetes import client, config


class CustomK8sDeploymentPatchOperator(BaseOperator):
    template_fields = ('namespace', 'deployment', 'body')

    @apply_defaults
    def __init__(
        self,
        namespace: str,
        deployment: str,
        body: dict = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.namespace = namespace
        self.deployment = deployment

        if isinstance(body, dict) or body is None:
            self.body = body
        else:
            raise ValueError("Body must be dict or None")

    def execute(self, context):
        config.load_incluster_config()
        apps_api = client.AppsV1Api()

        # 確認 deployment 是否存在
        apps_api.read_namespaced_deployment(
            name=self.deployment,
            namespace=self.namespace
        )

        apps_api.patch_namespaced_deployment(
            name=self.deployment,
            namespace=self.namespace,
            body=self.body
        )


class SQLTemplatedPythonOperator(PythonOperator):
    # somehow ('.sql',) doesn't work but tuple of two works...
    render_template_as_native_obj = True
    template_fields = ["sql", "templates_dict", "op_kwargs"]
    template_ext = [('.sql')]
    template_fields_renderers = {
        "templates_dict": "json",
        "sql": "sql",
        "op_args": "py", "op_kwargs": "json"
    }

    def __init__(
            self,
            templates_dict: dict = None,
            *args,
            **kwargs) -> None:
        self.templates_dict = templates_dict
        super().__init__(templates_dict=self.templates_dict, **kwargs)
        if self.templates_dict:
            self.sql = self.templates_dict["sql"]


class SQLTemplatedKubernetesPodOperator(KubernetesPodOperator):
    ui_color = '#FFC300'
    ui_fgcolor = '#FF5733'
    # somehow ('.sql',) doesn't work but tuple of two works...
    render_template_as_native_obj = True
    template_fields = ('sql', 'cmds', 'env_vars', 'image', 'arguments')
    template_ext = [('.sql')]
    template_fields_renderers = {
        "templates_dict": "json",
        "templates_dict.sql": "sql",
        "arguments": "json"
    }

    @apply_defaults
    def __init__(
            self,
            sql,
            arguments,
            sql_argument_name,
            *args,
            ** kwargs) -> None:

        self.sql = sql
        self.sql_argument_name = sql_argument_name
        self.arguments = arguments + [f'--{self.sql_argument_name}', self.sql]
        super().__init__(arguments=self.arguments, **kwargs)


class S3LoadStringOperator(BaseOperator):
    template_fields = ('string_data', 'dest_key', 'dest_bucket_name')

    @apply_defaults
    def __init__(
        self,
        *,
        string_data: str,
        dest_key: str,
        dest_bucket_name: str,
        replace: bool = False,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        encoding: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.string_data = string_data
        self.dest_key = dest_key
        self.dest_bucket_name = dest_bucket_name
        self.replace = replace
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.encoding = encoding

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_hook.load_string(
            string_data=self.string_data,
            key=self.dest_key,
            bucket_name=self.dest_bucket_name,
            replace=self.replace,
            encoding=self.encoding
        )
