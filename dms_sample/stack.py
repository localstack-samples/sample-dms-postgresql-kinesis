import json
import os

import aws_cdk as cdk
from aws_cdk import SecretValue, Stack
from aws_cdk import aws_dms as dms
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_rds as rds
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


USERNAME = os.getenv("USERNAME", "")
USER_PWD = os.getenv("USERPWD", "")
DB_NAME = os.getenv("DB_NAME", "")
SCHEMA_NAME = "public"


class DmsSampleStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # VPC and Security Group
        vpc = ec2.Vpc(
            self,
            "vpc",
            max_azs=2,
            nat_gateways=0,
            enable_dns_support=True,
            enable_dns_hostnames=True,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    cidr_mask=24,
                    subnet_type=ec2.SubnetType.PUBLIC,
                ),
            ],
        )
        security_group = create_security_group(self, vpc)

        # IAM Role for DMS
        dms_assume_role = iam.Role(
            self, "SuperRole", assumed_by=iam.ServicePrincipal("dms.amazonaws.com")
        )

        # Aurora PostgreSQL Cluster
        engine = rds.DatabaseClusterEngine.aurora_postgres(
            version=rds.AuroraPostgresEngineVersion.VER_15_3
        )

        parameter_group = rds.ParameterGroup(
            self,
            "parameterGroup",
            engine=engine,
            parameters={"rds.logical_replication": "1"},
        )

        credentials = rds.Credentials.from_password(
            username=USERNAME,
            password=SecretValue.unsafe_plain_text(USER_PWD),
        )

        aurora_cluster = rds.DatabaseCluster(
            self,
            "auroraCluster",
            engine=engine,
            parameter_group=parameter_group,
            vpc=vpc,
            security_groups=[security_group],
            writer=rds.ClusterInstance.serverless_v2("writer"),
            readers=[rds.ClusterInstance.serverless_v2("reader", scale_with_writer=True)],
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=1,
            credentials=credentials,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            default_database_name=DB_NAME,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )

        # Secrets Manager for DB Access
        db_port = cdk.Token.as_number(aurora_cluster.cluster_endpoint.port)

        allow_from_port(security_group, db_port)

        postgres_secret = create_secret(self, aurora_cluster.cluster_endpoint.hostname, db_port)

        postgres_access_role = create_postgres_access_role(self, postgres_secret)

        # Source Endpoint
        source_endpoint = create_source_endpoint(self, postgres_access_role, postgres_secret)
        
        # Kinesis Stream
        target_stream = create_kinesis_stream(self, dms_assume_role)

        # Target Endpoint
        target_endpoint = create_kinesis_target_endpoint(self, target_stream, dms_assume_role)

        # DMS Replication Instance
        replication_instance = create_replication_instance(self, vpc, security_group)

        # Create CDC replication task
        cdc_task = create_replication_task(
            self,
            "cdc-task",
            replication_instance=replication_instance,
            source=source_endpoint,
            target=target_endpoint,
            migration_type="cdc",
        )

        # Outputs
        cdk.CfnOutput(self, "kinesisStream", value=target_stream.stream_arn)
        cdk.CfnOutput(self, "cdcTask", value=cdc_task.ref)
        cdk.CfnOutput(self, "dbSecret", value=postgres_secret.ref)


def allow_from_port(security_group: ec2.SecurityGroup, port: int):
    security_group.connections.allow_from(
            port_range=ec2.Port.tcp_range(port, port),
            other=ec2.Peer.any_ipv4(),
        )


def create_replication_task(
    stack: Stack,
    id: str,
    replication_instance: dms.CfnReplicationInstance,
    source: dms.CfnEndpoint,
    target: dms.CfnEndpoint,
    migration_type: str = "cdc",
    replication_task_settings: dict = None,
) -> dms.CfnReplicationTask:
    table_mappings = {
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "rule1",
                "object-locator": {"schema-name": "public", "table-name": "%"},
                "rule-action": "include",
            }
        ]
    }
    replication_task_settings = {"Logging": {"EnableLogging": True}}

    return dms.CfnReplicationTask(
        stack,
        id,
        replication_task_identifier=id,
        migration_type=migration_type,
        replication_instance_arn=replication_instance.ref,
        source_endpoint_arn=source.ref,
        target_endpoint_arn=target.ref,
        table_mappings=json.dumps(table_mappings),
        replication_task_settings=json.dumps(replication_task_settings),
    )


def create_source_endpoint(stack: Stack, role: iam.Role, secret: secretsmanager.CfnSecret) -> dms.CfnEndpoint:
    return dms.CfnEndpoint(
            stack,
            "source-postgres",
            endpoint_type="source",
            engine_name="aurora-postgresql",
            database_name=DB_NAME,
            postgre_sql_settings=dms.CfnEndpoint.PostgreSqlSettingsProperty(
                secrets_manager_access_role_arn=role.role_arn,
                secrets_manager_secret_id=secret.ref,
            ),
        )


def create_secret(stack: Stack, host: str, db_port: int) -> secretsmanager.CfnSecret:
    return secretsmanager.CfnSecret(
            stack,
            "postgres-secret",
            secret_string=json.dumps(
                {
                    "username": USERNAME,
                    "password": USER_PWD,
                    "host": host,
                    "port": db_port,
                    "dbname": DB_NAME,
                }
            ),
        )


def create_security_group(stack: Stack, vpc: ec2.Vpc) -> ec2.SecurityGroup:
    return ec2.SecurityGroup(
        stack,
        "sg",
        vpc=vpc,
        description="Security group for DMS sample",
        allow_all_outbound=True,
    )


def create_kinesis_stream(stack: Stack, dms_assume_role: iam.Role) -> kinesis.Stream:
    stream = kinesis.Stream(
        stack, "TargetStream", shard_count=1, retention_period=cdk.Duration.hours(24)
    )
    stream.grant_read_write(dms_assume_role)
    stream.apply_removal_policy(cdk.RemovalPolicy.DESTROY)
    return stream


def create_postgres_access_role(stack: Stack, postgres_secret: secretsmanager.CfnSecret) -> iam.Role:
    return iam.Role(
        stack,
        "postgres-access-role",
        assumed_by=iam.ServicePrincipal(f"dms.{stack.region}.amazonaws.com"),
        inline_policies={
            "AllowSecrets": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["secretsmanager:GetSecretValue"],
                        effect=iam.Effect.ALLOW,
                        resources=[postgres_secret.ref],
                    )
                ]
            )
        },
    )


def create_kinesis_target_endpoint(stack: Stack, target_stream: kinesis.Stream, dms_assume_role: iam.Role) -> dms.CfnEndpoint:
    return dms.CfnEndpoint(
        stack,
        "target",
        endpoint_type="target",
        engine_name="kinesis",
        kinesis_settings=dms.CfnEndpoint.KinesisSettingsProperty(
            stream_arn=target_stream.stream_arn,
            message_format="json",
            service_access_role_arn=dms_assume_role.role_arn,
            include_control_details=True,
            include_null_and_empty=True,
            include_partition_value=True,
            include_table_alter_operations=True,
            include_transaction_details=False,
            partition_include_schema_table=True,
        ),
    )


def create_replication_instance(
    stack: Stack, vpc: ec2.Vpc, security_group: ec2.SecurityGroup
) -> dms.CfnReplicationInstance:
    # Role definitions
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "dms.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    iam.CfnRole(
        stack,
        "DmsVpcRole",
        managed_policy_arns=[
            "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole",
        ],
        assume_role_policy_document=assume_role_policy_document,
        role_name="dms-vpc-role",  # this exact name needs to be set
    )
    replication_subnet_group = dms.CfnReplicationSubnetGroup(
        stack,
        "ReplSubnetGroup",
        replication_subnet_group_description="Replication Subnet Group for DMS test",
        subnet_ids=[subnet.subnet_id for subnet in vpc.public_subnets],
    )

    return dms.CfnReplicationInstance(
        stack,
        "replication-instance",
        replication_instance_class="dms.t2.micro",
        allocated_storage=5,
        replication_subnet_group_identifier=replication_subnet_group.ref,
        allow_major_version_upgrade=False,
        auto_minor_version_upgrade=False,
        multi_az=False,
        publicly_accessible=True,
        vpc_security_group_ids=[security_group.security_group_id],
        availability_zone=vpc.public_subnets[0].availability_zone,
    )
