import * as cdk from "aws-cdk-lib";
import { CustomResource, Duration, RemovalPolicy } from "aws-cdk-lib";
import {
  InstanceType,
  InstanceClass,
  InstanceSize,
  SubnetType,
  Vpc,
} from "aws-cdk-lib/aws-ec2";
import { Runtime } from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import {
  DatabaseCluster,
  DatabaseClusterEngine,
  AuroraMysqlEngineVersion,
  Credentials,
} from "aws-cdk-lib/aws-rds";
import { Bucket, BlockPublicAccess, ObjectOwnership } from "aws-cdk-lib/aws-s3";
import { Provider } from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";
import { join } from "path";
import {
  PythonFunction,
  PythonLayerVersion,
} from "@aws-cdk/aws-lambda-python-alpha";

export class BlogCdkMysqlStreamStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new Vpc(this, "Vpc");
    const serverId = "1";
    const cluster = new DatabaseCluster(this, "Database", {
      clusterIdentifier: `stream-db`,
      engine: DatabaseClusterEngine.auroraMysql({
        version: AuroraMysqlEngineVersion.VER_2_10_1,
      }),
      defaultDatabaseName: "martzcodes",
      credentials: Credentials.fromGeneratedSecret("clusteradmin"),
      iamAuthentication: true,
      instanceProps: {
        instanceType: InstanceType.of(
          InstanceClass.BURSTABLE2,
          InstanceSize.SMALL
        ),
        vpcSubnets: {
          subnetType: SubnetType.PRIVATE_WITH_EGRESS,
        },
        vpc,
      },
      removalPolicy: RemovalPolicy.DESTROY,
      parameters: {
        "server-id": serverId,
        binlog_format: "ROW",
      },
    });

    const dbUser = "binlog-streamer";
    const tableInitFn = new NodejsFunction(this, `tableInitFn`, {
      entry: `${__dirname}/tableInit.ts`,
      timeout: Duration.minutes(5),
      runtime: Runtime.NODEJS_16_X,
      environment: {
        SECRET_ARN: cluster.secret!.secretArn,
        DB_USER: dbUser,
      },
      logRetention: RetentionDays.ONE_DAY,
    });
    cluster.secret!.grantRead(tableInitFn);

    const tableInitProvider = new Provider(scope, `tableInitProvider`, {
      onEventHandler: tableInitFn,
    });

    const tableInitResource = new CustomResource(scope, `tableInitResource`, {
      properties: { Version: "1" },
      serviceToken: tableInitProvider.serviceToken,
    });

    tableInitResource.node.addDependency(cluster);

    const binlogBucket = new Bucket(this, `binlog-bucket`, {
      removalPolicy: RemovalPolicy.DESTROY,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [
        {
          abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
          expiration: cdk.Duration.days(7),
        },
      ],
    });

    const binlogDepLayer = new PythonLayerVersion(this, `pybinlogdeplayer`, {
      entry: join(__dirname, "lambda/binlog"),
      compatibleRuntimes: [Runtime.PYTHON_3_8],
    });

    const binlogFn = new PythonFunction(this, `pybinlog`, {
      entry: join(__dirname, "binlog"),
      functionName: `pybinlog`,
      runtime: Runtime.PYTHON_3_8,
      environment: {
        SECRET_ARN: cluster.secret!.secretArn,
        SERVER_ID: serverId,
        BUCKET_NAME: binlogBucket.bucketName,
      },
      memorySize: 4096,
      timeout: Duration.minutes(15),
      vpc,
      layers: [binlogDepLayer],
      securityGroups: cluster.connections.securityGroups,
    });
    binlogBucket.grantReadWrite(binlogFn);
    cluster.secret!.grantRead(binlogFn);
  }
}
